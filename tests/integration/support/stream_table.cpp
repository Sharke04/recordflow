#include "support/stream_table.hpp"

#include <clickhouse/client.h>

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace recordflow::it {

StreamTable::StreamTable(const std::string& prefix)
    : name_(unique_name(prefix)), publication_(name_ + "_pub"), slot_(name_ + "_slot") {
    root_ = std::filesystem::temp_directory_path() / name_;
    data_dir_ = root_ / "data";
    std::filesystem::create_directories(root_);

    pg_.exec("CREATE TABLE " + qualified() + " (id bigint PRIMARY KEY, name text, amount numeric(12,2))");
    pg_.exec("GRANT SELECT ON " + qualified() + " TO recordflow");
    pg_.exec("CREATE PUBLICATION " + publication_ + " FOR TABLE " + qualified());
    // In its own statement: a slot cannot be created in a transaction that wrote.
    pg_.exec("SELECT pg_create_logical_replication_slot('" + slot_ + "', 'pgoutput')");
}

StreamTable::~StreamTable() {
    try {
        // The walsender of a stopped process may hold the slot for a moment.
        wait_until(
            [&] {
                return pg_.query("SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + slot_ +
                                 "' AND active").empty();
            },
            std::chrono::seconds(15));
        pg_.exec("SELECT pg_drop_replication_slot('" + slot_ + "')");
        pg_.exec("DROP PUBLICATION IF EXISTS " + publication_);
        pg_.exec("DROP TABLE IF EXISTS " + qualified());
        auto ch = clickhouse_admin();
        ch->Execute("DROP VIEW IF EXISTS " + target() + "_current");
        ch->Execute("DROP TABLE IF EXISTS " + target() + " SYNC");
    } catch (const std::exception& e) {
        ADD_FAILURE() << "cleanup of " << name_ << " failed: " << e.what();
    }
    std::error_code ignored;
    std::filesystem::remove_all(root_, ignored);
}

std::map<std::string, std::string> StreamTable::env() const {
    return {
        {"RECORDFLOW_SOURCE_CONNINFO",
         "host=" + setting("RECORDFLOW_IT_POSTGRES_HOST", "localhost") +
             " port=" + setting("RECORDFLOW_IT_POSTGRES_PORT", "5432") +
             " dbname=" + setting("POSTGRES_DB", "mydb") + " user=recordflow"},
        {"PGPASSWORD", setting("RECORDFLOW_PG_PASSWORD", "")},
        {"RECORDFLOW_SOURCE_SLOT", slot_},
        {"RECORDFLOW_SOURCE_PUBLICATION", publication_},
        {"RECORDFLOW_SOURCE_TABLE", qualified()},
        {"RECORDFLOW_CLICKHOUSE_HOST", setting("RECORDFLOW_IT_CLICKHOUSE_HOST", "localhost")},
        {"RECORDFLOW_CLICKHOUSE_PORT", setting("RECORDFLOW_IT_CLICKHOUSE_PORT", "9000")},
        {"RECORDFLOW_CLICKHOUSE_PASSWORD", setting("RECORDFLOW_CH_PASSWORD", "")},
        // LZ4 and ZSTD are broken in the vcpkg build of clickhouse-cpp.
        {"RECORDFLOW_CLICKHOUSE_COMPRESSION", "none"},
        {"RECORDFLOW_OUTPUT_SINK", "clickhouse"},
        {"RECORDFLOW_STATE_DATA_DIR", data_dir_.string()},
        {"RECORDFLOW_BATCH_MAX_DELAY_MS", "200"},
        {"RECORDFLOW_RETRY_INITIAL_MS", "200"},
        {"RECORDFLOW_RETRY_MAX_MS", "2000"},
    };
}

std::unique_ptr<RecordflowProcess> StreamTable::start(const std::map<std::string, std::string>& extra) {
    std::map<std::string, std::string> variables = env();
    for (const auto& [key, value] : extra) variables[key] = value;
    const auto log_file = root_ / ("recordflow." + std::to_string(++starts_) + ".log");
    return std::make_unique<RecordflowProcess>(variables, log_file);
}

std::unique_ptr<RecordflowProcess> StreamTable::start_streaming(const std::map<std::string, std::string>& extra) {
    auto process = start(extra);
    if (!process->wait_for_log("Streaming from slot", std::chrono::seconds(30))) {
        throw std::runtime_error("recordflow did not start streaming:\n" + process->log());
    }
    return process;
}

std::string StreamTable::slot_confirmed_lsn() {
    const auto rows =
        pg_.query("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '" + slot_ + "'");
    return rows.empty() ? "" : rows[0][0];
}

std::string StreamTable::state_last_flushed_lsn() const {
    std::ifstream in(data_dir_ / "recordflow.state.json");
    std::ostringstream text;
    text << in.rdbuf();
    const std::string json = text.str();
    const std::string key = "\"last_flushed_lsn\": \"";
    const auto begin = json.find(key);
    if (begin == std::string::npos) return "";
    const auto end = json.find('"', begin + key.size());
    return json.substr(begin + key.size(), end - begin - key.size());
}

std::vector<std::string> StreamTable::source_rows() {
    std::vector<std::string> rows;
    // numeric(12,2) -> Decimal(12, 2): ClickHouse prints no trailing zeros.
    for (const auto& row : pg_.query("SELECT id::text || '|' || coalesce(name, '<null>') || '|' || "
                                     "coalesce(trim_scale(amount)::text, '<null>') FROM " +
                                     qualified() + " ORDER BY id")) {
        rows.push_back(row[0]);
    }
    return rows;
}

std::vector<std::string> StreamTable::target_rows() {
    std::vector<std::string> rows;
    auto ch = clickhouse_admin();
    ch->Select("SELECT concat(toString(id), '|', ifNull(name, '<null>'), '|', ifNull(toString(amount), '<null>')) "
               "FROM " + target() + "_current ORDER BY id",
               [&](const clickhouse::Block& block) {
                   for (std::size_t r = 0; r < block.GetRowCount(); ++r) {
                       rows.emplace_back(block[0]->As<clickhouse::ColumnString>()->At(r));
                   }
               });
    return rows;
}

::testing::AssertionResult target_equals_source(StreamTable& table, std::chrono::milliseconds timeout) {
    std::vector<std::string> source;
    std::vector<std::string> target;
    const bool equal = wait_until(
        [&] {
            source = table.source_rows();
            try {
                target = table.target_rows();
            } catch (const std::exception&) {
                return false;  // the target may not exist yet
            }
            return source == target;
        },
        timeout, std::chrono::milliseconds(300));
    if (equal) return ::testing::AssertionSuccess();

    auto failure = ::testing::AssertionFailure();
    failure << table.target() << "_current differs from " << table.qualified() << ": " << source.size()
            << " source rows, " << target.size() << " target rows";
    for (std::size_t i = 0; i < std::max(source.size(), target.size()); ++i) {
        const std::string s = i < source.size() ? source[i] : "(none)";
        const std::string t = i < target.size() ? target[i] : "(none)";
        if (s != t) {
            failure << "; first difference at row " << i << ": source " << s << ", target " << t;
            break;
        }
    }
    return failure;
}

}  // namespace recordflow::it
