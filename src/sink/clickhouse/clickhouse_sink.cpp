#include "sink/clickhouse/clickhouse_sink.hpp"

#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/error_codes.h>

#include <algorithm>
#include <format>
#include <functional>
#include <stdexcept>
#include <string_view>
#include <thread>

#include "runtime/errors.hpp"
#include "runtime/log.hpp"
#include "sink/clickhouse/ddl.hpp"

namespace recordflow::sink::clickhouse {

namespace ch = ::clickhouse;

namespace {

std::string column_string(const ch::Block& block, std::size_t column, std::size_t row) {
    return std::string(block[column]->As<ch::ColumnString>()->At(row));
}

// "id, `odd name`" or "(id)" -> {"id", "odd name"} or {"id"}
std::vector<std::string> parse_sorting_key(std::string_view text) {
    if (text.size() >= 2 && text.front() == '(' && text.back() == ')') text = text.substr(1, text.size() - 2);
    std::vector<std::string> names;
    while (!text.empty()) {
        const auto comma = text.find(", ");
        std::string_view part = text.substr(0, comma);
        if (part.size() >= 2 && part.front() == '`' && part.back() == '`') part = part.substr(1, part.size() - 2);
        names.emplace_back(part);
        if (comma == std::string_view::npos) break;
        text.remove_prefix(comma + 2);
    }
    return names;
}

}  // namespace

ClickHouseSink::ClickHouseSink(const config::ClickHouseSection& section, const config::RetrySection& retry)
    : section_(section), client_(client_options(section)), backoff_("ClickHouse insert", retry) {}

std::string ClickHouseSink::target() const {
    return qualified_name(section_.database.value, table_);
}

bool ClickHouseSink::database_exists() {
    bool exists = false;
    client_.Select("SELECT name FROM system.databases WHERE name = " + quote_string(section_.database.value),
                   [&](const ch::Block& block) { exists = exists || block.GetRowCount() > 0; });
    return exists;
}

std::optional<ClickHouseSink::ExistingTable> ClickHouseSink::read_table() {
    const std::string database = quote_string(section_.database.value);
    const std::string name = quote_string(table_);
    std::optional<ExistingTable> table;
    client_.Select(std::format("SELECT toString(uuid), sorting_key FROM system.tables "
                               "WHERE database = {} AND name = {}", database, name),
                   [&](const ch::Block& block) {
                       if (block.GetRowCount() == 0 || table) return;
                       table.emplace();
                       table->uuid = column_string(block, 0, 0);
                       table->schema.sorting_key = parse_sorting_key(column_string(block, 1, 0));
                   });
    if (!table) return std::nullopt;

    std::vector<core::TargetColumn> meta;
    client_.Select(std::format("SELECT name, type FROM system.columns "
                               "WHERE database = {} AND table = {} ORDER BY position", database, name),
                   [&](const ch::Block& block) {
                       for (std::size_t row = 0; row < block.GetRowCount(); ++row) {
                           core::TargetColumn column{column_string(block, 0, row), column_string(block, 1, row), ""};
                           (column.name.starts_with(kMetaPrefix) ? meta : table->schema.columns)
                               .push_back(std::move(column));
                       }
                   });

    for (const auto& expected : kMetaColumns) {
        const auto found = std::find_if(meta.begin(), meta.end(),
                                        [&](const core::TargetColumn& c) { return c.name == expected.name; });
        if (found == meta.end()) {
            table->meta_diff.lines.push_back(std::format("- missing Recordflow column \"{}\" ({})",
                                                         expected.name, expected.type));
        } else if (found->type != expected.type) {
            table->meta_diff.lines.push_back(std::format("~ Recordflow column \"{}\": {} expected {}",
                                                         expected.name, found->type, expected.type));
        }
    }
    return table;
}

// The database is created once by an administrator (decision 10.2).
InterventionError ClickHouseSink::missing_database() const {
    return InterventionError(std::format(
        "ClickHouse database '{0}' does not exist (or user '{1}' has no access to it). "
        "An administrator creates it once:\n"
        "    CREATE DATABASE {0};\n"
        "    CREATE USER {1} IDENTIFIED WITH sha256_password BY '...';\n"
        "    GRANT CREATE TABLE, CREATE VIEW, INSERT, SELECT, TRUNCATE ON {0}.* TO {1};",
        section_.database.value, section_.user.value));
}

PrepareResult ClickHouseSink::prepare(const core::TableSchema& source) {
    source_ = source;
    table_ = target_table_name(section_, source);
    rows_.emplace(source);
    const std::string& database = section_.database.value;

    try {
        if (!database_exists()) throw missing_database();

        PrepareResult result;
        result.target = database + "." + table_;
        std::optional<ExistingTable> existing = read_table();
        if (!existing) {
            log::info("Creating ClickHouse table {} and view {}_current.", target(), table_);
            client_.Execute(create_table_sql(database, table_, source));
            result.created = true;
            existing = read_table();
            if (!existing) throw std::runtime_error(std::format("ClickHouse table {} was not created", target()));
        }
        // Also when the table exists: the view may have been dropped on its own.
        client_.Execute(create_view_sql(database, table_));

        result.table_identity = existing->uuid;
        result.diff = core::compare(existing->schema, core::derive_target(source));
        result.diff.lines.insert(result.diff.lines.end(), existing->meta_diff.lines.begin(),
                                 existing->meta_diff.lines.end());
        return result;
    } catch (const ServerError& e) {
        if (e.code() == ch::ErrorCodes::UNKNOWN_DATABASE) throw missing_database();
        if (e.code() == ch::ErrorCodes::ACCESS_DENIED) {
            throw InterventionError(std::format(
                "{} (user '{}' needs CREATE TABLE, CREATE VIEW, INSERT, SELECT, TRUNCATE ON {}.*)", e.what(),
                section_.user.value, database));
        }
        throw;
    }
}

void ClickHouseSink::with_retry(const std::function<void()>& write) {
    // Writes are idempotent (decision 2.2): the same Block or statement is retried.
    while (true) {
        try {
            write();
            backoff_.success();
            return;
        } catch (const TransientError& e) {
            client_.ResetConnection();
            std::this_thread::sleep_for(backoff_.failure(e.what()));
        }
    }
}

void ClickHouseSink::write_rows() {
    if (rows_->rows() == 0) return;
    const std::size_t rows = rows_->rows();
    const ch::Block block = rows_->take();
    with_retry([&] { client_.Insert(target(), block); });
    log::debug("Wrote {} rows to {}.", rows, target());
}

std::optional<core::Lsn> ClickHouseSink::flush(bool include_open) {
    if (!rows_) throw std::logic_error("ClickHouseSink::flush before prepare");
    TxBuffer::Drained drained = buffer_.drain(include_open);
    for (const auto& event : drained.events) {
        if (event.op != core::Op::Truncate) {
            rows_->add(event);
            continue;
        }
        if (event.schema_name != source_.schema || event.table != source_.name) continue;  // decision 4.4
        // At its stream position (decisions 2.5 and 9.4): the rows before it
        // are written first, the rows after it go into the next block.
        write_rows();
        with_retry([&] { client_.Execute("TRUNCATE TABLE " + target()); });
        log::info("Truncated {} (TRUNCATE of {} at {}).", target(), source_.qualified(), event.version.str());
    }
    write_rows();
    return drained.last_commit_end;
}

void ClickHouseSink::sweep(core::Version, core::Version) {
    throw std::logic_error("ClickHouseSink::sweep is not implemented yet");
}

}  // namespace recordflow::sink::clickhouse
