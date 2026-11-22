// Spike from decision 3: ReplacingMergeTree must accept a UInt128 version and
// keep the row with the highest version, independent of insertion order.

#include <clickhouse/client.h>

#include <string>
#include <utility>
#include <vector>

#include "support/support.hpp"

namespace {

using clickhouse::UInt128;

UInt128 version(std::uint64_t commit_lsn, std::uint64_t seq) {
    return absl::MakeUint128(commit_lsn, seq);
}

void insert(clickhouse::Client& client, const std::string& table, std::int64_t id,
            const std::string& value, UInt128 ver, std::uint8_t deleted) {
    auto ids = std::make_shared<clickhouse::ColumnInt64>();
    auto values = std::make_shared<clickhouse::ColumnString>();
    auto versions = std::make_shared<clickhouse::ColumnUInt128>();
    auto deletes = std::make_shared<clickhouse::ColumnUInt8>();
    ids->Append(id);
    values->Append(value);
    versions->Append(ver);
    deletes->Append(deleted);

    clickhouse::Block block;
    block.AppendColumn("id", ids);
    block.AppendColumn("val", values);
    block.AppendColumn("_rf_version", versions);
    block.AppendColumn("_rf_is_deleted", deletes);
    client.Insert(table, block);
}

std::vector<std::pair<std::int64_t, std::string>> current_rows(clickhouse::Client& client,
                                                               const std::string& table) {
    std::vector<std::pair<std::int64_t, std::string>> rows;
    client.Select("SELECT id, val FROM " + table + " FINAL WHERE _rf_is_deleted = 0 ORDER BY id",
                  [&](const clickhouse::Block& block) {
                      for (size_t i = 0; i < block.GetRowCount(); ++i) {
                          rows.emplace_back(block[0]->As<clickhouse::ColumnInt64>()->At(i),
                                            std::string(block[1]->As<clickhouse::ColumnString>()->At(i)));
                      }
                  });
    return rows;
}

}  // namespace

TEST(ClickHouseVersioning, ReplacingMergeTreeKeepsHighestUInt128Version) {
    RECORDFLOW_REQUIRE_INTEGRATION();

    auto client = recordflow::it::clickhouse_admin();
    const std::string table = "recordflow." + recordflow::it::unique_name("it_version");
    client->Execute("CREATE TABLE " + table +
                    " (id Int64, val String, _rf_version UInt128, _rf_is_deleted UInt8)"
                    " ENGINE = ReplacingMergeTree(_rf_version, _rf_is_deleted) ORDER BY id");

    // Key 1: the newer version is inserted first. Its seq is lower, so a
    // comparison truncated to the low 64 bits would pick the wrong row.
    insert(*client, table, 1, "new", version(16, 1), 0);
    insert(*client, table, 1, "old", version(15, 999), 0);
    insert(*client, table, 1, "new", version(16, 1), 0);  // replayed duplicate

    // Key 2: inserted, then deleted with a higher version.
    insert(*client, table, 2, "gone", version(20, 0), 0);
    insert(*client, table, 2, "", version(21, 0), 1);

    const auto before_merge = current_rows(*client, table);
    client->Execute("OPTIMIZE TABLE " + table + " FINAL");
    const auto after_merge = current_rows(*client, table);
    client->Execute("DROP TABLE " + table);

    const std::vector<std::pair<std::int64_t, std::string>> expected{{1, "new"}};
    EXPECT_EQ(before_merge, expected);
    EXPECT_EQ(after_merge, expected);
}
