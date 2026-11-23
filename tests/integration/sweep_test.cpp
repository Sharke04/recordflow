// [I7] Orphan sweep (decision 5.5): rows are in ClickHouse, recordflow is
// stopped, rows are deleted on the source and the slot is moved past those
// deletes (a lost slot). After a restart the deleted rows are gone from
// <target>_current through snapshot deletions, and the live rows are intact.

#include <signal.h>

#include <clickhouse/client.h>

#include <string>

#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;

std::uint64_t count(const std::string& sql) {
    std::uint64_t result = 0;
    clickhouse_admin()->Select(sql, [&](const clickhouse::Block& block) {
        if (block.GetRowCount() > 0) result = block[0]->As<clickhouse::ColumnUInt64>()->At(0);
    });
    return result;
}

TEST(Sweep, RemovesRowsDeletedBehindTheSlot) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table("it_sweep");
    const std::string t = table.qualified();
    table.pg().exec("INSERT INTO " + t + " SELECT g, 'row ' || g, g FROM generate_series(1, 500) g");

    auto first = table.start_streaming({{"RECORDFLOW_SNAPSHOT_CHUNK_SIZE", "100"}});
    ASSERT_TRUE(first->wait_for_log("Snapshot of " + t + " done", 60s)) << first->log();
    ASSERT_TRUE(target_equals_source(table, 20s));
    first->signal(SIGTERM);
    ASSERT_EQ(first->wait_exit(10s), 0) << first->log();

    // Deletes the stream will never deliver: the slot skips over them.
    table.pg().exec("DELETE FROM " + t + " WHERE id <= 50 OR id BETWEEN 300 AND 320");
    table.pg().exec("UPDATE " + t + " SET name = 'changed' WHERE id = 400");
    ASSERT_TRUE(wait_until(
        [&] {
            try {
                table.pg().exec("SELECT pg_replication_slot_advance('" + table.slot() + "', pg_current_wal_lsn())");
                return true;
            } catch (const std::exception&) {
                return false;  // the old walsender still holds the slot
            }
        },
        15s));
    EXPECT_EQ(table.target_rows().size(), 500u);

    auto second = table.start_streaming({{"RECORDFLOW_SNAPSHOT_CHUNK_SIZE", "100"}});
    ASSERT_TRUE(second->wait_for_log("Snapshot of " + t + " done", 60s)) << second->log();
    EXPECT_NE(second->log().find("71 orphan rows removed"), std::string::npos) << second->log();
    EXPECT_TRUE(target_equals_source(table, 20s));
    EXPECT_EQ(count("SELECT count() FROM " + table.target() +
                    " WHERE _rf_is_deleted = 1 AND _rf_source = 'snapshot' AND _rf_op = 'd'"),
              71u);

    second->signal(SIGTERM);
    EXPECT_EQ(second->wait_exit(10s), 0) << second->log();
}

}  // namespace
