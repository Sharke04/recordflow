// [I5] Graceful shutdown (decision 8.4): SIGTERM in the middle of a stream
// ends recordflow with exit code 0 within shutdown.timeout_s; the state file
// and the slot agree (last_flushed_lsn = confirmed_flush_lsn), and the next
// start does not write again what was already stored.

#include <clickhouse/client.h>
#include <signal.h>

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

#include "core/version.hpp"
#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;

// Rows of the base table written by transactions committed up to `lsn`.
std::uint64_t rows_up_to(const StreamTable& table, recordflow::core::Lsn lsn) {
    std::uint64_t count = 0;
    clickhouse_admin()->Select(
        "SELECT count() FROM " + table.target() + " WHERE bitShiftRight(_rf_version, 64) <= " + std::to_string(lsn),
        [&](const clickhouse::Block& block) {
            if (block.GetRowCount() > 0) count = block[0]->As<clickhouse::ColumnUInt64>()->At(0);
        });
    return count;
}

TEST(Shutdown, SigtermStoresAndConfirmsAndDoesNotReplay) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table;
    const std::string t = table.qualified();

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        PgAdmin pg;
        for (int i = 1; !stop; ++i) {
            pg.exec("INSERT INTO " + t + " VALUES (" + std::to_string(i) + ", 'w', " + std::to_string(i) + ")");
            std::this_thread::sleep_for(5ms);
        }
    });

    auto first = table.start_streaming();
    std::this_thread::sleep_for(1500ms);
    const auto signalled = std::chrono::steady_clock::now();
    first->signal(SIGTERM);
    EXPECT_EQ(first->wait_exit(10s), 0) << first->log();
    EXPECT_LT(std::chrono::steady_clock::now() - signalled, 8s);  // shutdown.timeout_s
    stop = true;
    writer.join();

    const std::string flushed = table.state_last_flushed_lsn();
    EXPECT_EQ(flushed, table.slot_confirmed_lsn());
    const auto lsn = recordflow::core::parse_lsn(flushed);
    ASSERT_TRUE(lsn.has_value()) << flushed;

    // Merges would hide duplicates: from here on only inserts change counts.
    clickhouse_admin()->Execute("SYSTEM STOP MERGES " + table.target());
    const std::uint64_t stored = rows_up_to(table, *lsn);
    EXPECT_GT(stored, 0u);

    // The rows written after the shutdown arrive; nothing before it again.
    auto second = table.start_streaming();
    EXPECT_NE(second->log().find("at " + flushed), std::string::npos) << second->log();
    EXPECT_TRUE(target_equals_source(table));
    second->signal(SIGTERM);
    EXPECT_EQ(second->wait_exit(10s), 0) << second->log();
    EXPECT_EQ(rows_up_to(table, *lsn), stored);
    EXPECT_EQ(table.state_last_flushed_lsn(), table.slot_confirmed_lsn());
}

}  // namespace
