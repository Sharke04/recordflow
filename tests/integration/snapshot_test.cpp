// [I6] Snapshot correctness under concurrent writes (decision 5, dblog.md
// 10.12): a table of a few thousand rows is read in small chunks while
// inserts, updates, deletes and key changes run on it; once the snapshot is
// done and the stream has caught up, <target>_current equals the source.

#include <signal.h>

#include <atomic>
#include <string>
#include <thread>

#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;

constexpr int kRows = 4000;

TEST(Snapshot, CorrectUnderConcurrentWrites) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table("it_snap");
    const std::string t = table.qualified();
    table.pg().exec("INSERT INTO " + t + " SELECT g, 'seed ' || g, g FROM generate_series(1, " +
                    std::to_string(kRows) + ") g");

    std::atomic<bool> stop{false};
    std::atomic<int> transactions{0};
    std::thread writer([&] {
        PgAdmin pg;  // its own connection
        for (int i = 1; !stop; ++i) {
            const auto id = [&](int salt) { return std::to_string(1 + (i * 7919 + salt) % kRows); };
            // Spread over the whole key range, so that open windows see them.
            pg.exec("BEGIN; "
                    "UPDATE " + t + " SET amount = amount + 1, name = 'u" + std::to_string(i) + "' WHERE id = " +
                    id(1) + "; "
                    "DELETE FROM " + t + " WHERE id = " + id(2) + "; "
                    "INSERT INTO " + t + " VALUES (" + id(3) + ", 'back " + std::to_string(i) +
                    "', 3) ON CONFLICT (id) DO NOTHING; "
                    "INSERT INTO " + t + " VALUES (" + std::to_string(kRows + i) + ", 'new', 1); "
                    // A key change inside the range: the old and the new key.
                    "UPDATE " + t + " SET id = id + 1000000 WHERE id = " + id(4) + " AND NOT EXISTS (SELECT 1 FROM " +
                    t + " WHERE id = " + id(4) + " + 1000000); "
                    "COMMIT");
            ++transactions;
            std::this_thread::sleep_for(2ms);
        }
    });

    auto process = table.start_streaming({{"RECORDFLOW_SNAPSHOT_CHUNK_SIZE", "100"}});
    const bool done = process->wait_for_log("Snapshot of " + t + " done", 120s);
    stop = true;
    writer.join();
    ASSERT_TRUE(done) << process->log();
    EXPECT_GT(transactions.load(), 10) << "the writes did not overlap the snapshot";

    EXPECT_TRUE(target_equals_source(table, 30s));
    const auto state = table.state();
    ASSERT_TRUE(state);
    EXPECT_EQ(state->snapshot.status, recordflow::state::SnapshotStatus::Done);
    ASSERT_TRUE(state->snapshot.max_pk);
    EXPECT_GE(std::stol(state->snapshot.max_pk->at(0)), kRows);

    process->signal(SIGTERM);
    EXPECT_EQ(process->wait_exit(10s), 0) << process->log();
}

}  // namespace
