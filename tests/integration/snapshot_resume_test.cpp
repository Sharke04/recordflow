// [I8] Snapshot resume after a hard kill (decisions 5.1 and 5.3): SIGKILL in
// the middle of a snapshot (status running, last_pk set), then a restart: the
// snapshot continues after last_pk with the same start_lsn and max_pk, and
// <target>_current ends up equal to the source.

#include <signal.h>

#include <atomic>
#include <string>
#include <thread>

#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;
using recordflow::state::SnapshotStatus;

constexpr int kRows = 2000;

TEST(SnapshotResume, ContinuesAfterLastPkWithTheSameBounds) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table("it_resume");
    const std::string t = table.qualified();
    table.pg().exec("INSERT INTO " + t + " SELECT g, 'seed ' || g, g FROM generate_series(1, " +
                    std::to_string(kRows) + ") g");

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        PgAdmin pg;
        for (int i = 1; !stop; ++i) {
            pg.exec("BEGIN; UPDATE " + t + " SET amount = amount + 1 WHERE id = " +
                    std::to_string(1 + (i * 613) % kRows) + "; DELETE FROM " + t + " WHERE id = " +
                    std::to_string(1 + (i * 389) % kRows) + "; INSERT INTO " + t + " VALUES (" +
                    std::to_string(kRows + i) + ", 'new', 1); COMMIT");
            std::this_thread::sleep_for(10ms);
        }
    });

    // Slow chunks, so that the kill lands in the middle of the pass.
    const std::map<std::string, std::string> slow{{"RECORDFLOW_SNAPSHOT_CHUNK_SIZE", "50"},
                                                  {"RECORDFLOW_SNAPSHOT_CHUNK_DELAY_MS", "100"}};
    auto first = table.start_streaming(slow);
    std::optional<recordflow::state::State> before;
    ASSERT_TRUE(wait_until(
        [&] {
            before = table.state();
            return before && before->snapshot.status == SnapshotStatus::Running &&
                   !before->snapshot.last_pk.empty() && std::stol(before->snapshot.last_pk[0]) >= 200;
        },
        30s, 20ms))
        << first->log();
    first->kill_hard();
    EXPECT_EQ(first->wait_exit(10s), 128 + SIGKILL);

    // The state file as the kill left it (it is replaced atomically).
    before = table.state();
    ASSERT_TRUE(before);
    ASSERT_EQ(before->snapshot.status, SnapshotStatus::Running);
    ASSERT_FALSE(before->snapshot.last_pk.empty());
    ASSERT_LT(std::stol(before->snapshot.last_pk[0]), kRows) << "the snapshot finished before the kill";
    ASSERT_TRUE(before->snapshot.start_lsn);
    ASSERT_TRUE(before->snapshot.max_pk);

    auto second = table.start_streaming();
    ASSERT_TRUE(second->wait_for_log("Snapshot of " + t + " done", 60s)) << second->log();
    EXPECT_NE(second->log().find("resumes after (" + before->snapshot.last_pk[0] + ")"), std::string::npos)
        << second->log();
    stop = true;
    writer.join();

    EXPECT_TRUE(target_equals_source(table, 30s));
    const auto after = table.state();
    ASSERT_TRUE(after);
    EXPECT_EQ(after->snapshot.status, SnapshotStatus::Done);
    EXPECT_EQ(after->snapshot.start_lsn, before->snapshot.start_lsn);
    EXPECT_EQ(after->snapshot.max_pk, before->snapshot.max_pk);
    EXPECT_EQ(after->snapshot.last_pk, *before->snapshot.max_pk);

    second->signal(SIGTERM);
    EXPECT_EQ(second->wait_exit(10s), 0) << second->log();
}

}  // namespace
