// [U15] Snapshot runner over a scripted stream (decisions 5.2-5.4): a fake
// query connection answers the chunk SELECT, the test feeds the messages that
// the stream would carry. L -> changes -> H -> Commit yields the surviving rows
// as READ events with the H version; old or foreign watermarks are ignored; a
// transaction the SELECT did not see drops the chunk; an empty result or a
// short chunk completes the pass.

#include <gtest/gtest.h>

#include <deque>
#include <string>
#include <vector>

#include "snapshot/snapshot_runner.hpp"

namespace {

using namespace recordflow;
using snapshot::PassStatus;
using snapshot::SnapshotRunner;
using Clock = SnapshotRunner::Clock;

core::TableSchema orders() {
    core::TableSchema table;
    table.schema = "public";
    table.name = "orders";
    table.oid = 16384;
    table.columns = {{"id", 20, -1, "bigint", true, false}, {"note", 25, -1, "text", false, false}};
    table.primary_key = {"id"};
    return table;
}

core::Row row(int id) {
    return core::Row{{"id", {core::Value::Kind::Text, std::to_string(id)}},
                     {"note", {core::Value::Kind::Text, "n" + std::to_string(id)}}};
}

class FakeQueries : public pg::SnapshotQueries {
public:
    void emit_watermark(const pg::Watermark& mark) override { marks.push_back(mark); }

    pg::ChunkResult select_chunk(const core::TableSchema&, const pg::ChunkRequest& request) override {
        requests.push_back(request);
        pg::ChunkResult result;
        result.snapshot = snapshot;
        if (request.read_max) result.max_pk = max_pk;
        for (int id : next_rows) result.rows.push_back(row(id));
        return result;
    }

    std::string snapshot = "100:100:";
    std::optional<std::vector<std::string>> max_pk = std::vector<std::string>{"5"};
    std::vector<int> next_rows;
    std::vector<pg::Watermark> marks;
    std::vector<pg::ChunkRequest> requests;
};

pgoutput::LogicalMessage message(const std::string& content, const std::string& prefix = "recordflow") {
    return pgoutput::LogicalMessage{true, 0, prefix, content};
}

core::ChangeEvent change(core::Op op, int id, core::Xid xid) {
    core::ChangeEvent event;
    event.op = op;
    event.schema_name = "public";
    event.table = "orders";
    event.xid = xid;
    (op == core::Op::Delete ? event.before : event.after) = row(id);
    return event;
}

class SnapshotRunnerTest : public ::testing::Test {
protected:
    SnapshotRunnerTest() : runner(orders(), {"slot_a", 3, std::chrono::milliseconds(0), std::chrono::seconds(60)}) {
        state::SnapshotProgress pending;
        runner.start(pending);
    }

    void open(std::vector<int> rows, std::optional<core::Xid> open_xid = std::nullopt) {
        queries.next_rows = std::move(rows);
        runner.open_window(queries, recent, open_xid, now);
    }
    // The watermark transactions as the stream delivers them: B, M, C.
    void low(core::Lsn lsn) {
        runner.on_message(message(pg::format_watermark(queries.marks.at(queries.marks.size() - 2))), lsn);
        EXPECT_FALSE(runner.on_commit({lsn, 0}, now));
    }
    std::optional<snapshot::AcceptedChunk> high(core::Lsn lsn, std::int64_t ts = 777) {
        runner.on_message(message(pg::format_watermark(queries.marks.back())), lsn);
        return runner.on_commit({lsn, ts}, now);
    }

    FakeQueries queries;
    SnapshotRunner runner;
    std::deque<core::Xid> recent;
    Clock::time_point now = Clock::now();
};

std::vector<std::string> ids(const snapshot::AcceptedChunk& chunk) {
    std::vector<std::string> out;
    for (const auto& event : chunk.rows) out.push_back(event.key.at(0));
    return out;
}

TEST_F(SnapshotRunnerTest, WindowYieldsSurvivorsWithTheHighWatermarkVersion) {
    ASSERT_TRUE(runner.ready_for_chunk(now));
    open({1, 2, 3});
    ASSERT_EQ(queries.marks.size(), 2u);
    EXPECT_EQ(queries.marks[0].kind, pg::WatermarkKind::Low);
    EXPECT_EQ(queries.marks[1].kind, pg::WatermarkKind::High);
    EXPECT_EQ(queries.marks[0].id, queries.marks[1].id);
    EXPECT_EQ(queries.marks[0].slot, "slot_a");
    EXPECT_TRUE(queries.requests[0].read_max);
    EXPECT_TRUE(queries.requests[0].after.empty());
    EXPECT_EQ(queries.requests[0].limit, 3u);
    EXPECT_FALSE(runner.ready_for_chunk(now));

    // Changes before L do not touch the chunk.
    runner.on_change(change(core::Op::Update, 1, 90));
    low(0x500);
    runner.on_change(change(core::Op::Update, 2, 101));
    EXPECT_FALSE(runner.on_commit({0x510, 0}, now));  // an ordinary transaction inside the window
    const auto accepted = high(0x600, 1234);

    ASSERT_TRUE(accepted);
    EXPECT_EQ(ids(*accepted), (std::vector<std::string>{"1", "3"}));
    for (const auto& event : accepted->rows) {
        EXPECT_EQ(event.op, core::Op::Read);
        EXPECT_EQ(event.source, core::EventSource::Snapshot);
        EXPECT_EQ(event.version, (core::Version{0x600, 0}));
        EXPECT_EQ(event.commit_ts, 1234);
        EXPECT_EQ(event.key_columns, std::vector<std::string>{"id"});
        ASSERT_TRUE(event.after);
        EXPECT_EQ(event.after->at(1).second.text, "n" + event.key[0]);
    }
    // The cursor is the last raw key; start_lsn is the first L, max_pk was read.
    EXPECT_EQ(accepted->progress.last_pk, std::vector<std::string>{"3"});
    EXPECT_EQ(accepted->progress.start_lsn, core::Lsn{0x500});
    EXPECT_EQ(accepted->progress.max_pk, (std::vector<std::string>{"5"}));
    EXPECT_EQ(accepted->progress.version_lsn, core::Lsn{0x600});
    EXPECT_EQ(accepted->progress.status, state::SnapshotStatus::Running);
    EXPECT_EQ(runner.status(), PassStatus::Running);
    EXPECT_EQ(runner.xid_horizon(), core::Xid{100});

    // The next chunk continues after the cursor, bounded by max_pk.
    ASSERT_TRUE(runner.ready_for_chunk(now));
    open({4, 5});
    EXPECT_FALSE(queries.requests[1].read_max);
    EXPECT_EQ(queries.requests[1].after, std::vector<std::string>{"3"});
    EXPECT_EQ(queries.requests[1].upto, (std::vector<std::string>{"5"}));
}

TEST_F(SnapshotRunnerTest, OldAndForeignWatermarksAreIgnored) {
    open({1, 2, 3});
    const pg::Watermark ours_low = queries.marks[0];
    pg::Watermark old_low = ours_low;
    old_low.id = "00000000-0000-4000-8000-000000000000";
    pg::Watermark other_slot = ours_low;
    other_slot.slot = "slot_b";

    runner.on_message(message(pg::format_watermark(old_low)), 0x400);
    runner.on_message(message(pg::format_watermark(other_slot)), 0x401);
    runner.on_message(message(pg::format_watermark(ours_low), "other_app"), 0x402);
    runner.on_message(message("not a watermark"), 0x403);
    // Still waiting for L: a change now is not removed from the chunk.
    runner.on_change(change(core::Op::Delete, 2, 99));
    pg::Watermark ours_high = queries.marks[1];
    runner.on_message(message(pg::format_watermark(ours_high)), 0x404);
    EXPECT_FALSE(runner.on_commit({0x404, 0}, now));

    low(0x500);
    const auto accepted = high(0x600);
    ASSERT_TRUE(accepted);
    EXPECT_EQ(ids(*accepted), (std::vector<std::string>{"1", "2", "3"}));
    EXPECT_EQ(accepted->progress.start_lsn, core::Lsn{0x500});

    // After acceptance the same watermarks no longer match.
    runner.on_message(message(pg::format_watermark(ours_high)), 0x700);
    EXPECT_FALSE(runner.on_commit({0x700, 0}, now));
}

TEST_F(SnapshotRunnerTest, TransactionsTheSelectDidNotSeeDropTheChunk) {
    queries.snapshot = "100:110:104";

    // Rule 1: committed before the pause, but still in progress for the SELECT.
    recent = {90, 104};
    open({1, 2, 3});
    EXPECT_EQ(queries.marks.size(), 1u);  // no H for a dropped chunk
    EXPECT_EQ(runner.retries(), 1);
    EXPECT_TRUE(runner.ready_for_chunk(now));

    // The transaction being decoded at the pause counts as well.
    recent = {90};
    open({1, 2, 3}, core::Xid{112});
    EXPECT_EQ(runner.retries(), 2);

    // Rule 2: committed after the pause and before L, not visible.
    open({1, 2, 3});
    runner.on_change(change(core::Op::Insert, 9, 110));
    EXPECT_EQ(runner.retries(), 3);
    low(0x500);  // the dropped window's L is ignored
    EXPECT_FALSE(high(0x600));

    // A visible one before L and anything inside the window are fine.
    open({1, 2, 3});
    runner.on_change(change(core::Op::Insert, 9, 105));
    low(0x700);
    runner.on_change(change(core::Op::Insert, 3, 104));
    const auto accepted = high(0x800);
    ASSERT_TRUE(accepted);
    EXPECT_EQ(ids(*accepted), (std::vector<std::string>{"1", "2"}));
    EXPECT_EQ(accepted->progress.start_lsn, core::Lsn{0x700});
    EXPECT_EQ(runner.retries(), 0);
}

TEST_F(SnapshotRunnerTest, RepeatedFailuresStopTheSnapshot) {
    queries.snapshot = "100:110:104";
    recent = {104};
    for (int i = 1; i < SnapshotRunner::kMaxRetries; ++i) open({1});
    EXPECT_THROW(open({1}), std::runtime_error);
}

TEST_F(SnapshotRunnerTest, WindowTimeoutDropsTheChunk) {
    open({1, 2, 3});
    runner.on_tick(now + std::chrono::seconds(59));
    EXPECT_TRUE(runner.window_open());
    runner.on_tick(now + std::chrono::seconds(60));
    EXPECT_FALSE(runner.window_open());
    EXPECT_EQ(runner.retries(), 1);
}

TEST_F(SnapshotRunnerTest, ShortChunkCompletesThePass) {
    open({1, 2, 3});
    low(0x500);
    ASSERT_TRUE(high(0x600));
    EXPECT_EQ(runner.status(), PassStatus::Running);

    open({4});  // fewer rows than chunk_size
    low(0x700);
    runner.on_change(change(core::Op::Delete, 4, 120));  // removed, but the raw count decides
    const auto last = high(0x800, 4321);
    ASSERT_TRUE(last);
    EXPECT_TRUE(last->rows.empty());
    EXPECT_EQ(last->progress.last_pk, std::vector<std::string>{"4"});
    EXPECT_EQ(runner.status(), PassStatus::Complete);
    EXPECT_FALSE(runner.ready_for_chunk(now));

    const snapshot::SweepBounds bounds = runner.sweep_bounds();
    EXPECT_EQ(bounds.below, (core::Version{0x500, 0}));
    EXPECT_EQ(bounds.tombstone, (core::Version{0x800, 1}));
    EXPECT_EQ(bounds.commit_ts, 4321);

    runner.finish();
    EXPECT_EQ(runner.status(), PassStatus::Inactive);
    EXPECT_EQ(runner.progress().status, state::SnapshotStatus::Done);
}

TEST_F(SnapshotRunnerTest, EmptyTableStillPassesAWindow) {
    queries.max_pk.reset();
    open({});
    low(0x500);
    const auto accepted = high(0x600);
    ASSERT_TRUE(accepted);
    EXPECT_TRUE(accepted->rows.empty());
    EXPECT_EQ(accepted->progress.start_lsn, core::Lsn{0x500});
    EXPECT_FALSE(accepted->progress.max_pk);
    EXPECT_TRUE(accepted->progress.last_pk.empty());
    EXPECT_EQ(runner.status(), PassStatus::Complete);
}

TEST_F(SnapshotRunnerTest, ResumeKeepsStartAndBound) {
    state::SnapshotProgress running;
    running.status = state::SnapshotStatus::Running;
    running.start_lsn = 0x300;
    running.max_pk = std::vector<std::string>{"5"};
    running.last_pk = {"3"};
    running.version_lsn = 0x350;
    runner.start(running);

    open({4, 5});
    EXPECT_FALSE(queries.requests[0].read_max);
    EXPECT_EQ(queries.requests[0].after, std::vector<std::string>{"3"});
    low(0x900);
    const auto accepted = high(0xA00);
    ASSERT_TRUE(accepted);
    EXPECT_EQ(accepted->progress.start_lsn, core::Lsn{0x300});
    EXPECT_EQ(accepted->progress.last_pk, std::vector<std::string>{"5"});
    EXPECT_EQ(runner.status(), PassStatus::Complete);
}

TEST_F(SnapshotRunnerTest, AbortedWindowIsReadAgain) {
    open({1, 2, 3});
    low(0x500);
    runner.abort_window("lost connection");
    EXPECT_FALSE(high(0x600));
    EXPECT_TRUE(runner.ready_for_chunk(now));
    open({1, 2, 3});
    EXPECT_TRUE(queries.requests.back().after.empty());
    EXPECT_TRUE(queries.requests.back().read_max);
}

}  // namespace
