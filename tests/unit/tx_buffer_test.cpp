// [U10] Transaction buffer and flush policy (decision 4.5): only complete
// transactions are written, abort() drops only the open one, flush returns the
// last complete commit_end, triggers wait for a transaction boundary and the
// hard limit forces a write inside a huge transaction.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <string>

#include "sink/flush_policy.hpp"
#include "sink/jsonl/jsonl_sink.hpp"
#include "sink/tx_buffer.hpp"

namespace {

using namespace recordflow;
using sink::FlushDecision;
using Clock = std::chrono::steady_clock;

core::ChangeEvent event(std::uint64_t seq) {
    core::ChangeEvent e;
    e.op = core::Op::Insert;
    e.schema_name = "public";
    e.table = "orders";
    e.key_columns = {"id"};
    e.key = {std::to_string(seq)};
    e.after = core::Row{{"id", {core::Value::Kind::Text, std::to_string(seq)}}};
    e.version = {100, seq};
    return e;
}

void transaction(sink::TxBuffer& buffer, int events, core::Lsn commit_end) {
    buffer.begin();
    for (int i = 0; i < events; ++i) buffer.append(event(static_cast<std::uint64_t>(i)));
    buffer.commit(commit_end);
}

TEST(TxBuffer, AbortDropsOnlyTheOpenTransaction) {
    sink::TxBuffer buffer;
    transaction(buffer, 2, 100);
    buffer.begin();
    buffer.append(event(7));
    EXPECT_EQ(buffer.stats().rows, 3u);
    EXPECT_EQ(buffer.stats().committed_rows, 2u);

    buffer.abort();
    EXPECT_FALSE(buffer.in_transaction());
    EXPECT_EQ(buffer.stats().rows, 2u);

    const auto drained = buffer.drain(false);
    ASSERT_EQ(drained.events.size(), 2u);
    EXPECT_EQ(drained.last_commit_end, 100u);
    EXPECT_EQ(buffer.stats().rows, 0u);
    EXPECT_FALSE(buffer.stats().oldest.has_value());
}

TEST(TxBuffer, DrainReturnsTheLastCompleteCommitAndKeepsTheOpenTransaction) {
    sink::TxBuffer buffer;
    transaction(buffer, 1, 100);
    transaction(buffer, 2, 200);
    buffer.begin();
    buffer.append(event(0));

    auto drained = buffer.drain(false);
    EXPECT_EQ(drained.events.size(), 3u);
    EXPECT_EQ(drained.last_commit_end, 200u);
    EXPECT_TRUE(buffer.in_transaction());
    EXPECT_EQ(buffer.stats().rows, 1u);
    EXPECT_EQ(buffer.stats().committed_rows, 0u);

    // Part of an oversized transaction: written, but no commit position.
    buffer.append(event(1));
    drained = buffer.drain(true);
    EXPECT_EQ(drained.events.size(), 2u);
    EXPECT_FALSE(drained.last_commit_end.has_value());
    EXPECT_TRUE(buffer.in_transaction());

    // Its Commit then reports the position, also for an empty remainder.
    buffer.commit(300);
    drained = buffer.drain(false);
    EXPECT_TRUE(drained.events.empty());
    EXPECT_EQ(drained.last_commit_end, 300u);
}

TEST(TxBuffer, JsonlSinkWritesCompleteTransactionsAndReportsTheirEnd) {
    std::FILE* out = std::tmpfile();
    ASSERT_NE(out, nullptr);
    sink::jsonl::JsonlSink sink(out);
    sink.begin();
    sink.append(event(0));
    sink.commit(500);
    sink.begin();
    sink.append(event(1));

    EXPECT_EQ(sink.flush(), 500u);
    std::rewind(out);
    char line[1024] = {};
    ASSERT_NE(std::fgets(line, sizeof line, out), nullptr);
    EXPECT_EQ(std::string(line).substr(0, 54), R"({"op":"c","source":"log","schema":"public","table":"or)");
    EXPECT_EQ(std::fgets(line, sizeof line, out), nullptr);  // the open transaction stays buffered
    std::fclose(out);
}

TEST(FlushPolicy, TriggersWaitForTheTransactionBoundary) {
    sink::FlushLimits limits;
    limits.max_rows = 10;
    limits.max_bytes = 1 << 20;
    limits.max_delay = std::chrono::milliseconds(1000);
    const sink::FlushPolicy policy(limits);
    const auto now = Clock::now();

    sink::BufferStats stats;
    EXPECT_EQ(policy.decide(stats, false, now), FlushDecision::None);  // empty buffer, even when old

    stats.rows = 10;
    stats.oldest = now;
    EXPECT_EQ(policy.decide(stats, true, now), FlushDecision::None);
    EXPECT_EQ(policy.decide(stats, false, now), FlushDecision::Complete);

    stats.rows = 1;
    stats.bytes = 1 << 20;
    EXPECT_EQ(policy.decide(stats, true, now), FlushDecision::None);
    EXPECT_EQ(policy.decide(stats, false, now), FlushDecision::Complete);

    stats.bytes = 100;
    stats.oldest = now - std::chrono::milliseconds(999);
    EXPECT_EQ(policy.decide(stats, false, now), FlushDecision::None);
    stats.oldest = now - std::chrono::milliseconds(1000);
    EXPECT_EQ(policy.decide(stats, true, now), FlushDecision::None);
    EXPECT_EQ(policy.decide(stats, false, now), FlushDecision::Complete);
}

TEST(FlushPolicy, HardLimitForcesAWriteInsideAHugeTransaction) {
    sink::FlushLimits limits;
    limits.max_rows = 10;
    limits.max_bytes = 1000;
    const sink::FlushPolicy policy(limits);
    const auto now = Clock::now();

    sink::BufferStats stats;
    stats.oldest = now;
    stats.rows = 39;
    EXPECT_EQ(policy.decide(stats, true, now), FlushDecision::None);
    stats.rows = 40;
    EXPECT_EQ(policy.decide(stats, true, now), FlushDecision::Oversized);

    stats.rows = 1;
    stats.bytes = 4000;
    EXPECT_EQ(policy.decide(stats, true, now), FlushDecision::Oversized);
}

}  // namespace
