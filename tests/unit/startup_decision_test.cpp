// [U12] Startup decisions (decisions 4.3 and 5.1): first start, a state file
// of another slot, table or cluster, a recreated target, a gap in the slot and
// the snapshot status against snapshot_on_start.

#include <gtest/gtest.h>

#include <string>

#include "app/startup_decision.hpp"

namespace {

using namespace recordflow;
using app::StartupAction;
using config::SnapshotOnStart;
using state::SnapshotStatus;

constexpr core::Lsn kFlushed = 0x5000;

state::State stored(SnapshotStatus status) {
    state::State s;
    s.source = {"7301", "recordflow_slot", "public.orders"};
    s.target = {"recordflow.public__orders", "uuid-a"};
    s.last_flushed_lsn = kFlushed;
    s.snapshot.status = status;
    if (status != SnapshotStatus::Pending) {
        s.snapshot.start_lsn = 0x1000;
        s.snapshot.max_pk = std::vector<std::string>{"900"};
        s.snapshot.last_pk = {status == SnapshotStatus::Done ? "900" : "400"};
        s.snapshot.version_lsn = 0x4000;
    }
    return s;
}

app::StartupFacts facts(std::optional<state::State> state, SnapshotOnStart on_start = SnapshotOnStart::Always) {
    app::StartupFacts f;
    f.stored = std::move(state);
    f.slot = "recordflow_slot";
    f.source_table = "public.orders";
    f.target_table = "recordflow.public__orders";
    f.system_id = "7301";
    f.target_uuid = "uuid-a";
    f.confirmed_flush_lsn = kFlushed - 0x100;  // Postgres may report an older confirmation
    f.snapshot_on_start = on_start;
    return f;
}

TEST(StartupDecision, FirstStartRecordsIdentityAndStartsASnapshot) {
    app::StartupFacts f = facts(std::nullopt);
    f.confirmed_flush_lsn = 0x7000;
    const auto d = app::decide_startup(f);
    EXPECT_EQ(d.action, StartupAction::NewSnapshot);
    ASSERT_TRUE(d.state.has_value());
    EXPECT_EQ(d.state->source.system_id, "7301");
    EXPECT_EQ(d.state->source.slot, "recordflow_slot");
    EXPECT_EQ(d.state->source.table, "public.orders");
    EXPECT_EQ(d.state->target.table, "recordflow.public__orders");
    EXPECT_EQ(d.state->target.table_uuid, "uuid-a");
    EXPECT_EQ(d.state->last_flushed_lsn, 0x7000u);
    EXPECT_EQ(d.state->snapshot.status, SnapshotStatus::Pending);
}

TEST(StartupDecision, RefusesAStateFileOfAnotherSlotTableOrCluster) {
    app::StartupFacts other_slot = facts(stored(SnapshotStatus::Done));
    other_slot.slot = "other_slot";
    app::StartupFacts other_table = facts(stored(SnapshotStatus::Done));
    other_table.source_table = "public.customers";
    app::StartupFacts other_cluster = facts(stored(SnapshotStatus::Done));
    other_cluster.system_id = "9999";

    for (const auto& f : {other_slot, other_table, other_cluster}) {
        const auto d = app::decide_startup(f);
        EXPECT_EQ(d.action, StartupAction::Refuse);
        EXPECT_FALSE(d.state.has_value());
        ASSERT_EQ(d.messages.size(), 1u);
    }
    EXPECT_NE(app::decide_startup(other_cluster).messages[0].find("9999"), std::string::npos);
}

TEST(StartupDecision, RecreatedTargetOrSlotGapResetsTheSnapshot) {
    app::StartupFacts recreated = facts(stored(SnapshotStatus::Done), SnapshotOnStart::IfIncomplete);
    recreated.target_uuid = "uuid-b";
    auto d = app::decide_startup(recreated);
    EXPECT_EQ(d.action, StartupAction::ResetSnapshot);
    EXPECT_EQ(d.state->target.table_uuid, "uuid-b");
    EXPECT_EQ(d.state->snapshot, app::fresh_snapshot());
    EXPECT_EQ(d.state->last_flushed_lsn, kFlushed);
    EXPECT_EQ(d.messages.size(), 1u);

    app::StartupFacts gap = facts(stored(SnapshotStatus::Running), SnapshotOnStart::IfIncomplete);
    gap.confirmed_flush_lsn = kFlushed + 0x10;
    d = app::decide_startup(gap);
    EXPECT_EQ(d.action, StartupAction::ResetSnapshot);
    EXPECT_EQ(d.state->snapshot.status, SnapshotStatus::Pending);
    EXPECT_TRUE(d.state->snapshot.last_pk.empty());
    EXPECT_EQ(d.state->last_flushed_lsn, kFlushed + 0x10);  // the gap is acknowledged

    // Equal positions are no gap.
    app::StartupFacts equal = facts(stored(SnapshotStatus::Done), SnapshotOnStart::IfIncomplete);
    equal.confirmed_flush_lsn = kFlushed;
    EXPECT_EQ(app::decide_startup(equal).action, StartupAction::Resume);
}

TEST(StartupDecision, SnapshotStatusAgainstSnapshotOnStart) {
    struct Case {
        SnapshotStatus status;
        SnapshotOnStart on_start;
        StartupAction expected;
    };
    const Case cases[] = {
        {SnapshotStatus::Pending, SnapshotOnStart::Always, StartupAction::NewSnapshot},
        {SnapshotStatus::Pending, SnapshotOnStart::IfIncomplete, StartupAction::NewSnapshot},
        {SnapshotStatus::Running, SnapshotOnStart::Always, StartupAction::Resume},
        {SnapshotStatus::Running, SnapshotOnStart::IfIncomplete, StartupAction::Resume},
        {SnapshotStatus::Done, SnapshotOnStart::Always, StartupAction::NewSnapshot},
        {SnapshotStatus::Done, SnapshotOnStart::IfIncomplete, StartupAction::Resume},
    };
    for (const Case& c : cases) {
        const state::State before = stored(c.status);
        const auto d = app::decide_startup(facts(before, c.on_start));
        SCOPED_TRACE(std::string(state::to_string(c.status)) +
                     (c.on_start == SnapshotOnStart::Always ? " / always" : " / if_incomplete"));
        EXPECT_EQ(d.action, c.expected);
        ASSERT_TRUE(d.state.has_value());
        EXPECT_EQ(d.state->last_flushed_lsn, kFlushed);  // an older confirmation is no gap
        if (c.expected == StartupAction::Resume) {
            EXPECT_EQ(d.state->snapshot, before.snapshot);  // running keeps start_lsn, max_pk, last_pk
        } else {
            EXPECT_EQ(d.state->snapshot, app::fresh_snapshot());
        }
        EXPECT_TRUE(d.messages.empty());
    }
}

}  // namespace
