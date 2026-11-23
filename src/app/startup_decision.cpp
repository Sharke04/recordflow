#include "app/startup_decision.hpp"

#include <format>
#include <utility>

namespace recordflow::app {

namespace {

StartupDecision refuse(std::string message) {
    StartupDecision decision;
    decision.action = StartupAction::Refuse;
    decision.messages.push_back(std::move(message));
    return decision;
}

}  // namespace

state::SnapshotProgress fresh_snapshot() {
    return state::SnapshotProgress{};
}

StartupDecision decide_startup(const StartupFacts& facts) {
    StartupDecision decision;

    if (!facts.stored) {
        // First start: everything before the slot's position is covered by the
        // snapshot that follows.
        state::State fresh;
        fresh.source = {facts.system_id, facts.slot, facts.source_table};
        fresh.target = {facts.target_table, facts.target_uuid};
        fresh.last_flushed_lsn = facts.confirmed_flush_lsn;
        fresh.snapshot = fresh_snapshot();
        decision.action = StartupAction::NewSnapshot;
        decision.state = std::move(fresh);
        return decision;
    }

    const state::State& stored = *facts.stored;
    if (stored.source.slot != facts.slot || stored.source.table != facts.source_table) {
        return refuse(std::format(
            "the state file belongs to slot '{}' and table {}, but the configuration names slot '{}' and table "
            "{}. Use another data directory for this configuration, or remove the state file to start over",
            stored.source.slot, stored.source.table, facts.slot, facts.source_table));
    }
    if (stored.source.system_id != facts.system_id) {
        return refuse(std::format(
            "the source is another PostgreSQL cluster (system_id {}, the state file has {}); its LSNs start over, "
            "so new versions would lose against stored ones. To start over: drop the ClickHouse table {} and its "
            "_current view, remove the state file and start Recordflow again (full snapshot)",
            facts.system_id, stored.source.system_id, stored.target.table));
    }

    state::State next = stored;
    next.target.table = facts.target_table;
    next.target.table_uuid = facts.target_uuid;
    bool reset = false;

    if (stored.target.table != facts.target_table || stored.target.table_uuid != facts.target_uuid) {
        decision.messages.push_back(std::format(
            "the target table {} (uuid {}) is not the one in the state file ({}, uuid {}); it was dropped or "
            "recreated, so the snapshot starts over",
            facts.target_table, facts.target_uuid.empty() ? "none" : facts.target_uuid, stored.target.table,
            stored.target.table_uuid.empty() ? "none" : stored.target.table_uuid));
        reset = true;
    }
    if (facts.confirmed_flush_lsn > stored.last_flushed_lsn) {
        decision.messages.push_back(std::format(
            "slot '{}' is at {}, past the last stored position {}: the slot was recreated or used by someone "
            "else and changes are missing, so the snapshot starts over",
            facts.slot, core::format_lsn(facts.confirmed_flush_lsn), core::format_lsn(stored.last_flushed_lsn)));
        // The gap is covered by the snapshot; streaming continues from the slot.
        next.last_flushed_lsn = facts.confirmed_flush_lsn;
        reset = true;
    }

    if (reset) {
        next.snapshot = fresh_snapshot();
        decision.action = StartupAction::ResetSnapshot;
    } else {
        switch (stored.snapshot.status) {
            case state::SnapshotStatus::Pending:
                next.snapshot = fresh_snapshot();
                decision.action = StartupAction::NewSnapshot;
                break;
            case state::SnapshotStatus::Running:
                decision.action = StartupAction::Resume;  // keeps start_lsn, max_pk and last_pk
                break;
            case state::SnapshotStatus::Done:
                if (facts.snapshot_on_start == config::SnapshotOnStart::Always) {
                    next.snapshot = fresh_snapshot();
                    decision.action = StartupAction::NewSnapshot;
                } else {
                    decision.action = StartupAction::Resume;
                }
                break;
        }
    }
    decision.state = std::move(next);
    return decision;
}

}  // namespace recordflow::app
