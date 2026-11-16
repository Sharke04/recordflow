#pragma once

#include <optional>
#include <string>
#include <vector>

#include "config/config.hpp"
#include "core/version.hpp"
#include "state/state.hpp"

// What a start does with the stored state (decisions 4.3 and 5.1). Pure logic:
// the engine gathers the facts and acts on the result.
namespace recordflow::app {

struct StartupFacts {
    std::optional<state::State> stored;  // nullopt: no state file (first start)
    std::string slot;                    // from the configuration
    std::string source_table;            // "public.orders"
    std::string target_table;            // "recordflow.public__orders"
    std::string system_id;               // IDENTIFY_SYSTEM
    std::string target_uuid;             // after sink.prepare(); empty for a sink without one
    core::Lsn confirmed_flush_lsn = 0;   // of the slot
    config::SnapshotOnStart snapshot_on_start = config::SnapshotOnStart::Always;
};

enum class StartupAction {
    Resume,         // continue from the stored state (a running snapshot continues, a done one stays done)
    ResetSnapshot,  // the target lost data or the stream has a gap: snapshot back to pending
    NewSnapshot,    // first start, a pending snapshot, or done with snapshot_on_start = always
    Refuse,         // the state belongs to another configuration or cluster: exit code 4
};

struct StartupDecision {
    StartupAction action = StartupAction::Refuse;
    // The state to save before streaming; unset for Refuse.
    std::optional<state::State> state;
    // For Refuse the reason with instructions, otherwise warnings to log.
    std::vector<std::string> messages;
};

StartupDecision decide_startup(const StartupFacts& facts);

// The snapshot of a new or reset decision: pending, no progress.
state::SnapshotProgress fresh_snapshot();

}  // namespace recordflow::app
