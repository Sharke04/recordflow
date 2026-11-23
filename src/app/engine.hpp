#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "config/config.hpp"
#include "core/table_schema.hpp"
#include "core/transaction_assembler.hpp"
#include "core/version.hpp"
#include "pgoutput/decoder.hpp"
#include "pgoutput/messages.hpp"
#include "sink/flush_policy.hpp"
#include "state/directory_lock.hpp"
#include "state/state.hpp"
#include "state/state_store.hpp"

namespace recordflow::pg {
class CopyData;
class QueryConnection;
class ReplicationConnection;
}  // namespace recordflow::pg

namespace recordflow::sink {
class Sink;
}  // namespace recordflow::sink

namespace recordflow::app {

// Owns all components and runs the main loop (decision 9.5): startup checks,
// the stream into the sink, checkpoints and recovery.
class Engine {
public:
    explicit Engine(config::Config config);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Returns the process exit code; errors propagate as exceptions from
    // runtime/errors.hpp.
    int run();

    // Position-only checkpoints (keepalives, transactions without rows for
    // the table) are written at most this often.
    static constexpr std::chrono::seconds kPositionCheckpointInterval{1};

private:
    // Decisions 5.1 and 8.2: lock and state, connections (with backoff while
    // the databases are unreachable), IDENTIFY_SYSTEM, catalog, publication and
    // privilege, sink.prepare, the slot's position, the startup decision and
    // START_REPLICATION from last_flushed_lsn.
    void startup();

    // Runs `step` until it does not throw a TransientError, with backoff
    // (decision 8.3); `on_failure` runs after each failed attempt.
    void retry_transient(std::string_view what, const std::function<void()>& step,
                         const std::function<void()>& on_failure = {});

    // The main loop (decisions 8.5 and 9.5).
    int stream();
    void handle_frame(const pg::CopyData& copy);
    void dispatch(const pgoutput::Message& message);
    void check_relation(const pgoutput::TableInfo& relation);
    bool tracked(const core::ChangeEvent& event) const;

    // Flushes when the batch policy says so (decision 4.5).
    void maybe_flush();
    // flush -> state file -> slot confirmation (decision 4.2).
    void flush_and_checkpoint(bool include_open);
    // Records that everything up to `lsn` is stored. Written to the state
    // file and confirmed at once if `now`, otherwise within the interval.
    void advance(core::Lsn lsn, bool now);
    void checkpoint_if_due(bool force = false);

    config::Config config_;
    state::StateStore store_;
    std::optional<state::DirectoryLock> lock_;
    state::State state_;
    core::TableSchema table_;
    std::unique_ptr<pg::QueryConnection> query_;
    std::unique_ptr<pg::ReplicationConnection> repl_;
    std::unique_ptr<sink::Sink> sink_;
    pgoutput::PgoutputDecoder decoder_;
    core::TransactionAssembler assembler_;
    sink::FlushPolicy policy_;

    core::Lsn stored_lsn_ = 0;  // highest position known to be stored, maybe not yet in the state file
    std::chrono::steady_clock::time_point last_checkpoint_{};
};

}  // namespace recordflow::app
