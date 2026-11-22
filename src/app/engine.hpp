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
#include "runtime/errors.hpp"
#include "pgoutput/decoder.hpp"
#include "pgoutput/messages.hpp"
#include "runtime/backoff.hpp"
#include "sink/flush_policy.hpp"
#include "snapshot/snapshot_runner.hpp"
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
    // Connects (with backoff), checks the system_id against the state file and
    // starts replication from last_flushed_lsn (decision 8.2).
    void start_replication();
    void connect_and_start(const std::string& slot, const std::string& publication);
    // A lost replication connection (decision 8.2): drop the open transaction,
    // reset the decoder, write the complete transactions, reconnect.
    void recover(const TransientError& error);

    // Runs `step` until it does not throw a TransientError, with backoff
    // (decision 8.3); `on_failure` runs after each failed attempt.
    void retry_transient(std::string_view what, const std::function<void()>& step,
                         const std::function<void()>& on_failure = {});

    // The main loop (decisions 8.5 and 9.5) until a stop signal.
    int stream();
    // The graceful shutdown of decision 8.4; returns exit code 0.
    int shutdown();
    void handle_frame(const pg::CopyData& copy);
    void dispatch(const pgoutput::Message& message);
    void check_relation(const pgoutput::TableInfo& relation);

    // Decisions 7.3 and 7.4: drops the open transaction, stores and confirms
    // the complete ones and throws SchemaChangeError with the report.
    [[noreturn]] void stop_on_schema_change(std::string_view where, std::optional<core::Lsn> detected,
                                            const std::string& diff);
    std::string schema_change_report(const std::string& target, std::string_view where,
                                     std::optional<core::Lsn> stored, std::optional<core::Lsn> detected,
                                     const std::string& diff) const;
    bool tracked(const core::ChangeEvent& event) const;
    // tracked(), and passes the event to an open snapshot window.
    bool tracked_change(const core::ChangeEvent& event);

    // Starts or resumes the snapshot pass that the startup decision left
    // pending or running (decision 5.1).
    void start_snapshot();
    // One snapshot step of the main loop: window timeout, the next chunk,
    // the end of the pass (decisions 5.2 and 5.5).
    void snapshot_step();
    void open_chunk(std::chrono::steady_clock::time_point now);
    void finish_snapshot();
    // Commit of a stream transaction: the H transaction releases the chunk
    // rows as a pseudo-transaction.
    void on_commit(const pgoutput::Commit& commit);

    // Flushes when the batch policy says so (decision 4.5).
    void maybe_flush();
    // flush -> state file -> slot confirmation (decision 4.2).
    void flush_and_checkpoint(bool include_open);
    // Records that everything up to `lsn` is stored. Written to the state
    // file and confirmed at once if `now`, otherwise within the interval.
    void advance(core::Lsn lsn, bool now);
    void checkpoint_if_due(bool force = false);
    // Writes a pending checkpoint and sends the position to the server now.
    void confirm_now();

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

    std::optional<snapshot::SnapshotRunner> snapshot_;
    // Progress of the last accepted chunk; it enters the state file with the
    // checkpoint that covers its rows (decision 5.2, step 6).
    struct PendingProgress {
        state::SnapshotProgress progress;
        core::Lsn commit_end = 0;
    };
    std::optional<PendingProgress> pending_progress_;
    runtime::Backoff chunk_backoff_;  // a failing query connection between chunks (decision 8.2)
    std::chrono::steady_clock::time_point next_chunk_after_{};

    core::Lsn transaction_lsn_ = 0;  // commit LSN of the current (or last) transaction
    core::Lsn stored_lsn_ = 0;  // highest position known to be stored, maybe not yet in the state file
    std::chrono::steady_clock::time_point last_checkpoint_{};
};

}  // namespace recordflow::app
