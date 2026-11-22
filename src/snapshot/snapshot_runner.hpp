#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "core/change_event.hpp"
#include "core/table_schema.hpp"
#include "core/version.hpp"
#include "pg/snapshot_queries.hpp"
#include "pgoutput/messages.hpp"
#include "snapshot/chunk_window.hpp"
#include "snapshot/visibility.hpp"
#include "state/state.hpp"

// One snapshot pass of the tracked table through DBLog windows (decision 5,
// interface from decision 9.4). The engine calls it in stream order; the query
// connection is passed per call.
namespace recordflow::snapshot {

struct RunnerSettings {
    std::string slot;  // part of the watermark content
    std::size_t chunk_size = 1000;
    std::chrono::milliseconds chunk_delay{0};
    std::chrono::seconds window_timeout{60};
};

enum class PassStatus {
    Inactive,  // no pass (none started, or finished)
    Running,   // chunks remain
    Complete,  // every chunk up to max_pk is read; the sweep is next
};

// The Commit of a transaction in the stream.
struct CommitInfo {
    core::Lsn commit_lsn = 0;
    std::int64_t commit_ts = 0;
};

// The surviving rows of a chunk after its high watermark committed, and the
// progress to store in the flush that writes them (decision 5.2, step 6).
struct AcceptedChunk {
    std::vector<core::ChangeEvent> rows;  // Op::Read, version (H commit LSN, 0)
    state::SnapshotProgress progress;
};

// Arguments of Sink::sweep at the end of a pass (decision 5.5).
struct SweepBounds {
    core::Version below;      // (S, 0)
    core::Version tombstone;  // (last H commit LSN, 1)
    std::int64_t commit_ts = 0;  // of the last H
};

class SnapshotRunner {
public:
    using Clock = std::chrono::steady_clock;

    SnapshotRunner(core::TableSchema table, RunnerSettings settings);

    // Starts a pass: a pending snapshot starts from the beginning, a running
    // one resumes after last_pk with its start_lsn and max_pk (decision 5.1).
    void start(const state::SnapshotProgress& progress);

    PassStatus status() const;
    // The progress of the accepted chunks; status Done after finish().
    const state::SnapshotProgress& progress() const { return progress_; }
    bool window_open() const { return phase_ != Phase::Idle; }

    // A pass is running, no window is open and chunk_delay has passed.
    bool ready_for_chunk(Clock::time_point now) const;

    // Steps 2-4 of decision 5.2 while the stream is not read: low watermark,
    // the chunk SELECT, high watermark. `recent_xids` are the transactions
    // committed in the stream so far and `open_xid` the one being decoded;
    // if the SELECT did not see one of them the chunk is retried
    // (decision 5.4). Errors of the queries propagate after the window is
    // dropped.
    void open_window(pg::SnapshotQueries& queries, const std::deque<core::Xid>& recent_xids,
                     std::optional<core::Xid> open_xid, Clock::time_point now);

    // A logical message in the stream; only L and H of the current window
    // count. `commit_lsn` is the commit LSN of the message's transaction.
    void on_message(const pgoutput::LogicalMessage& message, core::Lsn commit_lsn);
    // A change of the tracked table.
    void on_change(const core::ChangeEvent& event);
    // A TRUNCATE of the tracked table.
    void on_truncate(const core::ChangeEvent& event);
    // After the Commit of the H transaction: the chunk's surviving rows.
    std::optional<AcceptedChunk> on_commit(const CommitInfo& commit, Clock::time_point now);

    // Drops an open window (lost connection, shutdown); the chunk is read
    // again later with new watermarks.
    void abort_window(std::string_view reason);
    // Drops the window if L or H has not arrived within window_timeout.
    void on_tick(Clock::time_point now);

    // xmin of the last chunk's snapshot: older xids are visible to every later
    // snapshot and may be forgotten (decision 5.4).
    std::optional<core::Xid> xid_horizon() const { return horizon_; }
    // Chunks retried in a row since the last accepted one.
    int retries() const { return retries_; }

    // Retries of one chunk in a row before the snapshot gives up (exit code 1).
    static constexpr int kMaxRetries = 10;

    SweepBounds sweep_bounds() const;
    // After the sweep: status Done, no pass.
    void finish();

private:
    enum class Phase { Idle, AwaitLow, InWindow };

    std::string new_window_id();
    // Drops the window and counts a retry; throws after kMaxRetries.
    void retry(const std::string& reason);
    // Rule 2 of decision 5.4 for an event before L; false if the window was dropped.
    bool check_before_low(const core::ChangeEvent& event);
    core::ChangeEvent read_event(core::Row row, core::Lsn high_lsn, std::int64_t commit_ts) const;

    core::TableSchema table_;
    RunnerSettings settings_;
    std::mt19937_64 random_;

    bool active_ = false;
    bool complete_ = false;
    state::SnapshotProgress progress_;

    Phase phase_ = Phase::Idle;
    std::string window_id_;
    Clock::time_point opened_at_{};
    Clock::time_point next_chunk_at_{};
    ChunkWindow chunk_;
    std::optional<std::vector<std::string>> read_max_;  // max_pk read by this window's SELECT
    bool first_chunk_ = false;
    core::Lsn low_lsn_ = 0;
    std::optional<core::Lsn> high_lsn_;  // H seen in the current transaction
    std::int64_t last_high_ts_ = 0;
    PgSnapshot snapshot_;  // of the current window's SELECT
    std::optional<core::Xid> horizon_;
    int retries_ = 0;
};

}  // namespace recordflow::snapshot
