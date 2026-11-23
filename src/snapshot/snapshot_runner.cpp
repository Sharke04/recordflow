#include "snapshot/snapshot_runner.hpp"

#include <format>
#include <stdexcept>
#include <utility>

#include "runtime/log.hpp"

namespace recordflow::snapshot {

namespace {

std::string describe_key(const std::vector<std::string>& key) {
    std::string out = "(";
    for (std::size_t i = 0; i < key.size(); ++i) out += (i ? ", " : "") + key[i];
    return out + ")";
}

}  // namespace

SnapshotRunner::SnapshotRunner(core::TableSchema table, RunnerSettings settings)
    : table_(std::move(table)), settings_(std::move(settings)), random_(std::random_device{}()) {
    if (settings_.chunk_size == 0) throw std::invalid_argument("chunk_size must be positive");
}

void SnapshotRunner::start(const state::SnapshotProgress& progress) {
    progress_ = progress;
    if (progress_.status == state::SnapshotStatus::Pending) {
        // A new pass; start_lsn and max_pk come with the first chunk (decision 5.3).
        progress_ = state::SnapshotProgress{};
    }
    progress_.status = state::SnapshotStatus::Running;
    active_ = true;
    complete_ = false;
    phase_ = Phase::Idle;
    chunk_.clear();
    next_chunk_at_ = {};
    if (progress_.start_lsn) {
        log::info("Snapshot of {} resumes after {} (up to {}, started at {}).", table_.qualified(),
                  progress_.last_pk.empty() ? "the beginning" : describe_key(progress_.last_pk),
                  progress_.max_pk ? describe_key(*progress_.max_pk) : "an empty table",
                  core::format_lsn(*progress_.start_lsn));
    } else {
        log::info("Snapshot of {} starts (chunks of {} rows).", table_.qualified(), settings_.chunk_size);
    }
}

PassStatus SnapshotRunner::status() const {
    if (!active_) return PassStatus::Inactive;
    return complete_ ? PassStatus::Complete : PassStatus::Running;
}

bool SnapshotRunner::ready_for_chunk(Clock::time_point now) const {
    return active_ && !complete_ && phase_ == Phase::Idle && now >= next_chunk_at_;
}

std::string SnapshotRunner::new_window_id() {
    // A random (version 4) UUID; it only has to differ between windows.
    const std::uint64_t high = (random_() & 0xFFFFFFFFFFFF0FFFull) | 0x0000000000004000ull;
    const std::uint64_t low = (random_() & 0x3FFFFFFFFFFFFFFFull) | 0x8000000000000000ull;
    return std::format("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", high >> 32, (high >> 16) & 0xFFFF, high & 0xFFFF,
                       low >> 48, low & 0xFFFFFFFFFFFFull);
}

void SnapshotRunner::open_window(pg::SnapshotQueries& queries, const std::deque<core::Xid>& /*recent_xids*/,
                                 std::optional<core::Xid> /*open_xid*/, Clock::time_point now) {
    if (!ready_for_chunk(now)) throw std::logic_error("open_window without a pending chunk");

    window_id_ = new_window_id();
    first_chunk_ = !progress_.start_lsn.has_value();
    high_lsn_.reset();
    read_max_.reset();
    chunk_.clear();
    opened_at_ = now;
    try {
        queries.emit_watermark({pg::WatermarkKind::Low, settings_.slot, window_id_});

        pg::ChunkRequest request;
        request.after = progress_.last_pk;
        request.upto = progress_.max_pk;
        request.read_max = first_chunk_;
        request.limit = settings_.chunk_size;
        pg::ChunkResult result = queries.select_chunk(table_, request);
        if (first_chunk_) read_max_ = result.max_pk;
        chunk_.load(table_.primary_key, std::move(result.rows));

        queries.emit_watermark({pg::WatermarkKind::High, settings_.slot, window_id_});
    } catch (...) {
        // Watermarks already written are ignored: no window has this id any more.
        window_id_.clear();
        chunk_.clear();
        throw;
    }
    phase_ = Phase::AwaitLow;
    log::debug("Snapshot window {} opened: {} rows after {}.", window_id_, chunk_.raw_size(),
               progress_.last_pk.empty() ? "the beginning" : describe_key(progress_.last_pk));
}

void SnapshotRunner::on_message(const pgoutput::LogicalMessage& message, core::Lsn commit_lsn) {
    if (message.prefix != pg::kWatermarkPrefix) return;
    const std::optional<pg::Watermark> mark = pg::parse_watermark(message.content);
    if (!mark || mark->slot != settings_.slot || phase_ == Phase::Idle || mark->id != window_id_) {
        // Old windows, other instances, or not a watermark at all.
        log::debug("Ignoring watermark '{}'.", message.content);
        return;
    }
    if (mark->kind == pg::WatermarkKind::Low && phase_ == Phase::AwaitLow) {
        phase_ = Phase::InWindow;
        low_lsn_ = commit_lsn;
    } else if (mark->kind == pg::WatermarkKind::High && phase_ == Phase::InWindow) {
        // The rows follow the Commit of this transaction (dblog.md 10.6, step 4).
        high_lsn_ = commit_lsn;
    }
}

void SnapshotRunner::on_change(const core::ChangeEvent& event) {
    if (phase_ == Phase::InWindow && !high_lsn_) chunk_.on_change(event);
}

void SnapshotRunner::on_truncate(const core::ChangeEvent& /*event*/) {
    if (phase_ == Phase::InWindow && !high_lsn_) chunk_.on_truncate();
}

core::ChangeEvent SnapshotRunner::read_event(core::Row row, core::Lsn high_lsn, std::int64_t commit_ts) const {
    core::ChangeEvent event;
    event.op = core::Op::Read;
    event.source = core::EventSource::Snapshot;
    event.schema_name = table_.schema;
    event.table = table_.name;
    event.key_columns = table_.primary_key;
    for (const auto& column : table_.primary_key) {
        for (const auto& [name, value] : row) {
            if (name == column) {
                event.key.push_back(value.text);
                break;
            }
        }
    }
    event.after = std::move(row);
    event.version = core::Version{high_lsn, 0};
    event.commit_ts = commit_ts;
    return event;
}

std::optional<AcceptedChunk> SnapshotRunner::on_commit(const CommitInfo& commit, Clock::time_point now) {
    if (phase_ != Phase::InWindow || !high_lsn_) return std::nullopt;
    const core::Lsn high_lsn = *high_lsn_;

    AcceptedChunk accepted;
    for (auto& row : chunk_.survivors()) accepted.rows.push_back(read_event(std::move(row), high_lsn, commit.commit_ts));

    if (first_chunk_) {
        progress_.start_lsn = low_lsn_;
        progress_.max_pk = read_max_;
    }
    if (chunk_.last_raw_key()) progress_.last_pk = *chunk_.last_raw_key();
    progress_.version_lsn = high_lsn;
    last_high_ts_ = commit.commit_ts;
    // A short chunk is the last one (decision 5.3).
    complete_ = chunk_.raw_size() < settings_.chunk_size;
    log::debug("Snapshot chunk accepted at {}: {} of {} rows{}.", core::format_lsn(high_lsn), accepted.rows.size(),
               chunk_.raw_size(), complete_ ? ", the last one" : "");

    accepted.progress = progress_;
    phase_ = Phase::Idle;
    window_id_.clear();
    high_lsn_.reset();
    chunk_.clear();
    next_chunk_at_ = now + settings_.chunk_delay;
    return accepted;
}

void SnapshotRunner::abort_window(std::string_view reason) {
    if (phase_ == Phase::Idle) return;
    log::info("Snapshot chunk dropped ({}); it is read again.", reason);
    phase_ = Phase::Idle;
    window_id_.clear();
    high_lsn_.reset();
    chunk_.clear();
}

SweepBounds SnapshotRunner::sweep_bounds() const {
    if (!progress_.start_lsn || !progress_.version_lsn) throw std::logic_error("sweep before any chunk");
    return SweepBounds{core::Version{*progress_.start_lsn, 0}, core::Version{*progress_.version_lsn, 1},
                       last_high_ts_};
}

void SnapshotRunner::finish() {
    progress_.status = state::SnapshotStatus::Done;
    active_ = false;
    complete_ = false;
    phase_ = Phase::Idle;
}

}  // namespace recordflow::snapshot
