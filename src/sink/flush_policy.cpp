#include "sink/flush_policy.hpp"

namespace recordflow::sink {

FlushLimits FlushLimits::from(const config::BatchSection& batch) {
    FlushLimits limits;
    limits.max_rows = static_cast<std::size_t>(batch.max_rows.value);
    limits.max_bytes = static_cast<std::size_t>(batch.max_mb.value) << 20;
    limits.max_delay = std::chrono::milliseconds(batch.max_delay_ms.value);
    return limits;
}

FlushDecision FlushPolicy::decide(const BufferStats& stats, bool in_transaction,
                                  std::chrono::steady_clock::time_point now) const {
    if (stats.rows == 0) {
        // Nothing to write, but an oversized transaction may have committed
        // after its rows were written (decision 4.5 rule 2).
        return !in_transaction && stats.commit_pending ? FlushDecision::Complete : FlushDecision::None;
    }

    if (in_transaction) {
        const bool over_hard_limit = stats.rows >= kHardLimitFactor * limits_.max_rows ||
                                     stats.bytes >= kHardLimitFactor * limits_.max_bytes;
        return over_hard_limit ? FlushDecision::Oversized : FlushDecision::None;
    }

    if (stats.rows >= limits_.max_rows || stats.bytes >= limits_.max_bytes) return FlushDecision::Complete;
    if (stats.oldest && now - *stats.oldest >= limits_.max_delay) return FlushDecision::Complete;
    return FlushDecision::None;
}

}  // namespace recordflow::sink
