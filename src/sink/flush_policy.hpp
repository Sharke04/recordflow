#pragma once

#include <chrono>
#include <cstddef>

#include "config/config.hpp"
#include "sink/tx_buffer.hpp"

// When the buffered events are written (decision 4.5).
namespace recordflow::sink {

struct FlushLimits {
    std::size_t max_rows = 50000;
    std::size_t max_bytes = 32u << 20;
    std::chrono::milliseconds max_delay{1000};

    static FlushLimits from(const config::BatchSection& batch);
};

enum class FlushDecision {
    None,       // keep buffering
    Complete,   // write the complete transactions (at a transaction boundary)
    Oversized,  // the open transaction passed the hard limit: write it so far too
};

class FlushPolicy {
public:
    // The hard limit for one huge transaction, as a multiple of the row and
    // byte limits (implementation plan, assumptions).
    static constexpr std::size_t kHardLimitFactor = 4;

    explicit FlushPolicy(FlushLimits limits) : limits_(limits) {}

    // The triggers (rows, bytes, age of the oldest event; the first one wins)
    // are checked only between transactions; one that fires inside a
    // transaction waits for its Commit. Inside a transaction only the hard
    // limit forces a write.
    FlushDecision decide(const BufferStats& stats, bool in_transaction,
                         std::chrono::steady_clock::time_point now) const;

    const FlushLimits& limits() const { return limits_; }

private:
    FlushLimits limits_;
};

}  // namespace recordflow::sink
