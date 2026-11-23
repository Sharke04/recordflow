#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <vector>

#include "core/change_event.hpp"
#include "core/version.hpp"

namespace recordflow::sink {

struct BufferStats {
    std::size_t rows = 0;            // all buffered events
    std::size_t bytes = 0;           // estimated
    std::size_t committed_rows = 0;  // events of complete transactions
    std::optional<std::chrono::steady_clock::time_point> oldest;  // first buffered event
    // A Commit whose events were all written already (an oversized
    // transaction): its position still has to be reported by a flush.
    bool commit_pending = false;
};

// Buffers events by transaction (decisions 4.5 and 8.2): complete transactions
// can be flushed, the open one can be dropped after a lost connection.
class TxBuffer {
public:
    void begin();
    void append(core::ChangeEvent event);
    void commit(core::Lsn commit_end);
    void abort();  // drops the open transaction's events

    bool in_transaction() const { return in_transaction_; }
    BufferStats stats() const;

    struct Drained {
        std::vector<core::ChangeEvent> events;
        // commit_end of the last complete transaction among `events`, if any
        std::optional<core::Lsn> last_commit_end;
    };

    // Removes the complete transactions, plus the open one's events so far if
    // `include_open` (an oversized transaction, decision 4.5 rule 2).
    Drained drain(bool include_open);

private:
    static std::size_t estimate(const core::ChangeEvent& event);

    std::vector<core::ChangeEvent> committed_;
    std::vector<core::ChangeEvent> open_;
    std::optional<core::Lsn> last_commit_end_;
    std::size_t committed_bytes_ = 0;
    std::size_t open_bytes_ = 0;
    bool in_transaction_ = false;
    std::optional<std::chrono::steady_clock::time_point> oldest_;
};

}  // namespace recordflow::sink
