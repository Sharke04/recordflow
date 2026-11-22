#include "sink/tx_buffer.hpp"

#include <iterator>
#include <stdexcept>
#include <utility>

namespace recordflow::sink {

std::size_t TxBuffer::estimate(const core::ChangeEvent& event) {
    std::size_t bytes = 64;
    for (const auto* row : {&event.before, &event.after}) {
        if (!*row) continue;
        for (const auto& [name, value] : **row) bytes += value.text.size() + 8;
    }
    return bytes;
}

void TxBuffer::begin() {
    if (in_transaction_) throw std::logic_error("transaction already open");
    in_transaction_ = true;
}

void TxBuffer::append(core::ChangeEvent event) {
    if (!in_transaction_) throw std::logic_error("append outside a transaction");
    if (!oldest_) oldest_ = std::chrono::steady_clock::now();
    open_bytes_ += estimate(event);
    open_.push_back(std::move(event));
}

void TxBuffer::commit(core::Lsn commit_end) {
    if (!in_transaction_) throw std::logic_error("commit outside a transaction");
    committed_.insert(committed_.end(), std::make_move_iterator(open_.begin()),
                      std::make_move_iterator(open_.end()));
    committed_bytes_ += open_bytes_;
    open_.clear();
    open_bytes_ = 0;
    last_commit_end_ = commit_end;
    commits_.emplace_back(committed_.size(), commit_end);
    in_transaction_ = false;
}

void TxBuffer::abort() {
    open_.clear();
    open_bytes_ = 0;
    in_transaction_ = false;
    if (committed_.empty()) oldest_.reset();
}

BufferStats TxBuffer::stats() const {
    BufferStats stats{committed_.size() + open_.size(), committed_bytes_ + open_bytes_, committed_.size(),
                      oldest_};
    stats.commit_pending = last_commit_end_.has_value();
    return stats;
}

TxBuffer::Drained TxBuffer::drain(bool include_open) {
    Drained drained;
    drained.events = std::move(committed_);
    drained.last_commit_end = std::exchange(last_commit_end_, std::nullopt);
    drained.commits = std::exchange(commits_, {});
    committed_.clear();
    committed_bytes_ = 0;
    if (include_open) {
        drained.events.insert(drained.events.end(), std::make_move_iterator(open_.begin()),
                              std::make_move_iterator(open_.end()));
        open_.clear();
        open_bytes_ = 0;
    }
    oldest_ = open_.empty() ? std::nullopt : std::optional(std::chrono::steady_clock::now());
    return drained;
}

}  // namespace recordflow::sink
