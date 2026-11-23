#include "snapshot/chunk_window.hpp"

#include <stdexcept>
#include <utility>

namespace recordflow::snapshot {

std::string serialize_key(const std::vector<std::string>& values) {
    std::string key;
    for (const auto& value : values) {
        key += std::to_string(value.size());
        key += ':';
        key += value;
    }
    return key;
}

void ChunkWindow::clear() {
    key_columns_.clear();
    rows_.clear();
    index_.clear();
    alive_ = 0;
    last_raw_key_.reset();
}

void ChunkWindow::load(std::vector<std::string> key_columns, std::vector<core::Row> rows) {
    clear();
    key_columns_ = std::move(key_columns);
    rows_.reserve(rows.size());
    for (auto& row : rows) {
        std::optional<std::vector<std::string>> key = key_of(row);
        if (!key) throw std::invalid_argument("chunk row without its primary key");
        index_[serialize_key(*key)] = rows_.size();
        rows_.push_back(Entry{std::move(row), true});
        last_raw_key_ = std::move(key);
    }
    alive_ = rows_.size();
}

std::optional<std::vector<std::string>> ChunkWindow::key_of(const core::Row& row) const {
    std::vector<std::string> key;
    key.reserve(key_columns_.size());
    for (const auto& column : key_columns_) {
        const core::Value* found = nullptr;
        for (const auto& [name, value] : row) {
            if (name == column) {
                found = &value;
                break;
            }
        }
        if (found == nullptr || found->kind != core::Value::Kind::Text) return std::nullopt;
        key.push_back(found->text);
    }
    return key;
}

void ChunkWindow::remove(const std::optional<core::Row>& row) {
    if (!row || alive_ == 0) return;
    const std::optional<std::vector<std::string>> key = key_of(*row);
    if (!key) {
        on_truncate();
        return;
    }
    const auto it = index_.find(serialize_key(*key));
    if (it == index_.end()) return;
    Entry& entry = rows_[it->second];
    if (entry.alive) {
        entry.alive = false;
        --alive_;
    }
}

void ChunkWindow::on_change(const core::ChangeEvent& event) {
    switch (event.op) {
        case core::Op::Insert:
            remove(event.after);
            break;
        case core::Op::Update:
            remove(event.after);
            // Present when the key changed ('K') or with REPLICA IDENTITY FULL ('O').
            if (event.before) remove(event.before);
            break;
        case core::Op::Delete:
            remove(event.before);
            break;
        case core::Op::Truncate:
            on_truncate();
            break;
        default:
            break;
    }
}

void ChunkWindow::on_truncate() {
    for (auto& entry : rows_) entry.alive = false;
    alive_ = 0;
}

std::vector<core::Row> ChunkWindow::survivors() const {
    std::vector<core::Row> out;
    out.reserve(alive_);
    for (const auto& entry : rows_) {
        if (entry.alive) out.push_back(entry.row);
    }
    return out;
}

}  // namespace recordflow::snapshot
