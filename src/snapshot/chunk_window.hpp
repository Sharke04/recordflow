#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/change_event.hpp"

// The rows of one DBLog chunk between its low and high watermark (decision 5.2,
// dblog.md 10.6). Pure logic: the snapshot runner loads the SELECT result and
// passes the tracked table's change events; what survives is emitted after
// the high watermark.
namespace recordflow::snapshot {

// Length-prefixed key ("3:abc1:x"), so that ("a|b") and ("a", "b") differ.
std::string serialize_key(const std::vector<std::string>& values);

class ChunkWindow {
public:
    // `rows` in primary-key order as the chunk SELECT returned them; every
    // row holds all `key_columns`.
    void load(std::vector<std::string> key_columns, std::vector<core::Row> rows);

    // A change of the tracked table inside the window: removes the keys it
    // touches (`after` for inserts and updates, `before` for deletes and for
    // updates that carry the old key). A key that cannot be read (missing or
    // unchanged-TOAST column) clears the whole chunk, which is always safe.
    void on_change(const core::ChangeEvent& event);
    // A TRUNCATE of the tracked table inside the window: nothing survives.
    void on_truncate();

    // The rows still present, in primary-key order.
    std::vector<core::Row> survivors() const;
    std::size_t surviving() const { return alive_; }

    // Rows the SELECT returned, before any removal.
    std::size_t raw_size() const { return rows_.size(); }
    // Primary key of the SELECT's last row (the next chunk starts after it),
    // nullopt for an empty result (decision 5.2, step 6).
    const std::optional<std::vector<std::string>>& last_raw_key() const { return last_raw_key_; }

    void clear();

private:
    struct Entry {
        core::Row row;
        bool alive = true;
    };

    // nullopt if a key column is missing or not plain text.
    std::optional<std::vector<std::string>> key_of(const core::Row& row) const;
    void remove(const std::optional<core::Row>& row);

    std::vector<std::string> key_columns_;
    std::vector<Entry> rows_;
    std::unordered_map<std::string, std::size_t> index_;
    std::size_t alive_ = 0;
    std::optional<std::vector<std::string>> last_raw_key_;
};

}  // namespace recordflow::snapshot
