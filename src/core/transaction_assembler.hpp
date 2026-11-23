#pragma once

#include <cstddef>
#include <deque>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/change_event.hpp"
#include "core/version.hpp"
#include "pgoutput/messages.hpp"

namespace recordflow::core {

// Turns decoded pgoutput messages into ChangeEvents (decisions 2.1 and 2.2):
// tracks transaction boundaries, numbers the changes of a transaction (`seq`),
// assigns versions (commit LSN, seq) and key values, and remembers recently
// committed transaction ids for the snapshot visibility check (decision 5.4).
class TransactionAssembler {
public:
    static constexpr std::size_t kMaxRecentXids = 100000;

    // Key columns of a table ("schema.table"), in primary-key order. Tables
    // without an entry use the replica identity columns of their Relation.
    void set_primary_key(const std::string& qualified_table, std::vector<std::string> columns);

    void begin(const pgoutput::Begin& begin);
    ChangeEvent change(const pgoutput::Change& change);
    std::vector<ChangeEvent> truncate(const pgoutput::Truncate& truncate);
    void commit(const pgoutput::Commit& commit);

    bool in_transaction() const { return in_transaction_; }

    // Transaction ids committed recently, oldest first.
    const std::deque<Xid>& recent_xids() const { return recent_xids_; }

    // Drops remembered xids that precede `xmin` (in wraparound order).
    void forget_xids_before(Xid xmin);
    void forget_all_xids() { recent_xids_.clear(); }

    // The xid of the transaction being decoded, if any.
    std::optional<Xid> open_xid() const {
        return in_transaction_ ? std::optional<Xid>(current_.xid) : std::nullopt;
    }

    // Forgets the open transaction (after a lost connection).
    void reset();

private:
    ChangeEvent event_for(Op op, const pgoutput::TableInfo& table);

    std::unordered_map<std::string, std::vector<std::string>> primary_keys_;
    pgoutput::Begin current_;
    bool in_transaction_ = false;
    std::uint64_t next_seq_ = 0;
    std::deque<Xid> recent_xids_;
};

// True if `a` is older than `b`, treating xids as a circular 32-bit space.
inline bool xid_precedes(Xid a, Xid b) {
    return static_cast<std::int32_t>(a - b) < 0;
}

}  // namespace recordflow::core
