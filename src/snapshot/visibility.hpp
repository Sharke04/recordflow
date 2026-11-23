#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "core/version.hpp"

// Which committed transactions a chunk SELECT saw (decision 5.4). Pure logic
// over the text of pg_current_snapshot() from the chunk's transaction.
namespace recordflow::snapshot {

// A 64-bit transaction id (xid8): epoch in the high 32 bits.
using FullXid = std::uint64_t;

struct PgSnapshot {
    FullXid xmin = 0;
    FullXid xmax = 0;
    std::vector<FullXid> xip;  // in progress when the snapshot was taken

    // The 32-bit xid of xmin, for pruning the recent-xid set.
    core::Xid xmin32() const { return static_cast<core::Xid>(xmin); }
};

// "xmin:xmax:xip1,xip2,..." (the xip list may be empty); nullopt if malformed.
std::optional<PgSnapshot> parse_snapshot(std::string_view text);

// The 64-bit id of a 32-bit xid near `reference` (the snapshot's xmax): the
// candidate within 2^31 of it, so that xids just before an epoch boundary
// keep the previous epoch.
FullXid widen_xid(core::Xid xid, FullXid reference);

// True if the snapshot sees the effects of the committed transaction `xid`:
// it finished before the snapshot (xid < xmax) and was not in progress then.
bool is_visible(const PgSnapshot& snapshot, core::Xid xid);

}  // namespace recordflow::snapshot
