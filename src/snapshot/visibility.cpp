#include "snapshot/visibility.hpp"

#include <algorithm>
#include <charconv>

namespace recordflow::snapshot {

namespace {

bool parse_xid(std::string_view text, FullXid& out) {
    if (text.empty()) return false;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), out);
    return ec == std::errc() && ptr == text.data() + text.size();
}

}  // namespace

std::optional<PgSnapshot> parse_snapshot(std::string_view text) {
    const auto first = text.find(':');
    if (first == std::string_view::npos) return std::nullopt;
    const auto second = text.find(':', first + 1);
    if (second == std::string_view::npos) return std::nullopt;

    PgSnapshot snapshot;
    if (!parse_xid(text.substr(0, first), snapshot.xmin)) return std::nullopt;
    if (!parse_xid(text.substr(first + 1, second - first - 1), snapshot.xmax)) return std::nullopt;
    if (snapshot.xmin > snapshot.xmax) return std::nullopt;

    std::string_view rest = text.substr(second + 1);
    while (!rest.empty()) {
        const auto comma = rest.find(',');
        FullXid xid = 0;
        if (!parse_xid(rest.substr(0, comma), xid)) return std::nullopt;
        snapshot.xip.push_back(xid);
        if (comma == std::string_view::npos) break;
        rest = rest.substr(comma + 1);
        if (rest.empty()) return std::nullopt;  // trailing comma
    }
    std::sort(snapshot.xip.begin(), snapshot.xip.end());
    return snapshot;
}

FullXid widen_xid(core::Xid xid, FullXid reference) {
    const auto distance = static_cast<std::int32_t>(xid - static_cast<core::Xid>(reference));
    if (distance < 0 && static_cast<FullXid>(-static_cast<std::int64_t>(distance)) > reference) {
        return xid;  // would fall before the first epoch
    }
    return reference + static_cast<FullXid>(static_cast<std::int64_t>(distance));
}

bool is_visible(const PgSnapshot& snapshot, core::Xid xid) {
    const FullXid full = widen_xid(xid, snapshot.xmax);
    if (full < snapshot.xmin) return true;
    if (full >= snapshot.xmax) return false;
    return !std::binary_search(snapshot.xip.begin(), snapshot.xip.end(), full);
}

}  // namespace recordflow::snapshot
