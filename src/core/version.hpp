#pragma once

#include <charconv>
#include <compare>
#include <cstdint>
#include <format>
#include <optional>
#include <string>
#include <string_view>

// Positions and versions (decision 2.2).
namespace recordflow::core {

// A position in the WAL.
using Lsn = std::uint64_t;

// A 32-bit transaction id as pgoutput reports it.
using Xid = std::uint32_t;

// "16/B374D848", the notation PostgreSQL uses.
inline std::string format_lsn(Lsn lsn) {
    return std::format("{:X}/{:X}", static_cast<std::uint32_t>(lsn >> 32),
                       static_cast<std::uint32_t>(lsn));
}

inline std::optional<Lsn> parse_lsn(std::string_view text) {
    const auto slash = text.find('/');
    if (slash == std::string_view::npos || slash == 0 || slash + 1 == text.size()) return std::nullopt;
    std::uint32_t high = 0;
    std::uint32_t low = 0;
    const auto parse = [](std::string_view part, std::uint32_t& out) {
        const auto [ptr, ec] = std::from_chars(part.data(), part.data() + part.size(), out, 16);
        return ec == std::errc() && ptr == part.data() + part.size();
    };
    if (!parse(text.substr(0, slash), high) || !parse(text.substr(slash + 1), low)) return std::nullopt;
    return (static_cast<Lsn>(high) << 32) | low;
}

// Row version: the commit LSN of the transaction in the high 64 bits and the
// change's position inside the transaction in the low 64 bits. Snapshot rows
// use the commit LSN of the high watermark and seq 0. The largest version of a
// key is its current state; replaying a transaction yields the same versions.
struct Version {
    Lsn commit_lsn = 0;
    std::uint64_t seq = 0;

    auto operator<=>(const Version&) const = default;

    std::string str() const { return std::format("{}:{}", format_lsn(commit_lsn), seq); }
};

}  // namespace recordflow::core
