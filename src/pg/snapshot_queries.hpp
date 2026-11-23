#pragma once

#include <optional>
#include <string>
#include <string_view>

// The query-connection side of a DBLog chunk (decision 5.2), as an interface so
// that the snapshot runner can be tested without a server.
namespace recordflow::pg {

// Prefix of Recordflow's logical decoding messages; such messages never reach
// the sink.
inline constexpr std::string_view kWatermarkPrefix = "recordflow";

enum class WatermarkKind : char { Low = 'L', High = 'H' };

struct Watermark {
    WatermarkKind kind = WatermarkKind::Low;
    std::string slot;
    std::string id;  // UUID of the window
};

// "L:<slot>:<uuid>"
inline std::string format_watermark(const Watermark& mark) {
    return std::string(1, static_cast<char>(mark.kind)) + ":" + mark.slot + ":" + mark.id;
}

// nullopt for content that is not a watermark. The slot name cannot contain
// ':' (only lower-case letters, digits and underscores), the id is the rest.
inline std::optional<Watermark> parse_watermark(std::string_view content) {
    if (content.size() < 4 || content[1] != ':') return std::nullopt;
    if (content[0] != 'L' && content[0] != 'H') return std::nullopt;
    const auto colon = content.find(':', 2);
    if (colon == std::string_view::npos || colon == 2 || colon + 1 == content.size()) return std::nullopt;
    return Watermark{static_cast<WatermarkKind>(content[0]), std::string(content.substr(2, colon - 2)),
                     std::string(content.substr(colon + 1))};
}

class SnapshotQueries {
public:
    virtual ~SnapshotQueries() = default;

    // Writes a transactional logical message with kWatermarkPrefix in its own
    // autocommit transaction; it appears in the stream at its commit position.
    virtual void emit_watermark(const Watermark& mark) = 0;
};

}  // namespace recordflow::pg
