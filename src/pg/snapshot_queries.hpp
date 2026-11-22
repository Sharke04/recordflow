#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/change_event.hpp"
#include "core/table_schema.hpp"

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

// One chunk SELECT (decisions 5.2 and 5.3). Keys are primary-key values in
// index order, as text.
struct ChunkRequest {
    std::vector<std::string> after;               // exclusive lower bound; empty: from the start
    std::optional<std::vector<std::string>> upto;  // inclusive upper bound (max_pk)
    // The first chunk of a snapshot: read the largest key in the same
    // transaction and use it as `upto`.
    bool read_max = false;
    std::size_t limit = 1000;
};

struct ChunkResult {
    std::string snapshot;                            // pg_current_snapshot() of the transaction
    std::optional<std::vector<std::string>> max_pk;  // with read_max; nullopt for an empty table
    std::vector<core::Row> rows;                     // in primary-key order, all columns
};

class SnapshotQueries {
public:
    virtual ~SnapshotQueries() = default;

    // Writes a transactional logical message with kWatermarkPrefix in its own
    // autocommit transaction; it appears in the stream at its commit position.
    virtual void emit_watermark(const Watermark& mark) = 0;

    // Reads one chunk of `table` in a REPEATABLE READ READ ONLY transaction,
    // together with the transaction's snapshot. Without an upper bound and
    // without read_max (a snapshot of an empty table) no rows are read. Throws
    // SchemaChangeError if the result columns differ from `table`
    // (decision 7.2).
    virtual ChunkResult select_chunk(const core::TableSchema& table, const ChunkRequest& request) = 0;
};

}  // namespace recordflow::pg
