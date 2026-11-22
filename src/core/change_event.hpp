#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/version.hpp"

// The internal event model shared by the change stream and snapshots
// (decision 2.1). Sinks see only this type.
namespace recordflow::core {

struct Value {
    // UnchangedToast is for values in TOAST table
    enum class Kind { Null, UnchangedToast, Text };

    Kind kind = Kind::Null;
    std::string text;
};

using Row = std::vector<std::pair<std::string, Value>>;

// Read: a row copied by a snapshot chunk (applied as an upsert).
enum class Op { Unknown, Insert, Update, Delete, Truncate, Read };

enum class EventSource { Log, Snapshot };

struct ChangeEvent {
    Op op = Op::Unknown;
    EventSource source = EventSource::Log;
    std::string schema_name;
    std::string table;
    std::vector<std::string> key_columns;
    // Values of key_columns, in the same order (from `after`, for deletes from `before`).
    std::vector<std::string> key;
    std::optional<Row> before;
    std::optional<Row> after;
    Version version;
    Xid xid = 0;
    std::int64_t commit_ts = 0;  // microseconds since 2000-01-01 00:00:00 UTC (PostgreSQL epoch)
};

// The PostgreSQL epoch in microseconds since the Unix epoch.
inline constexpr std::int64_t kPostgresEpochUnixMicros = 946'684'800'000'000;

}  // namespace recordflow::core
