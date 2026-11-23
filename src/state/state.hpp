#pragma once

#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "core/version.hpp"

// Contents of data/recordflow.state.json (decisions 4.2 and 5): what the
// stored data belongs to and how far it reaches.
namespace recordflow::state {

inline constexpr int kFormatVersion = 1;

enum class SnapshotStatus { Pending, Running, Done };

std::string_view to_string(SnapshotStatus status);

struct SourceIdentity {
    std::string system_id;  // IDENTIFY_SYSTEM of the PostgreSQL cluster
    std::string slot;
    std::string table;      // "public.orders"
};

struct TargetIdentity {
    std::string table;       // "recordflow.public__orders"
    std::string table_uuid;  // system.tables.uuid; empty for a sink without one
};

struct SnapshotProgress {
    SnapshotStatus status = SnapshotStatus::Pending;
    // Commit LSN of the snapshot's first low watermark (S), used by the sweep.
    std::optional<core::Lsn> start_lsn;
    // Largest primary key when the snapshot started; nullopt before the first
    // chunk and for an empty table (then start_lsn is set).
    std::optional<std::vector<std::string>> max_pk;
    // Primary key of the last completed chunk, empty before the first one.
    std::vector<std::string> last_pk;
    // Commit LSN of that chunk's high watermark.
    std::optional<core::Lsn> version_lsn;

    bool operator==(const SnapshotProgress&) const = default;
};

struct State {
    int format_version = kFormatVersion;
    SourceIdentity source;
    TargetIdentity target;
    // commit_end_lsn of the last transaction stored in the target.
    core::Lsn last_flushed_lsn = 0;
    SnapshotProgress snapshot;
};

class StateFormatError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Pretty-printed JSON in the layout of decision 4.2, ending with a newline.
std::string to_json(const State& state);

// Throws StateFormatError for invalid JSON, a missing or mistyped field, or an
// unknown format_version.
State from_json(std::string_view text);

}  // namespace recordflow::state
