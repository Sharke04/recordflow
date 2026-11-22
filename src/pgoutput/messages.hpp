#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "core/change_event.hpp"
#include "core/version.hpp"

// Typed pgoutput messages (protocol version 1, text format).
namespace recordflow::pgoutput {

struct ColumnInfo {
    std::string name;
    std::uint32_t type_oid = 0;
    std::int32_t type_mod = -1;
    bool part_of_key = false;  // part of the replica identity, not necessarily the PK
};

struct TableInfo {
    std::uint32_t oid = 0;
    std::string schema;
    std::string name;
    char replica_identity = '\0';
    std::vector<ColumnInfo> columns;
};

struct Begin {
    core::Lsn final_lsn = 0;  // commit LSN of the transaction
    std::int64_t commit_ts = 0;
    core::Xid xid = 0;
};

struct Commit {
    std::uint8_t flags = 0;
    core::Lsn commit_lsn = 0;
    core::Lsn end_lsn = 0;  // position to confirm once the transaction is stored
    std::int64_t commit_ts = 0;
};

struct Relation {
    std::shared_ptr<const TableInfo> table;
};

// Insert, update or delete of one row.
struct Change {
    core::Op op = core::Op::Unknown;
    // Never null; a placeholder named "oid=<n>" if no Relation was seen.
    std::shared_ptr<const TableInfo> table;
    // Old key ('K') or full old row ('O'); for deletes always present.
    std::optional<core::Row> before;
    bool before_is_key_only = false;
    std::optional<core::Row> after;
};

struct Truncate {
    std::uint8_t options = 0;  // 1 = CASCADE, 2 = RESTART IDENTITY
    std::vector<std::shared_ptr<const TableInfo>> tables;
};

// pg_logical_emit_message (needs `messages 'true'`).
struct LogicalMessage {
    bool transactional = false;
    core::Lsn lsn = 0;
    std::string prefix;
    std::string content;
};

// Origin, Type and unknown messages, which Recordflow does not decode.
struct Other {
    char type = 0;
};

using Message = std::variant<Begin, Commit, Relation, Change, Truncate, LogicalMessage, Other>;

}  // namespace recordflow::pgoutput
