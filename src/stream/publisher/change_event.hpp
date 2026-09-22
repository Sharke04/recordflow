#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace pgoutput {

// One column value as it arrived on the wire. The three kinds are deliberately
// distinct: an unchanged TOAST value still exists in the row and was merely
// omitted from the wire, so collapsing it into Null would erase data
// downstream.
struct Value {
    enum class Kind { Null, UnchangedToast, Text };

    Kind kind = Kind::Null;
    std::string text;
};

using Row = std::vector<std::pair<std::string, Value>>;

enum class Op { Insert, Update, Delete, Truncate };

// A decoded row change, and at the same time the payload published to Kafka.
// The transaction fields are stamped from the enclosing Begin message, which
// already carries the xid, the commit timestamp and the LSN of the commit
// record, so a DML event is complete the moment it is decoded.
struct ChangeEvent {
    Op op = Op::Insert;
    std::string schema;
    std::string table;
    std::vector<std::string> key_columns;
    std::optional<Row> before;
    std::optional<Row> after;
    std::uint32_t xid = 0;
    std::uint64_t lsn = 0;
    std::int64_t commit_ts = 0;

    // Partition key. Table-qualified, because every table shares one topic, and
    // primary-key-valued, so that all versions of a row hash to the same
    // partition: that is what keeps their order intact and lets log compaction
    // reduce them to the latest one. Falls back to the bare `schema.table` when
    // no key columns are known (TRUNCATE, or a relation whose R message was
    // missed), which still pins one table to one partition rather than
    // scattering it round-robin.
    std::string key() const;

    std::string json() const;
};

}
