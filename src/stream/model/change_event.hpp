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

struct ChangeEvent {
    Op op = Op::Insert; //TODO: Why is it Insert by default?
    std::string schema;
    std::string table;
    std::vector<std::string> key_columns;
    std::optional<Row> before;
    std::optional<Row> after;
    std::uint32_t xid = 0;
    std::uint64_t lsn = 0;
    std::int64_t commit_ts = 0;
};

}
