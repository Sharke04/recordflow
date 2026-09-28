#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace pgoutput {

struct Value {
    // UnchangedToast is for values in TOAST table
    enum class Kind { Null, UnchangedToast, Text };

    Kind kind = Kind::Null;
    std::string text;
};

using Row = std::vector<std::pair<std::string, Value>>;

enum class Op { Unknown, Insert, Update, Delete, Truncate };

struct ChangeEvent {
    Op op = Op::Unknown;
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
