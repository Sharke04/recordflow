#pragma once

#include <string>
#include <vector>

#include "core/table_schema.hpp"

// Schema-change detection (decision 7.1): the ClickHouse schema is derived from
// the PostgreSQL schema; any difference to the existing target is a change.
namespace recordflow::core {

struct TargetColumn {
    std::string name;
    std::string type;         // ClickHouse type name, e.g. "Nullable(Decimal(12, 2))"
    std::string source_type;  // PostgreSQL type, for messages; empty for an existing target

    bool operator==(const TargetColumn& other) const { return name == other.name && type == other.type; }
};

// The source-derived part of a target table (Recordflow's _rf_ columns excluded).
struct TargetSchema {
    std::vector<TargetColumn> columns;
    std::vector<std::string> sorting_key;
};

struct SchemaDiff {
    std::vector<std::string> lines;  // "+ new column ...", "- dropped column ...", "~ ..."

    bool empty() const { return lines.empty(); }
    std::string text() const;  // one line per difference, indented
};

TargetSchema derive_target(const TableSchema& table);

// Differences that would make writes wrong. Type changes that map to the same
// ClickHouse type (varchar(50) -> varchar(100)) are not differences.
SchemaDiff compare(const TargetSchema& existing, const TargetSchema& derived);

}  // namespace recordflow::core
