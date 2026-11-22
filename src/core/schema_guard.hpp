#pragma once

#include <functional>
#include <string>
#include <vector>

#include "core/table_schema.hpp"
#include "pgoutput/messages.hpp"

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

// Fills type_name and is_enum of a column whose type the known schema does
// not describe (a Relation message carries only the type OID and modifier).
using TypeLookup = std::function<void(ColumnSchema& column)>;

// Whether a Relation message is about the tracked table: the same OID (also
// after a rename) or the same name (also after DROP and CREATE).
bool same_table(const TableSchema& known, const pgoutput::TableInfo& relation);

// Differences between the table the target was derived from and a Relation
// message about it (decisions 7.1 and 7.2): columns through the type mapping,
// the replica identity and its key columns, the table's name and OID.
// NOT NULL is not in the message; it is kept from `known` per column name.
SchemaDiff compare_relation(const TableSchema& known, const pgoutput::TableInfo& relation,
                            const TypeLookup& lookup);

}  // namespace recordflow::core
