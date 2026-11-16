#include "core/schema_guard.hpp"

#include <algorithm>
#include <format>

#include "sink/clickhouse/type_mapping.hpp"

namespace recordflow::core {

namespace {

std::string join(const std::vector<std::string>& names) {
    std::string out;
    for (const auto& name : names) out += (out.empty() ? "" : ", ") + name;
    return out;
}

const TargetColumn* find(const std::vector<TargetColumn>& columns, const std::string& name) {
    for (const auto& column : columns) {
        if (column.name == name) return &column;
    }
    return nullptr;
}

bool same_names(std::vector<std::string> a, std::vector<std::string> b) {
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    return a == b;
}

std::string described(const TargetColumn& column) {
    return column.source_type.empty() ? column.type : std::format("{} -> {}", column.source_type, column.type);
}

}  // namespace

std::string SchemaDiff::text() const {
    std::string out;
    for (const auto& line : lines) out += (out.empty() ? "    " : "\n    ") + line;
    return out;
}

TargetSchema derive_target(const TableSchema& table) {
    TargetSchema target;
    for (const auto& column : table.columns) {
        const bool in_key = std::find(table.primary_key.begin(), table.primary_key.end(), column.name) !=
                            table.primary_key.end();
        target.columns.push_back(
            {column.name, sink::clickhouse::map_type(column, in_key).name(), column.type_name});
    }
    target.sorting_key = table.primary_key;
    return target;
}

SchemaDiff compare(const TargetSchema& existing, const TargetSchema& derived) {
    SchemaDiff diff;
    for (const auto& column : derived.columns) {
        const TargetColumn* old = find(existing.columns, column.name);
        if (old == nullptr) {
            diff.lines.push_back(std::format("+ new column \"{}\" ({})", column.name, described(column)));
        } else if (old->type != column.type) {
            diff.lines.push_back(std::format("~ column \"{}\": {} now {}", column.name, described(*old),
                                             described(column)));
        }
    }
    for (const auto& column : existing.columns) {
        if (find(derived.columns, column.name) == nullptr) {
            diff.lines.push_back(std::format("- dropped column \"{}\" ({})", column.name, column.type));
        }
    }

    // Same columns, different order: ClickHouse blocks are positional per name,
    // but the order still has to match for the derived DDL and the view.
    if (diff.empty() && existing.columns != derived.columns) {
        std::vector<std::string> before, after;
        for (const auto& c : existing.columns) before.push_back(c.name);
        for (const auto& c : derived.columns) after.push_back(c.name);
        diff.lines.push_back(std::format("~ column order: ({}) now ({})", join(before), join(after)));
    }
    if (existing.sorting_key != derived.sorting_key) {
        diff.lines.push_back(std::format("~ primary key: ({}) now ({})", join(existing.sorting_key),
                                         join(derived.sorting_key)));
    }
    return diff;
}

bool same_table(const TableSchema& known, const pgoutput::TableInfo& relation) {
    return relation.oid == known.oid || (relation.schema == known.schema && relation.name == known.name);
}

SchemaDiff compare_relation(const TableSchema& known, const pgoutput::TableInfo& relation,
                            const TypeLookup& lookup) {
    SchemaDiff diff;
    if (relation.schema != known.schema || relation.name != known.name) {
        diff.lines.push_back(std::format("~ table: {} now {}.{}", known.qualified(), relation.schema, relation.name));
    }
    if (relation.oid != known.oid) {
        diff.lines.push_back(std::format("~ table recreated: oid {} now {}", known.oid, relation.oid));
    }

    TableSchema current = known;
    current.columns.clear();
    std::vector<std::string> key_columns;
    for (const auto& column : relation.columns) {
        ColumnSchema described{column.name, column.type_oid, column.type_mod, "", false, false};
        const auto old = std::find_if(known.columns.begin(), known.columns.end(),
                                      [&](const ColumnSchema& c) { return c.name == column.name; });
        if (old != known.columns.end()) described.not_null = old->not_null;
        if (old != known.columns.end() && old->type_oid == column.type_oid && old->type_mod == column.type_mod) {
            described.type_name = old->type_name;
            described.is_enum = old->is_enum;
        } else if (lookup) {
            lookup(described);
        }
        if (column.part_of_key) key_columns.push_back(column.name);
        current.columns.push_back(std::move(described));
    }
    // A primary-key column that is gone leaves the key to the column diff.
    const SchemaDiff columns = compare(derive_target(known), derive_target(current));
    diff.lines.insert(diff.lines.end(), columns.lines.begin(), columns.lines.end());

    if (relation.replica_identity != known.replica_identity) {
        diff.lines.push_back(std::format("~ replica identity: {} now {}", known.replica_identity,
                                         relation.replica_identity));
    } else if (known.replica_identity == 'd' && !same_names(key_columns, known.primary_key)) {
        // Under REPLICA IDENTITY DEFAULT the key flags are the primary key
        // (flags in column order, the key in index order).
        diff.lines.push_back(std::format("~ primary key: ({}) now ({})", join(known.primary_key),
                                         join(key_columns)));
    }
    return diff;
}

}  // namespace recordflow::core
