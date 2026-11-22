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

}  // namespace recordflow::core
