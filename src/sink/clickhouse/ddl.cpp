#include "sink/clickhouse/ddl.hpp"

#include <format>
#include <stdexcept>

#include "core/schema_guard.hpp"

namespace recordflow::sink::clickhouse {

std::string quote_identifier(std::string_view name) {
    std::string quoted = "`";
    for (char ch : name) {
        if (ch == '`' || ch == '\\') quoted += '\\';
        quoted += ch;
    }
    quoted += '`';
    return quoted;
}

std::string quote_string(std::string_view text) {
    std::string quoted = "'";
    for (char ch : text) {
        if (ch == '\'' || ch == '\\') quoted += '\\';
        quoted += ch;
    }
    quoted += '\'';
    return quoted;
}

std::string target_table_name(const config::ClickHouseSection& section, const core::TableSchema& source) {
    if (!section.table.value.empty()) return section.table.value;
    return source.schema + "__" + source.name;
}

std::string qualified_name(std::string_view database, std::string_view table) {
    return quote_identifier(database) + "." + quote_identifier(table);
}

std::string create_table_sql(std::string_view database, std::string_view table, const core::TableSchema& source) {
    if (source.primary_key.empty()) {
        throw std::invalid_argument(std::format("table {} has no primary key", source.qualified()));
    }
    const core::TargetSchema target = core::derive_target(source);

    std::string sql = std::format("CREATE TABLE IF NOT EXISTS {}\n(\n", qualified_name(database, table));
    for (const auto& column : target.columns) {
        sql += std::format("    {} {},\n", quote_identifier(column.name), column.type);
    }
    for (const auto& meta : kMetaColumns) {
        sql += std::format("    {} {}", quote_identifier(meta.name), meta.type);
        if (meta.name == kIngestedAtColumn) sql += " DEFAULT now64(3)";
        sql += meta.name == kMetaColumns.back().name ? "\n" : ",\n";
    }
    sql += std::format(")\nENGINE = ReplacingMergeTree({}, {})\nORDER BY (", quote_identifier(kVersionColumn),
                       quote_identifier(kIsDeletedColumn));
    for (std::size_t i = 0; i < target.sorting_key.size(); ++i) {
        sql += (i ? ", " : "") + quote_identifier(target.sorting_key[i]);
    }
    sql += ")";
    return sql;
}

std::string create_view_sql(std::string_view database, std::string_view table) {
    const std::string view = std::string(table) + std::string(kCurrentViewSuffix);
    return std::format(
        "CREATE VIEW IF NOT EXISTS {} AS\nSELECT * EXCEPT ({})\nFROM {} FINAL\nWHERE {} = 0",
        qualified_name(database, view), quote_identifier(kIsDeletedColumn), qualified_name(database, table),
        quote_identifier(kIsDeletedColumn));
}

}  // namespace recordflow::sink::clickhouse
