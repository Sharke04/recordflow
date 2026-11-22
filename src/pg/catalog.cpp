#include "pg/catalog.hpp"

#include <format>
#include <string>

#include "runtime/errors.hpp"
#include "runtime/log.hpp"

namespace recordflow::pg {

namespace {

const std::string& text(const QueryResult& result, std::size_t row, std::size_t column) {
    static const std::string kEmpty;
    const auto& value = result.rows[row][column];
    return value ? *value : kEmpty;
}

}  // namespace

core::TableSchema read_table_schema(QueryConnection& query, const std::string& qualified_table) {
    const auto dot = qualified_table.find('.');
    core::TableSchema table;
    table.schema = qualified_table.substr(0, dot);
    table.name = qualified_table.substr(dot + 1);

    const QueryResult relation = query.exec(
        "SELECT c.oid, c.relreplident "
        "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')",
        {table.schema, table.name});
    if (relation.empty()) {
        throw InterventionError(std::format("source table {} does not exist (or is not visible to this role)",
                                            qualified_table));
    }
    table.oid = static_cast<std::uint32_t>(std::stoul(text(relation, 0, 0)));
    table.replica_identity = text(relation, 0, 1).empty() ? 'd' : text(relation, 0, 1)[0];
    const std::string oid = std::to_string(table.oid);

    const QueryResult columns = query.exec(
        "SELECT a.attname, a.atttypid, a.atttypmod, format_type(a.atttypid, a.atttypmod), "
        "       a.attnotnull, t.typtype = 'e' "
        "FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid "
        "WHERE a.attrelid = $1::oid AND a.attnum > 0 AND NOT a.attisdropped "
        "ORDER BY a.attnum",
        {oid});
    for (std::size_t r = 0; r < columns.size(); ++r) {
        core::ColumnSchema column;
        column.name = text(columns, r, 0);
        column.type_oid = static_cast<std::uint32_t>(std::stoul(text(columns, r, 1)));
        column.type_mod = std::stoi(text(columns, r, 2));
        column.type_name = text(columns, r, 3);
        column.not_null = text(columns, r, 4) == "t";
        column.is_enum = text(columns, r, 5) == "t";
        table.columns.push_back(std::move(column));
    }

    const QueryResult key = query.exec(
        "SELECT a.attname "
        "FROM pg_index i "
        "CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
        "WHERE i.indrelid = $1::oid AND i.indisprimary "
        "ORDER BY k.ord",
        {oid});
    for (std::size_t r = 0; r < key.size(); ++r) table.primary_key.push_back(text(key, r, 0));

    if (table.primary_key.empty()) {
        throw InterventionError(std::format("source table {} has no primary key; Recordflow needs one", qualified_table));
    }
    if (table.replica_identity == 'n') {
        throw InterventionError(std::format(
            "source table {} has REPLICA IDENTITY NOTHING, so deletes carry no key; use DEFAULT", qualified_table));
    }
    return table;
}

void check_publication(QueryConnection& query, const std::string& publication,
                       const core::TableSchema& table) {
    const QueryResult exists =
        query.exec("SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", {publication});
    if (text(exists, 0, 0) != "t") {
        throw InterventionError(std::format("publication '{}' does not exist", publication));
    }

    const QueryResult tables = query.exec(
        "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1, 2",
        {publication});
    bool found = false;
    for (std::size_t r = 0; r < tables.size(); ++r) {
        if (text(tables, r, 0) == table.schema && text(tables, r, 1) == table.name) {
            found = true;
        } else {
            log::warn("publication '{}' also contains {}.{}; its changes are skipped (Recordflow v1 "
                      "transfers one table)",
                      publication, text(tables, r, 0), text(tables, r, 1));
        }
    }
    if (!found) {
        throw InterventionError(std::format("publication '{}' does not include {}; run: ALTER PUBLICATION {} "
                                            "ADD TABLE {}",
                                            publication, table.qualified(), publication, table.qualified()));
    }
}

void check_watermark_privilege(QueryConnection& query) {
    const QueryResult result = query.exec(
        "SELECT current_setting('server_version_num')::int, current_user, "
        "       coalesce(bool_or(has_function_privilege(p.oid, 'EXECUTE')), false) "
        "FROM pg_proc p "
        "WHERE p.proname = 'pg_logical_emit_message' "
        "  AND p.pronamespace = 'pg_catalog'::regnamespace "
        "  AND p.proargtypes[2] = 'text'::regtype");
    const int version = std::stoi(text(result, 0, 0));
    const std::string& user = text(result, 0, 1);
    if (version < 140000) {
        throw InterventionError(std::format(
            "PostgreSQL 14 or newer is required (logical messages in pgoutput), the server is {}", version));
    }
    if (text(result, 0, 2) != "t") {
        const char* signature = version >= 170000 ? "boolean, text, text, boolean" : "boolean, text, text";
        throw InterventionError(std::format(
            "role {} may not call pg_logical_emit_message, which snapshots need for watermarks; run: "
            "GRANT EXECUTE ON FUNCTION pg_logical_emit_message({}) TO {}",
            user, signature, user));
    }
}

}  // namespace recordflow::pg
