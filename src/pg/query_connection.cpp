#include "pg/query_connection.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <format>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "core/schema_guard.hpp"
#include "pg/errors.hpp"
#include "runtime/errors.hpp"

namespace recordflow::pg {

QueryConnection::QueryConnection(KeyValues params) : params_(std::move(params)) {}

QueryConnection::~QueryConnection() {
    reset();
}

void QueryConnection::reset() {
    if (conn_ != nullptr) {
        PQfinish(conn_);
        conn_ = nullptr;
    }
}

void QueryConnection::ensure_connected() {
    if (conn_ != nullptr && PQstatus(conn_) == CONNECTION_OK) return;
    reset();

    std::vector<const char*> keywords, values;
    for (const auto& [keyword, value] : params_) {
        keywords.push_back(keyword.c_str());
        values.push_back(value.c_str());
    }
    keywords.push_back(nullptr);
    values.push_back(nullptr);

    conn_ = PQconnectdbParams(keywords.data(), values.data(), 0);
    if (PQstatus(conn_) != CONNECTION_OK) {
        try {
            raise(Resource::PostgresQuery, "query connection failed", conn_);
        } catch (...) {
            reset();
            throw;
        }
    }
}

QueryResult QueryConnection::exec(const std::string& sql, const std::vector<Param>& params) {
    ensure_connected();

    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& param : params) values.push_back(param ? param->c_str() : nullptr);

    PGresult* res = PQexecParams(conn_, sql.c_str(), static_cast<int>(values.size()), nullptr,
                                 values.data(), nullptr, nullptr, /*resultFormat=*/0);
    const auto status = res != nullptr ? PQresultStatus(res) : PGRES_FATAL_ERROR;
    if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
        try {
            raise(Resource::PostgresQuery, "query failed", conn_, res);
        } catch (...) {
            PQclear(res);
            if (PQstatus(conn_) != CONNECTION_OK) reset();
            throw;
        }
    }

    QueryResult result;
    const int ncols = PQnfields(res);
    for (int c = 0; c < ncols; ++c) {
        result.column_names.emplace_back(PQfname(res, c));
        result.column_types.push_back(PQftype(res, c));
        result.column_mods.push_back(PQfmod(res, c));
        result.column_tables.push_back(PQftable(res, c));
    }
    const int nrows = PQntuples(res);
    result.rows.reserve(static_cast<std::size_t>(nrows));
    for (int r = 0; r < nrows; ++r) {
        std::vector<std::optional<std::string>> row;
        row.reserve(static_cast<std::size_t>(ncols));
        for (int c = 0; c < ncols; ++c) {
            if (PQgetisnull(res, r, c)) {
                row.emplace_back(std::nullopt);
            } else {
                row.emplace_back(std::string(PQgetvalue(res, r, c),
                                             static_cast<std::size_t>(PQgetlength(res, r, c))));
            }
        }
        result.rows.push_back(std::move(row));
    }
    PQclear(res);
    return result;
}

void QueryConnection::emit_watermark(const Watermark& mark) {
    exec("SELECT pg_logical_emit_message(true, $1, $2)", {std::string(kWatermarkPrefix), format_watermark(mark)});
}

void QueryConnection::rollback_quietly() noexcept {
    if (conn_ == nullptr || PQstatus(conn_) != CONNECTION_OK) return;
    if (PQtransactionStatus(conn_) == PQTRANS_IDLE) return;
    PQclear(PQexec(conn_, "ROLLBACK"));
}

namespace {

// "\"a\", \"b\"" (with `suffix` after each, e.g. " DESC")
std::string joined(const std::vector<std::string>& quoted, std::string_view suffix = "") {
    std::string list;
    for (std::size_t i = 0; i < quoted.size(); ++i) {
        list += (i ? ", " : "") + quoted[i] + std::string(suffix);
    }
    return list;
}

// "(\"a\", \"b\")", a row value for the key comparisons
std::string key_list(const std::vector<std::string>& quoted) {
    return "(" + joined(quoted) + ")";
}

// "($1::bigint, $2::text)" from parameter `first` on.
std::string param_list(const std::vector<std::string>& types, std::size_t first) {
    std::string list = "(";
    for (std::size_t i = 0; i < types.size(); ++i) {
        list += std::format("{}${}::{}", i ? ", " : "", first + i, types[i]);
    }
    return list + ")";
}

std::vector<std::string> key_values(const QueryResult& result, std::size_t row) {
    std::vector<std::string> key;
    for (const auto& value : result.rows[row]) key.push_back(value.value_or(""));
    return key;
}

}  // namespace

ChunkResult QueryConnection::select_chunk(const core::TableSchema& table, const ChunkRequest& request) {
    std::vector<std::string> keys;  // quoted primary-key columns
    std::vector<std::string> types;  // their types, for the parameter casts
    for (const auto& name : table.primary_key) {
        keys.push_back(escape_identifier(name));
        const auto column = std::find_if(table.columns.begin(), table.columns.end(),
                                         [&](const core::ColumnSchema& c) { return c.name == name; });
        if (column == table.columns.end() || column->type_name.empty()) {
            throw std::logic_error("primary-key column " + name + " missing from the table schema");
        }
        types.push_back(column->type_name);
    }
    const std::string relation = escape_identifier(table.schema) + "." + escape_identifier(table.name);

    ChunkResult chunk;
    try {
        exec("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
        // The first statement takes the transaction's snapshot (decision 5.4).
        chunk.snapshot = exec("SELECT pg_current_snapshot()::text").rows.at(0).at(0).value_or("");

        std::optional<std::vector<std::string>> upto = request.upto;
        if (request.read_max) {
            // ORDER BY ... DESC LIMIT 1 instead of max() over a row value.
            const QueryResult max = exec(
                std::format("SELECT {} FROM {} ORDER BY {} LIMIT 1", joined(keys), relation, joined(keys, " DESC")));
            if (!max.empty()) chunk.max_pk = key_values(max, 0);
            upto = chunk.max_pk;
        }

        if (upto) {
            std::vector<Param> params;
            std::string where;
            if (!request.after.empty()) {
                where = std::format("{} > {}", key_list(keys), param_list(types, 1));
                params.insert(params.end(), request.after.begin(), request.after.end());
            }
            where += std::format("{}{} <= {}", where.empty() ? "" : " AND ", key_list(keys),
                                 param_list(types, params.size() + 1));
            params.insert(params.end(), upto->begin(), upto->end());
            const QueryResult rows = exec(std::format("SELECT * FROM {} WHERE {} ORDER BY {} LIMIT {}", relation,
                                                      where, joined(keys), request.limit),
                                          params);
            check_chunk_columns(table, rows);
            chunk.rows.reserve(rows.size());
            for (const auto& values : rows.rows) {
                core::Row row;
                row.reserve(values.size());
                for (std::size_t c = 0; c < values.size(); ++c) {
                    row.emplace_back(rows.column_names[c],
                                     values[c] ? core::Value{core::Value::Kind::Text, *values[c]} : core::Value{});
                }
                chunk.rows.push_back(std::move(row));
            }
        }
        exec("COMMIT");
    } catch (...) {
        rollback_quietly();
        throw;
    }
    return chunk;
}

void QueryConnection::check_chunk_columns(const core::TableSchema& table, const QueryResult& rows) {
    // The result described like a Relation message, so that the stream's
    // rules apply (decision 7.2): only differences that change the target count.
    pgoutput::TableInfo described;
    described.schema = table.schema;
    described.name = table.name;
    described.oid = rows.column_tables.empty() || rows.column_tables[0] == 0 ? table.oid : rows.column_tables[0];
    described.replica_identity = table.replica_identity;
    for (std::size_t c = 0; c < rows.column_names.size(); ++c) {
        const bool in_key = std::find(table.primary_key.begin(), table.primary_key.end(), rows.column_names[c]) !=
                            table.primary_key.end();
        described.columns.push_back({rows.column_names[c], rows.column_types[c], rows.column_mods[c], in_key});
    }
    const core::SchemaDiff diff = core::compare_relation(table, described, [&](core::ColumnSchema& column) {
        const QueryResult type =
            exec("SELECT format_type($1::oid, $2::int), typtype = 'e' FROM pg_type WHERE oid = $1::oid",
                 {std::to_string(column.type_oid), std::to_string(column.type_mod)});
        if (type.empty()) return;
        column.type_name = type.rows[0][0].value_or("");
        column.is_enum = type.rows[0][1].value_or("") == "t";
    });
    if (!diff.empty()) throw SchemaChangeError("the chunk SELECT returned other columns", diff.text());
}

std::string QueryConnection::escape_identifier(const std::string& name) {
    ensure_connected();
    char* escaped = PQescapeIdentifier(conn_, name.c_str(), name.size());
    if (escaped == nullptr) raise(Resource::PostgresQuery, "cannot escape identifier", conn_);
    std::string result = escaped;
    PQfreemem(escaped);
    return result;
}

}  // namespace recordflow::pg
