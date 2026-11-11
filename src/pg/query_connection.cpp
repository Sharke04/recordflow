#include "pg/query_connection.hpp"

#include <libpq-fe.h>

#include <utility>

#include "pg/errors.hpp"

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

std::string QueryConnection::escape_identifier(const std::string& name) {
    ensure_connected();
    char* escaped = PQescapeIdentifier(conn_, name.c_str(), name.size());
    if (escaped == nullptr) raise(Resource::PostgresQuery, "cannot escape identifier", conn_);
    std::string result = escaped;
    PQfreemem(escaped);
    return result;
}

}  // namespace recordflow::pg
