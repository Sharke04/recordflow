#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "pg/conninfo.hpp"

struct pg_conn;

namespace recordflow::pg {

using Param = std::optional<std::string>;  // nullopt = SQL NULL

// Text-format result of one statement.
struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::uint32_t> column_types;
    std::vector<std::int32_t> column_mods;
    std::vector<std::vector<std::optional<std::string>>> rows;

    std::size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }
};

// The regular (non-replication) connection used for catalog queries,
// watermarks and snapshot chunks. It connects on first use and reconnects on
// the next use after the connection was lost.
class QueryConnection {
public:
    // `params` from build_connection_params(..., ConnectionKind::Query).
    explicit QueryConnection(KeyValues params);
    ~QueryConnection();
    QueryConnection(const QueryConnection&) = delete;
    QueryConnection& operator=(const QueryConnection&) = delete;

    // Runs one statement with text parameters ($1, $2, ...). Errors are
    // classified by pg/errors (Resource::PostgresQuery).
    QueryResult exec(const std::string& sql, const std::vector<Param>& params = {});

    // Quotes an identifier for use in SQL text ("public" -> "\"public\"").
    std::string escape_identifier(const std::string& name);

    // Drops the connection; the next call reconnects.
    void reset();

private:
    void ensure_connected();

    KeyValues params_;
    pg_conn* conn_ = nullptr;
};

}  // namespace recordflow::pg
