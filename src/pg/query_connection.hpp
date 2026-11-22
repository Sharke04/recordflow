#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "pg/conninfo.hpp"
#include "pg/snapshot_queries.hpp"

struct pg_conn;

namespace recordflow::pg {

using Param = std::optional<std::string>;  // nullopt = SQL NULL

// Text-format result of one statement.
struct QueryResult {
    std::vector<std::string> column_names;
    std::vector<std::uint32_t> column_types;
    std::vector<std::int32_t> column_mods;
    std::vector<std::uint32_t> column_tables;  // PQftable: OID of the column's table, 0 for expressions
    std::vector<std::vector<std::optional<std::string>>> rows;

    std::size_t size() const { return rows.size(); }
    bool empty() const { return rows.empty(); }
};

// The regular (non-replication) connection used for catalog queries,
// watermarks and snapshot chunks. It connects on first use and reconnects on
// the next use after the connection was lost.
class QueryConnection : public SnapshotQueries {
public:
    // `params` from build_connection_params(..., ConnectionKind::Query).
    explicit QueryConnection(KeyValues params);
    ~QueryConnection() override;
    QueryConnection(const QueryConnection&) = delete;
    QueryConnection& operator=(const QueryConnection&) = delete;

    // Runs one statement with text parameters ($1, $2, ...). Errors are
    // classified by pg/errors (Resource::PostgresQuery).
    QueryResult exec(const std::string& sql, const std::vector<Param>& params = {});

    // Quotes an identifier for use in SQL text ("public" -> "\"public\"").
    std::string escape_identifier(const std::string& name);

    // SELECT pg_logical_emit_message(true, 'recordflow', 'L:<slot>:<uuid>')
    // in autocommit (decision 5.2).
    void emit_watermark(const Watermark& mark) override;

    // BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; pg_current_snapshot();
    // with read_max `ORDER BY pk DESC LIMIT 1`; then
    // `SELECT * ... WHERE (pk) > ($after) AND (pk) <= ($upto) ORDER BY pk LIMIT n`.
    ChunkResult select_chunk(const core::TableSchema& table, const ChunkRequest& request) override;

    // Drops the connection; the next call reconnects.
    void reset();

private:
    void ensure_connected();
    // ROLLBACK if a transaction is open on a live connection; errors ignored.
    void rollback_quietly() noexcept;
    // Result columns against the known schema; SchemaChangeError on a difference.
    void check_chunk_columns(const core::TableSchema& table, const QueryResult& rows);

    KeyValues params_;
    pg_conn* conn_ = nullptr;
};

}  // namespace recordflow::pg
