#pragma once

#include <string>

#include "core/table_schema.hpp"
#include "pg/query_connection.hpp"

namespace recordflow::pg {

// Reads columns, primary key and replica identity of "schema.table".
// A missing table, a table without a primary key or one with
// REPLICA IDENTITY NOTHING is an InterventionError.
core::TableSchema read_table_schema(QueryConnection& query, const std::string& qualified_table);

}  // namespace recordflow::pg
