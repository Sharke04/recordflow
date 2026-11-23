#pragma once

#include <optional>
#include <string>

#include "core/table_schema.hpp"
#include "core/version.hpp"
#include "pg/query_connection.hpp"

namespace recordflow::pg {

// Reads columns, primary key and replica identity of "schema.table".
// A missing table, a table without a primary key or one with
// REPLICA IDENTITY NOTHING is an InterventionError.
core::TableSchema read_table_schema(QueryConnection& query, const std::string& qualified_table);

// The publication must exist and include the table (InterventionError
// otherwise); other tables in it are reported as warnings (decision 4.4).
void check_publication(QueryConnection& query, const std::string& publication,
                       const core::TableSchema& table);

// PostgreSQL 14+ and EXECUTE on pg_logical_emit_message (looked up by name,
// since PostgreSQL 17 added a fourth parameter). InterventionError otherwise,
// with the GRANT statement that fixes it.
void check_watermark_privilege(QueryConnection& query);

struct SlotInfo {
    core::Lsn confirmed_flush_lsn = 0;
    std::optional<int> active_pid;  // the walsender using the slot, if any
};

// The slot must exist, be a pgoutput slot of this database and not be
// invalidated (InterventionError otherwise, decisions 4.1 and 8.6).
// Recordflow never creates slots.
SlotInfo read_slot(QueryConnection& query, const std::string& slot);

}  // namespace recordflow::pg
