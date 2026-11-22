#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace recordflow::core {

struct ColumnSchema {
    std::string name;
    std::uint32_t type_oid = 0;
    std::int32_t type_mod = -1;
    std::string type_name;  // format_type(), e.g. "numeric(12,2)"
    bool not_null = false;
    bool is_enum = false;
};

// The source table as the PostgreSQL catalog describes it.
struct TableSchema {
    std::string schema;
    std::string name;
    std::uint32_t oid = 0;
    char replica_identity = 'd';  // d(efault), n(othing), f(ull), i(ndex)
    std::vector<ColumnSchema> columns;     // in attribute order
    std::vector<std::string> primary_key;  // in index order

    std::string qualified() const { return schema + "." + name; }
};

}  // namespace recordflow::core
