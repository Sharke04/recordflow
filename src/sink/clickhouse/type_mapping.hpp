#pragma once

#include <cstdint>
#include <string>

#include "core/table_schema.hpp"

// PostgreSQL to ClickHouse type mapping (decision 2.4).
namespace recordflow::sink::clickhouse {

enum class ChKind {
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    String,
    LowCardinalityString,
    Decimal,
    Date32,
    DateTime64,     // timestamp without time zone
    DateTime64Utc,  // timestamp with time zone
    Uuid,
};

// What a PostgreSQL text value has to be parsed as.
enum class PgText { Integer, Float, Bool, Numeric, Date, Timestamp, TimestampTz, Uuid, Bytea, Text };

struct ChType {
    ChKind kind = ChKind::String;
    PgText source = PgText::Text;
    bool nullable = false;
    int precision = 0;  // Decimal
    int scale = 0;      // Decimal

    // The type name as ClickHouse reports it in system.columns,
    // e.g. "Nullable(Decimal(12, 2))" or "DateTime64(6, 'UTC')".
    std::string name() const;

    bool operator==(const ChType&) const = default;
};

// Primary-key columns are never Nullable. Enums map to LowCardinality(String)
// (Nullable(String) when the column allows NULL).
ChType map_type(const core::ColumnSchema& column, bool part_of_primary_key);

// PostgreSQL type OIDs used by the mapping.
namespace pg_oid {
inline constexpr std::uint32_t kBool = 16;
inline constexpr std::uint32_t kBytea = 17;
inline constexpr std::uint32_t kChar = 18;
inline constexpr std::uint32_t kName = 19;
inline constexpr std::uint32_t kInt8 = 20;
inline constexpr std::uint32_t kInt2 = 21;
inline constexpr std::uint32_t kInt4 = 23;
inline constexpr std::uint32_t kText = 25;
inline constexpr std::uint32_t kJson = 114;
inline constexpr std::uint32_t kFloat4 = 700;
inline constexpr std::uint32_t kFloat8 = 701;
inline constexpr std::uint32_t kBpchar = 1042;
inline constexpr std::uint32_t kVarchar = 1043;
inline constexpr std::uint32_t kDate = 1082;
inline constexpr std::uint32_t kTimestamp = 1114;
inline constexpr std::uint32_t kTimestampTz = 1184;
inline constexpr std::uint32_t kNumeric = 1700;
inline constexpr std::uint32_t kUuid = 2950;
inline constexpr std::uint32_t kJsonb = 3802;
}  // namespace pg_oid

}  // namespace recordflow::sink::clickhouse
