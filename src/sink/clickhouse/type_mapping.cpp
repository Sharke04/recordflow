#include "sink/clickhouse/type_mapping.hpp"

#include <format>

namespace recordflow::sink::clickhouse {

namespace {

constexpr int kMaxDecimalPrecision = 76;

// numeric(p, s) typmod: ((p << 16) | (s & 0x7ff)) + 4; scale is an 11-bit
// signed value since PostgreSQL 15.
bool numeric_precision_scale(std::int32_t typmod, int& precision, int& scale) {
    if (typmod < 4) return false;
    const std::int32_t packed = typmod - 4;
    precision = (packed >> 16) & 0xffff;
    scale = ((packed & 0x7ff) ^ 1024) - 1024;
    return precision > 0 && precision <= kMaxDecimalPrecision && scale >= 0 && scale <= precision;
}

std::string base_name(const ChType& type) {
    switch (type.kind) {
        case ChKind::Int16:                return "Int16";
        case ChKind::Int32:                return "Int32";
        case ChKind::Int64:                return "Int64";
        case ChKind::Float32:              return "Float32";
        case ChKind::Float64:              return "Float64";
        case ChKind::Bool:                 return "Bool";
        case ChKind::String:               return "String";
        case ChKind::LowCardinalityString: return "LowCardinality(String)";
        case ChKind::Decimal:              return std::format("Decimal({}, {})", type.precision, type.scale);
        case ChKind::Date32:               return "Date32";
        case ChKind::DateTime64:           return "DateTime64(6)";
        case ChKind::DateTime64Utc:        return "DateTime64(6, 'UTC')";
        case ChKind::Uuid:                 return "UUID";
    }
    return "String";
}

}  // namespace

std::string ChType::name() const {
    const std::string base = base_name(*this);
    return nullable ? "Nullable(" + base + ")" : base;
}

ChType map_type(const core::ColumnSchema& column, bool part_of_primary_key) {
    ChType type;
    type.nullable = !column.not_null && !part_of_primary_key;

    if (column.is_enum) {
        type.kind = type.nullable ? ChKind::String : ChKind::LowCardinalityString;
        return type;
    }

    using namespace pg_oid;
    switch (column.type_oid) {
        case kInt2:   type.kind = ChKind::Int16; type.source = PgText::Integer; break;
        case kInt4:   type.kind = ChKind::Int32; type.source = PgText::Integer; break;
        case kInt8:   type.kind = ChKind::Int64; type.source = PgText::Integer; break;
        case kFloat4: type.kind = ChKind::Float32; type.source = PgText::Float; break;
        case kFloat8: type.kind = ChKind::Float64; type.source = PgText::Float; break;
        case kBool:   type.kind = ChKind::Bool; type.source = PgText::Bool; break;
        case kNumeric:
            if (numeric_precision_scale(column.type_mod, type.precision, type.scale)) {
                type.kind = ChKind::Decimal;
                type.source = PgText::Numeric;
            }  // unconstrained or out-of-range numeric stays String
            break;
        case kDate:        type.kind = ChKind::Date32; type.source = PgText::Date; break;
        case kTimestamp:   type.kind = ChKind::DateTime64; type.source = PgText::Timestamp; break;
        case kTimestampTz: type.kind = ChKind::DateTime64Utc; type.source = PgText::TimestampTz; break;
        case kUuid:        type.kind = ChKind::Uuid; type.source = PgText::Uuid; break;
        case kBytea:       type.kind = ChKind::String; type.source = PgText::Bytea; break;
        default:
            // text, varchar, char, name, json(b), arrays, interval, time, custom types
            type.kind = ChKind::String;
            type.source = PgText::Text;
            break;
    }
    return type;
}

}  // namespace recordflow::sink::clickhouse
