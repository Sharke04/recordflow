#include "sink/clickhouse/value_converter.hpp"

#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <format>
#include <limits>
#include <utility>

#include "runtime/errors.hpp"
#include "runtime/log.hpp"

namespace recordflow::sink::clickhouse {

namespace ch = ::clickhouse;

namespace {

ch::ColumnRef make_column(const ChType& type) {
    switch (type.kind) {
        case ChKind::Int16:                return std::make_shared<ch::ColumnInt16>();
        case ChKind::Int32:                return std::make_shared<ch::ColumnInt32>();
        case ChKind::Int64:                return std::make_shared<ch::ColumnInt64>();
        case ChKind::Float32:              return std::make_shared<ch::ColumnFloat32>();
        case ChKind::Float64:              return std::make_shared<ch::ColumnFloat64>();
        case ChKind::Bool:                 return std::make_shared<ch::ColumnBool>();
        case ChKind::Decimal:
            return std::make_shared<ch::ColumnDecimal>(static_cast<std::size_t>(type.precision),
                                                       static_cast<std::size_t>(type.scale));
        case ChKind::Uuid:                 return std::make_shared<ch::ColumnUUID>();
        case ChKind::Date32:               return std::make_shared<ch::ColumnDate32>();
        case ChKind::DateTime64:           return std::make_shared<ch::ColumnDateTime64>(6);
        case ChKind::DateTime64Utc:        return std::make_shared<ch::ColumnDateTime64>(6, "UTC");
        case ChKind::LowCardinalityString: return std::make_shared<ch::ColumnLowCardinalityT<ch::ColumnString>>();
        default:                           return std::make_shared<ch::ColumnString>();
    }
}

template <typename T>
T parse_integer(std::string_view text, const std::string& column) {
    T value{};
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc() || ptr != text.data() + text.size()) {
        throw ConversionError(std::format("column {}: '{}' is not a valid integer", column, text));
    }
    return value;
}

template <typename T>
T parse_float(std::string_view text, const std::string& column) {
    if (text == "NaN") return std::numeric_limits<T>::quiet_NaN();
    if (text == "Infinity") return std::numeric_limits<T>::infinity();
    if (text == "-Infinity") return -std::numeric_limits<T>::infinity();
    T value{};
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc() || ptr != text.data() + text.size()) {
        throw ConversionError(std::format("column {}: '{}' is not a valid floating-point value", column, text));
    }
    return value;
}

// "-123.45" with scale 3 -> -123450. numeric output never uses exponents.
ch::Int128 parse_decimal(std::string_view text, const ChType& type, const std::string& column) {
    const auto fail = [&](std::string_view why) -> ConversionError {
        return ConversionError(std::format("column {}: '{}' {} for {}", column, text, why, type.name()));
    };
    if (text == "NaN" || text == "Infinity" || text == "-Infinity") throw fail("cannot be stored");

    std::string_view rest = text;
    bool negative = false;
    if (!rest.empty() && (rest.front() == '-' || rest.front() == '+')) {
        negative = rest.front() == '-';
        rest.remove_prefix(1);
    }
    const auto dot = rest.find('.');
    std::string_view integer = rest.substr(0, dot);
    const std::string_view fraction = dot == std::string_view::npos ? std::string_view{} : rest.substr(dot + 1);
    if (integer.empty() && fraction.empty()) throw fail("is not a valid number");
    while (integer.size() > 1 && integer.front() == '0') integer.remove_prefix(1);

    const auto digits = [](std::string_view part) {
        for (char ch : part) {
            if (ch < '0' || ch > '9') return false;
        }
        return true;
    };
    if (!digits(integer) || !digits(fraction)) throw fail("is not a valid number");
    if (static_cast<int>(fraction.size()) > type.scale) throw fail("has more decimal places than the scale");
    const bool zero_integer = integer.empty() || integer == "0";
    if (!zero_integer && static_cast<int>(integer.size()) > type.precision - type.scale) {
        throw fail("is out of range");
    }

    ch::Int128 value = 0;
    for (char ch : integer) value = value * 10 + (ch - '0');
    for (char ch : fraction) value = value * 10 + (ch - '0');
    for (int i = static_cast<int>(fraction.size()); i < type.scale; ++i) value *= 10;
    return negative ? -value : value;
}

// ClickHouse Date32 and DateTime64 cover 1900-01-01 .. 2299-12-31.
constexpr std::chrono::sys_days kMinDay = std::chrono::year{1900} / 1 / 1;
constexpr std::chrono::sys_days kMaxDay = std::chrono::year{2299} / 12 / 31;
constexpr std::int64_t kMicrosPerDay = 86'400'000'000;

struct Temporal {
    enum class Kind { Value, PlusInfinity, MinusInfinity } kind = Kind::Value;
    std::int64_t days = 0;    // since 1970-01-01
    std::int64_t micros = 0;  // since 1970-01-01 00:00:00 UTC (timestamps)
};

int parse_int(std::string_view text, const std::string& column, std::string_view whole) {
    int value = 0;
    const auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (ec != std::errc() || ptr != text.data() + text.size() || text.empty()) {
        throw ConversionError(std::format("column {}: '{}' is not a valid date or time", column, whole));
    }
    return value;
}

// "YYYY-MM-DD[ HH:MM:SS[.ffffff][+HH[:MM[:SS]]]][ BC]" as PostgreSQL prints it with DateStyle=ISO.
Temporal parse_temporal(std::string_view text, bool with_time, bool with_zone, const std::string& column) {
    if (text == "infinity") return {Temporal::Kind::PlusInfinity};
    if (text == "-infinity") return {Temporal::Kind::MinusInfinity};
    const std::string_view whole = text;
    const auto bad = [&] {
        return ConversionError(std::format("column {}: '{}' is not a valid date or time", column, whole));
    };

    if (text.ends_with(" BC")) return {Temporal::Kind::MinusInfinity};  // far before 1900

    const auto dash1 = text.find('-', 1);
    const auto dash2 = dash1 == std::string_view::npos ? dash1 : text.find('-', dash1 + 1);
    if (dash2 == std::string_view::npos || dash2 + 3 > text.size()) throw bad();
    const int year = parse_int(text.substr(0, dash1), column, whole);
    const int month = parse_int(text.substr(dash1 + 1, dash2 - dash1 - 1), column, whole);
    const int day = parse_int(text.substr(dash2 + 1, 2), column, whole);
    const std::chrono::year_month_day ymd{std::chrono::year{year}, std::chrono::month{static_cast<unsigned>(month)},
                                          std::chrono::day{static_cast<unsigned>(day)}};
    if (!ymd.ok()) throw bad();
    Temporal result;
    result.days = std::chrono::sys_days{ymd}.time_since_epoch().count();
    text.remove_prefix(dash2 + 3);
    if (!with_time) {
        if (!text.empty()) throw bad();
        return result;
    }

    if (text.size() < 9 || text[0] != ' ' || text[3] != ':' || text[6] != ':') throw bad();
    const int hour = parse_int(text.substr(1, 2), column, whole);
    const int minute = parse_int(text.substr(4, 2), column, whole);
    const int second = parse_int(text.substr(7, 2), column, whole);
    text.remove_prefix(9);
    std::int64_t fraction = 0;
    if (!text.empty() && text.front() == '.') {
        text.remove_prefix(1);
        int digits = 0;
        while (!text.empty() && text.front() >= '0' && text.front() <= '9') {
            if (digits < 6) fraction = fraction * 10 + (text.front() - '0');
            ++digits;
            text.remove_prefix(1);
        }
        if (digits == 0) throw bad();
        for (; digits < 6; ++digits) fraction *= 10;
    }
    std::int64_t offset_seconds = 0;
    if (with_zone) {
        if (text.empty() || (text.front() != '+' && text.front() != '-')) throw bad();
        const int sign = text.front() == '-' ? -1 : 1;
        text.remove_prefix(1);
        int parts[3] = {0, 0, 0};
        for (int i = 0; i < 3 && !text.empty(); ++i) {
            if (i > 0) {
                if (text.front() != ':') throw bad();
                text.remove_prefix(1);
            }
            parts[i] = parse_int(text.substr(0, 2), column, whole);
            text.remove_prefix(std::min<std::size_t>(2, text.size()));
        }
        offset_seconds = sign * (parts[0] * 3600 + parts[1] * 60 + parts[2]);
    }
    if (!text.empty()) throw bad();
    result.micros = result.days * kMicrosPerDay +
                    (static_cast<std::int64_t>(hour) * 3600 + minute * 60 + second - offset_seconds) * 1'000'000 +
                    fraction;
    return result;
}

// Values outside the ClickHouse range (including +/-infinity) become the
// nearest representable value.
std::int32_t clamp_days(const Temporal& t, const std::string& column, std::string_view text) {
    const std::int64_t low = kMinDay.time_since_epoch().count();
    const std::int64_t high = kMaxDay.time_since_epoch().count();
    std::int64_t days = t.days;
    if (t.kind == Temporal::Kind::PlusInfinity || days > high) days = high;
    if (t.kind == Temporal::Kind::MinusInfinity || days < low) days = low;
    if (days != t.days || t.kind != Temporal::Kind::Value) {
        log::warn("column {}: '{}' is outside the ClickHouse date range, stored as the nearest limit", column, text);
    }
    return static_cast<std::int32_t>(days);
}

std::int64_t clamp_micros(const Temporal& t, const std::string& column, std::string_view text) {
    const std::int64_t low = kMinDay.time_since_epoch().count() * kMicrosPerDay;
    const std::int64_t high = (kMaxDay.time_since_epoch().count() + 1) * kMicrosPerDay - 1;
    std::int64_t micros = t.micros;
    if (t.kind == Temporal::Kind::PlusInfinity || micros > high) micros = high;
    if (t.kind == Temporal::Kind::MinusInfinity || micros < low) micros = low;
    if (micros != t.micros || t.kind != Temporal::Kind::Value) {
        log::warn("column {}: '{}' is outside the ClickHouse time range, stored as the nearest limit", column, text);
    }
    return micros;
}

int hex_digit(char ch) {
    if (ch >= '0' && ch <= '9') return ch - '0';
    if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
    if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
    return -1;
}

// "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" -> (high 64 bits, low 64 bits)
ch::UUID parse_uuid(std::string_view text, const std::string& column) {
    std::uint64_t halves[2] = {0, 0};
    int digits = 0;
    for (char c : text) {
        if (c == '-') continue;
        const int value = hex_digit(c);
        if (value < 0 || digits >= 32) {
            throw ConversionError(std::format("column {}: '{}' is not a valid uuid", column, text));
        }
        halves[digits / 16] = (halves[digits / 16] << 4) | static_cast<std::uint64_t>(value);
        ++digits;
    }
    if (digits != 32) throw ConversionError(std::format("column {}: '{}' is not a valid uuid", column, text));
    return {halves[0], halves[1]};
}

// bytea_output=hex: "\x48656c6c6f" -> "Hello"
std::string parse_bytea(std::string_view text, const std::string& column) {
    if (!text.starts_with("\\x") || text.size() % 2 != 0) {
        throw ConversionError(std::format("column {}: bytea value is not in hex format", column));
    }
    std::string bytes;
    bytes.reserve((text.size() - 2) / 2);
    for (std::size_t i = 2; i < text.size(); i += 2) {
        const int high = hex_digit(text[i]);
        const int low = hex_digit(text[i + 1]);
        if (high < 0 || low < 0) throw ConversionError(std::format("column {}: invalid hex in bytea value", column));
        bytes.push_back(static_cast<char>((high << 4) | low));
    }
    return bytes;
}

bool parse_bool(std::string_view text, const std::string& column) {
    if (text == "t" || text == "true") return true;
    if (text == "f" || text == "false") return false;
    throw ConversionError(std::format("column {}: '{}' is not a valid boolean", column, text));
}

}  // namespace

ColumnBuilder::ColumnBuilder(std::string name, ChType type)
    : name_(std::move(name)),
      type_(type),
      nested_(make_column(type)),
      nulls_(type.nullable ? std::make_shared<ch::ColumnUInt8>() : nullptr) {}

AppendOutcome ColumnBuilder::append(const core::Value& value) {
    switch (value.kind) {
        case core::Value::Kind::Null:
            if (!type_.nullable) {
                throw SchemaChangeError(
                    std::format("column {} received NULL, but its ClickHouse type {} is not Nullable", name_,
                                type_.name()),
                    std::format("~ column \"{}\": NOT NULL was dropped on the source", name_));
            }
            append_null();
            return AppendOutcome::Ok;
        case core::Value::Kind::UnchangedToast:
            append_default();
            return AppendOutcome::UnchangedToast;
        case core::Value::Kind::Text:
            break;
    }

    const bool temporal = type_.kind == ChKind::Date32 || type_.kind == ChKind::DateTime64 ||
                          type_.kind == ChKind::DateTime64Utc;
    if (temporal && type_.nullable && (value.text == "infinity" || value.text == "-infinity")) {
        log::warn("column {}: '{}' has no ClickHouse equivalent, stored as NULL", name_, value.text);
        append_null();
        return AppendOutcome::Ok;
    }

    append_text(value.text);
    if (nulls_) nulls_->Append(0);
    ++size_;
    return AppendOutcome::Ok;
}

void ColumnBuilder::append_default() {
    if (type_.nullable) {
        append_null();
        return;
    }
    append_nested_default();
    ++size_;
}

void ColumnBuilder::append_null() {
    append_nested_default();
    nulls_->Append(1);
    ++size_;
}

void ColumnBuilder::append_nested_default() {
    switch (type_.kind) {
        case ChKind::Int16:         nested_->As<ch::ColumnInt16>()->Append(0); return;
        case ChKind::Int32:         nested_->As<ch::ColumnInt32>()->Append(0); return;
        case ChKind::Int64:         nested_->As<ch::ColumnInt64>()->Append(0); return;
        case ChKind::Float32:       nested_->As<ch::ColumnFloat32>()->Append(0); return;
        case ChKind::Float64:       nested_->As<ch::ColumnFloat64>()->Append(0); return;
        case ChKind::Bool:          nested_->As<ch::ColumnBool>()->Append(false); return;
        case ChKind::Decimal:       nested_->As<ch::ColumnDecimal>()->Append(ch::Int128(0)); return;
        case ChKind::Date32:        nested_->As<ch::ColumnDate32>()->AppendRaw(0); return;
        case ChKind::DateTime64:
        case ChKind::DateTime64Utc: nested_->As<ch::ColumnDateTime64>()->Append(0); return;
        case ChKind::Uuid:          nested_->As<ch::ColumnUUID>()->Append(ch::UUID{0, 0}); return;
        case ChKind::LowCardinalityString:
            nested_->As<ch::ColumnLowCardinalityT<ch::ColumnString>>()->Append(std::string_view{});
            return;
        case ChKind::String:        nested_->As<ch::ColumnString>()->Append(std::string_view{}); return;
    }
}

void ColumnBuilder::append_text(std::string_view text) {
    switch (type_.kind) {
        case ChKind::Int16:
            nested_->As<ch::ColumnInt16>()->Append(parse_integer<std::int16_t>(text, name_));
            return;
        case ChKind::Int32:
            nested_->As<ch::ColumnInt32>()->Append(parse_integer<std::int32_t>(text, name_));
            return;
        case ChKind::Int64:
            nested_->As<ch::ColumnInt64>()->Append(parse_integer<std::int64_t>(text, name_));
            return;
        case ChKind::Float32:
            nested_->As<ch::ColumnFloat32>()->Append(parse_float<float>(text, name_));
            return;
        case ChKind::Float64:
            nested_->As<ch::ColumnFloat64>()->Append(parse_float<double>(text, name_));
            return;
        case ChKind::Bool:
            nested_->As<ch::ColumnBool>()->Append(parse_bool(text, name_));
            return;
        case ChKind::Decimal:
            nested_->As<ch::ColumnDecimal>()->Append(parse_decimal(text, type_, name_));
            return;
        case ChKind::Date32:
            nested_->As<ch::ColumnDate32>()->AppendRaw(
                clamp_days(parse_temporal(text, false, false, name_), name_, text));
            return;
        case ChKind::DateTime64:
            nested_->As<ch::ColumnDateTime64>()->Append(
                clamp_micros(parse_temporal(text, true, false, name_), name_, text));
            return;
        case ChKind::DateTime64Utc:
            nested_->As<ch::ColumnDateTime64>()->Append(
                clamp_micros(parse_temporal(text, true, true, name_), name_, text));
            return;
        case ChKind::LowCardinalityString:
            nested_->As<ch::ColumnLowCardinalityT<ch::ColumnString>>()->Append(text);
            return;
        case ChKind::Uuid:
            nested_->As<ch::ColumnUUID>()->Append(parse_uuid(text, name_));
            return;
        case ChKind::String:
            if (type_.source == PgText::Bytea) {
                nested_->As<ch::ColumnString>()->Append(parse_bytea(text, name_));
            } else {
                // text, varchar, json(b), arrays, interval, time, wide numerics, custom types
                nested_->As<ch::ColumnString>()->Append(text);
            }
            return;
        default:
            throw ConversionError(std::format("column {}: type {} is not supported yet", name_, type_.name()));
    }
}

ch::ColumnRef ColumnBuilder::take() {
    ch::ColumnRef finished = std::exchange(nested_, make_column(type_));
    size_ = 0;
    if (!nulls_) return finished;
    auto nulls = std::exchange(nulls_, std::make_shared<ch::ColumnUInt8>());
    return std::make_shared<ch::ColumnNullable>(finished, nulls);
}

}  // namespace recordflow::sink::clickhouse
