#include "sink/clickhouse/value_converter.hpp"

#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/string.h>

#include <charconv>
#include <format>
#include <limits>
#include <utility>

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

bool parse_bool(std::string_view text, const std::string& column) {
    if (text == "t" || text == "true") return true;
    if (text == "f" || text == "false") return false;
    throw ConversionError(std::format("column {}: '{}' is not a valid boolean", column, text));
}

}  // namespace

ColumnBuilder::ColumnBuilder(std::string name, ChType type)
    : name_(std::move(name)), type_(type), nested_(make_column(type)) {}

AppendOutcome ColumnBuilder::append(const core::Value& value) {
    if (value.kind != core::Value::Kind::Text) {
        throw ConversionError(std::format("column {}: NULL and unchanged values are not supported yet", name_));
    }
    append_text(value.text);
    ++size_;
    return AppendOutcome::Ok;
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
        case ChKind::LowCardinalityString:
            nested_->As<ch::ColumnLowCardinalityT<ch::ColumnString>>()->Append(text);
            return;
        case ChKind::String:
            nested_->As<ch::ColumnString>()->Append(text);
            return;
        default:
            throw ConversionError(std::format("column {}: type {} is not supported yet", name_, type_.name()));
    }
}

ch::ColumnRef ColumnBuilder::take() {
    ch::ColumnRef finished = std::exchange(nested_, make_column(type_));
    size_ = 0;
    return finished;
}

}  // namespace recordflow::sink::clickhouse
