#include <gtest/gtest.h>

#include <clickhouse/columns/bool.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>

#include <cmath>
#include <string>
#include <vector>

#include "runtime/errors.hpp"
#include "sink/clickhouse/value_converter.hpp"

namespace {

using namespace recordflow;
using namespace recordflow::sink::clickhouse;
namespace ch = ::clickhouse;

core::Value text(const std::string& t) { return core::Value{core::Value::Kind::Text, t}; }
core::Value null_value() { return core::Value{}; }

template <typename Column>
std::shared_ptr<Column> build(ChType type, const std::vector<core::Value>& values) {
    ColumnBuilder builder("c", type);
    for (const auto& value : values) builder.append(value);
    return builder.take()->As<Column>();
}

}  // namespace

// [U6] numeric -> Decimal(p, s) as scaled integers, with range and scale checks.
TEST(ValueConverter, NumericToDecimal) {
    const ChType money{ChKind::Decimal, PgText::Numeric, false, 12, 2};
    const auto col = build<ch::ColumnDecimal>(money, {text("12.50"), text("-0.05"), text("0"), text("7"),
                                                     text("9999999999.99"), text("-3.1")});
    EXPECT_EQ(col->At(0), ch::Int128(1250));
    EXPECT_EQ(col->At(1), ch::Int128(-5));
    EXPECT_EQ(col->At(2), ch::Int128(0));
    EXPECT_EQ(col->At(3), ch::Int128(700));
    EXPECT_EQ(col->At(4), ch::Int128(999999999999));
    EXPECT_EQ(col->At(5), ch::Int128(-310));

    const ChType wide{ChKind::Decimal, PgText::Numeric, false, 38, 10};
    const auto big = build<ch::ColumnDecimal>(wide, {text("9999999999999999999999999999.9999999999")});
    ch::Int128 expected = 0;
    for (int i = 0; i < 38; ++i) expected = expected * 10 + 9;
    EXPECT_EQ(big->At(0), expected);

    for (const char* bad : {"12345678901.00", "1.234", "NaN", "Infinity", "1e5", "1.2.3", "-"}) {
        ColumnBuilder builder("c", money);
        EXPECT_THROW(builder.append(text(bad)), ConversionError) << bad;
    }

    // Wider than Decimal(38): mapped to String by the type mapping.
    core::ColumnSchema numeric80{"n", 1700, (80 << 16 | 2) + 4, "numeric(80,2)", true, false};
    EXPECT_EQ(map_type(numeric80, false).kind, ChKind::String);
}

// [U7] date/timestamp/timestamptz under DateStyle=ISO and TimeZone=UTC; values
// outside 1900..2299 are clamped, and +/-infinity in a Nullable column is NULL.
TEST(ValueConverter, DatesAndTimestamps) {
    const auto days = build<ch::ColumnDate32>(ChType{ChKind::Date32, PgText::Date},
                                              {text("1970-01-02"), text("2026-11-22"), text("1899-12-31"),
                                               text("infinity"), text("0044-03-15 BC")});
    EXPECT_EQ(days->RawAt(0), 1);
    EXPECT_EQ(days->RawAt(1), 20779);
    EXPECT_EQ(days->RawAt(2), -25567);   // 1900-01-01
    EXPECT_EQ(days->RawAt(3), 120529);   // 2299-12-31
    EXPECT_EQ(days->RawAt(4), -25567);

    const auto local = build<ch::ColumnDateTime64>(ChType{ChKind::DateTime64, PgText::Timestamp},
                                                   {text("1970-01-01 00:00:01.5"), text("2026-11-22 23:01:39.149067")});
    EXPECT_EQ(local->At(0), 1'500'000);
    EXPECT_EQ(local->At(1), 1'795'388'499'149'067);

    const auto utc = build<ch::ColumnDateTime64>(
        ChType{ChKind::DateTime64Utc, PgText::TimestampTz},
        {text("2026-11-22 23:01:39.149067+00"), text("2026-11-23 01:01:39.149067+02"),
         text("2026-11-22 18:31:39.149067-04:30")});
    for (std::size_t i = 0; i < 3; ++i) EXPECT_EQ(utc->At(i), 1'795'388'499'149'067) << i;

    ColumnBuilder nullable("c", ChType{ChKind::DateTime64Utc, PgText::TimestampTz, true});
    nullable.append(text("infinity"));
    nullable.append(text("2026-11-22 23:01:39+00"));
    const auto wrapped = nullable.take()->As<ch::ColumnNullable>();
    EXPECT_TRUE(wrapped->IsNull(0));
    EXPECT_FALSE(wrapped->IsNull(1));

    for (const char* bad : {"2026-13-01", "2026-02-30", "2026-11-22T10:00:00", "abc"}) {
        ColumnBuilder builder("c", ChType{ChKind::Date32, PgText::Date});
        EXPECT_THROW(builder.append(text(bad)), ConversionError) << bad;
    }
}

// [U8] the remaining types, NULL, unchanged TOAST and the schema-change signal
// for a NULL in a column that is not Nullable.
TEST(ValueConverter, OtherTypesNullsAndToast) {
    const auto flags = build<ch::ColumnBool>(ChType{ChKind::Bool, PgText::Bool}, {text("t"), text("f")});
    EXPECT_TRUE(flags->At(0));
    EXPECT_FALSE(flags->At(1));

    const auto floats = build<ch::ColumnFloat64>(ChType{ChKind::Float64, PgText::Float},
                                                 {text("1.5"), text("NaN"), text("-Infinity")});
    EXPECT_EQ(floats->At(0), 1.5);
    EXPECT_TRUE(std::isnan(floats->At(1)));
    EXPECT_TRUE(std::isinf(floats->At(2)) && floats->At(2) < 0);

    const auto bytes = build<ch::ColumnString>(ChType{ChKind::String, PgText::Bytea}, {text("\\x48656c6c6f00ff")});
    EXPECT_EQ(bytes->At(0), std::string_view("Hello\0\xff", 7));

    const auto ids = build<ch::ColumnUUID>(ChType{ChKind::Uuid, PgText::Uuid}, {text("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")});
    EXPECT_EQ(ids->At(0), (ch::UUID{0xa0eebc999c0b4ef8ULL, 0xbb6d6bb9bd380a11ULL}));

    ColumnBuilder integers("c", ChType{ChKind::Int32, PgText::Integer});
    EXPECT_THROW(integers.append(text("12x")), ConversionError);
    EXPECT_THROW(integers.append(text("99999999999")), ConversionError);

    // Nullable: NULL and unchanged TOAST become NULL; the outcome reports TOAST.
    ColumnBuilder note("note", ChType{ChKind::String, PgText::Text, true});
    EXPECT_EQ(note.append(text("x")), AppendOutcome::Ok);
    EXPECT_EQ(note.append(null_value()), AppendOutcome::Ok);
    EXPECT_EQ(note.append(core::Value{core::Value::Kind::UnchangedToast, ""}), AppendOutcome::UnchangedToast);
    const auto notes = note.take()->As<ch::ColumnNullable>();
    EXPECT_FALSE(notes->IsNull(0));
    EXPECT_TRUE(notes->IsNull(1));
    EXPECT_TRUE(notes->IsNull(2));

    // Not Nullable: unchanged TOAST becomes the default, NULL signals a schema change.
    ColumnBuilder customer("customer", ChType{ChKind::String, PgText::Text, false});
    EXPECT_EQ(customer.append(core::Value{core::Value::Kind::UnchangedToast, ""}), AppendOutcome::UnchangedToast);
    EXPECT_EQ(customer.take()->As<ch::ColumnString>()->At(0), "");
    try {
        customer.append(null_value());
        ADD_FAILURE() << "expected SchemaChangeError";
    } catch (const SchemaChangeError& e) {
        EXPECT_NE(e.diff().find("customer"), std::string::npos);
    }
}
