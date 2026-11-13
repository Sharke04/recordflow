#include <gtest/gtest.h>

#include <string>

#include "core/schema_guard.hpp"

namespace {

using namespace recordflow::core;

constexpr std::uint32_t kInt8 = 20, kText = 25, kVarchar = 1043, kNumeric = 1700, kInt4 = 23;

std::int32_t numeric_mod(int precision, int scale) { return ((precision << 16) | scale) + 4; }

TableSchema orders() {
    TableSchema t;
    t.schema = "public";
    t.name = "orders";
    t.columns = {
        {"id", kInt8, -1, "bigint", true, false},
        {"customer", kVarchar, 54, "character varying(50)", true, false},
        {"amount", kNumeric, numeric_mod(10, 2), "numeric(10,2)", false, false},
    };
    t.primary_key = {"id"};
    return t;
}

bool has_line(const SchemaDiff& diff, const std::string& part) {
    for (const auto& line : diff.lines) {
        if (line.find(part) != std::string::npos) return true;
    }
    return false;
}

}  // namespace

// [U9] harmless changes pass; columns, types and the key that change the
// ClickHouse schema produce readable differences (decision 7.1).
TEST(SchemaGuard, SeparatesHarmlessChangesFromBreakingOnes) {
    const TargetSchema existing = derive_target(orders());
    EXPECT_EQ(existing.columns[2].type, "Nullable(Decimal(10, 2))");
    EXPECT_TRUE(compare(existing, derive_target(orders())).empty());

    // varchar(50) -> varchar(100) and text are all String: not a change.
    TableSchema wider = orders();
    wider.columns[1] = {"customer", kText, -1, "text", true, false};
    EXPECT_TRUE(compare(existing, derive_target(wider)).empty());

    // numeric(10,2) -> numeric(12,2) changes the ClickHouse type; a new column too.
    TableSchema changed = orders();
    changed.columns[2] = {"amount", kNumeric, numeric_mod(12, 2), "numeric(12,2)", false, false};
    changed.columns.push_back({"discount", kNumeric, -1, "numeric", false, false});
    const SchemaDiff diff = compare(existing, derive_target(changed));
    EXPECT_TRUE(has_line(diff, "~ column \"amount\": numeric(10,2) -> Nullable(Decimal(10, 2)) now "
                               "numeric(12,2) -> Nullable(Decimal(12, 2))"))
        << diff.text();
    EXPECT_TRUE(has_line(diff, "+ new column \"discount\" (numeric -> Nullable(String))")) << diff.text();

    TableSchema dropped = orders();
    dropped.columns.pop_back();
    EXPECT_TRUE(has_line(compare(existing, derive_target(dropped)), "- dropped column \"amount\""));

    TableSchema rekeyed = orders();
    rekeyed.columns.push_back({"region", kInt4, -1, "integer", true, false});
    rekeyed.primary_key = {"id", "region"};
    const SchemaDiff key_diff = compare(existing, derive_target(rekeyed));
    EXPECT_TRUE(has_line(key_diff, "~ primary key: (id) now (id, region)")) << key_diff.text();

    TableSchema reordered = orders();
    std::swap(reordered.columns[1], reordered.columns[2]);
    EXPECT_TRUE(has_line(compare(existing, derive_target(reordered)), "~ column order"));
}
