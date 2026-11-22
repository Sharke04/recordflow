// [U13] Chunk window (decision 5.2, dblog.md 10.6): changes inside the window
// remove their keys from the chunk (inserts and updates by `after`, deletes by
// `before`, a key change by both), TRUNCATE empties it, survivors keep primary
// key order and the cursor is the last key of the raw SELECT result.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "snapshot/chunk_window.hpp"

namespace {

using namespace recordflow;
using core::Op;
using core::Row;
using core::Value;

Value text(const std::string& value) {
    return {Value::Kind::Text, value};
}

// Composite key (region, id) plus one payload column.
Row row(const std::string& region, int id, const std::string& note = "n") {
    return Row{{"region", text(region)}, {"id", text(std::to_string(id))}, {"note", text(note)}};
}

core::ChangeEvent change(Op op, std::optional<Row> before, std::optional<Row> after) {
    core::ChangeEvent event;
    event.op = op;
    event.schema_name = "public";
    event.table = "orders";
    event.before = std::move(before);
    event.after = std::move(after);
    return event;
}

std::vector<int> surviving_ids(const snapshot::ChunkWindow& window) {
    std::vector<int> ids;
    for (const auto& survivor : window.survivors()) ids.push_back(std::stoi(survivor[1].second.text));
    return ids;
}

snapshot::ChunkWindow loaded() {
    snapshot::ChunkWindow window;
    window.load({"region", "id"}, {row("eu", 1), row("eu", 2), row("eu", 3), row("us", 4), row("us", 5)});
    return window;
}

TEST(ChunkWindow, SerializedKeysDoNotCollide) {
    EXPECT_NE(snapshot::serialize_key({"a|b"}), snapshot::serialize_key({"a", "b"}));
    EXPECT_NE(snapshot::serialize_key({"1:a", ""}), snapshot::serialize_key({"1", "a"}));
    EXPECT_EQ(snapshot::serialize_key({"eu", "1"}), snapshot::serialize_key({"eu", "1"}));
}

TEST(ChunkWindow, EachEventTypeRemovesItsKey) {
    snapshot::ChunkWindow window = loaded();
    window.on_change(change(Op::Insert, std::nullopt, row("eu", 2)));
    window.on_change(change(Op::Update, std::nullopt, row("us", 4, "changed")));
    // A delete under REPLICA IDENTITY DEFAULT carries only the key columns.
    window.on_change(change(Op::Delete, Row{{"region", text("eu")}, {"id", text("1")}}, std::nullopt));
    // Keys outside the chunk and same values under another key are ignored.
    window.on_change(change(Op::Insert, std::nullopt, row("eu", 99)));
    window.on_change(change(Op::Update, std::nullopt, row("us", 3)));

    EXPECT_EQ(surviving_ids(window), (std::vector<int>{3, 5}));
    EXPECT_EQ(window.surviving(), 2u);
    EXPECT_EQ(window.raw_size(), 5u);
}

TEST(ChunkWindow, KeyChangeRemovesTheOldAndTheNewKey) {
    snapshot::ChunkWindow window = loaded();
    // UPDATE ... SET id = 5 WHERE region = 'eu' AND id = 3 (old key 'K').
    window.on_change(change(Op::Update, Row{{"region", text("eu")}, {"id", text("3")}}, row("us", 5)));
    EXPECT_EQ(surviving_ids(window), (std::vector<int>{1, 2, 4}));
}

TEST(ChunkWindow, UnreadableKeyClearsTheChunk) {
    snapshot::ChunkWindow window = loaded();
    window.on_change(
        change(Op::Update, std::nullopt, Row{{"region", {Value::Kind::UnchangedToast, ""}}, {"id", text("1")}}));
    EXPECT_TRUE(window.survivors().empty());
}

TEST(ChunkWindow, TruncateEmptiesTheChunk) {
    snapshot::ChunkWindow window = loaded();
    window.on_truncate();
    EXPECT_TRUE(window.survivors().empty());
    EXPECT_EQ(window.surviving(), 0u);

    snapshot::ChunkWindow other = loaded();
    other.on_change(change(Op::Truncate, std::nullopt, std::nullopt));
    EXPECT_TRUE(other.survivors().empty());
}

TEST(ChunkWindow, CursorIsTheLastRawKeyEvenWhenItWasRemoved) {
    snapshot::ChunkWindow window = loaded();
    window.on_change(change(Op::Delete, row("us", 5), std::nullopt));
    window.on_truncate();
    ASSERT_TRUE(window.last_raw_key());
    EXPECT_EQ(*window.last_raw_key(), (std::vector<std::string>{"us", "5"}));

    snapshot::ChunkWindow empty;
    empty.load({"region", "id"}, {});
    EXPECT_FALSE(empty.last_raw_key());
    EXPECT_EQ(empty.raw_size(), 0u);
}

TEST(ChunkWindow, SurvivorsKeepTheirValuesAndOrder) {
    snapshot::ChunkWindow window;
    window.load({"region", "id"}, {row("eu", 1, "first"), row("eu", 2), row("us", 5, "last")});
    window.on_change(change(Op::Insert, std::nullopt, row("eu", 2)));
    const std::vector<Row> survivors = window.survivors();
    ASSERT_EQ(survivors.size(), 2u);
    EXPECT_EQ(survivors.front()[1].second.text, "1");
    EXPECT_EQ(survivors.front()[2].second.text, "first");
    EXPECT_EQ(survivors.back()[0].second.text, "us");
    EXPECT_EQ(survivors.back()[2].second.text, "last");
}

}  // namespace
