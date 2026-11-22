#include <gtest/gtest.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "core/transaction_assembler.hpp"
#include "pgoutput/decoder.hpp"

namespace {

using namespace recordflow;
using pgoutput::Message;

// Builds pgoutput messages byte by byte (network byte order).
class Frame {
public:
    explicit Frame(char type) { bytes_ += type; }

    Frame& u8(std::uint8_t v) { bytes_ += static_cast<char>(v); return *this; }
    Frame& i16(std::int16_t v) { return be(static_cast<std::uint64_t>(static_cast<std::uint16_t>(v)), 2); }
    Frame& i32(std::int32_t v) { return be(static_cast<std::uint64_t>(static_cast<std::uint32_t>(v)), 4); }
    Frame& i64(std::uint64_t v) { return be(v, 8); }
    Frame& str(const std::string& s) { bytes_ += s; bytes_ += '\0'; return *this; }
    Frame& text(const std::string& s) { u8('t'); i32(static_cast<std::int32_t>(s.size())); bytes_ += s; return *this; }
    Frame& null() { return u8('n'); }
    Frame& unchanged() { return u8('u'); }

    Message decode(pgoutput::PgoutputDecoder& decoder) const { return decoder.decode(bytes_.data(), bytes_.size()); }
    const std::string& bytes() const { return bytes_; }

private:
    Frame& be(std::uint64_t v, int n) {
        for (int shift = (n - 1) * 8; shift >= 0; shift -= 8) bytes_ += static_cast<char>((v >> shift) & 0xff);
        return *this;
    }
    std::string bytes_;
};

constexpr std::int32_t kOrders = 16386;

Frame relation_orders() {
    Frame f('R');
    f.i32(kOrders).str("public").str("orders").u8('d').i16(2);
    f.u8(1).str("id").i32(20).i32(-1);
    f.u8(0).str("note").i32(25).i32(-1);
    return f;
}

pgoutput::Change change_of(const Message& m) { return std::get<pgoutput::Change>(m); }

}  // namespace

// [U1] every pgoutput message v1 needs, including K/O tuples, NULL, unchanged
// TOAST, truncate, logical messages, unknown relations and malformed input.
TEST(Decoder, DecodesAllMessageTypes) {
    pgoutput::PgoutputDecoder decoder;

    const auto begin = std::get<pgoutput::Begin>(Frame('B').i64(0x1C2D668).i64(123456).i32(791).decode(decoder));
    EXPECT_EQ(begin.final_lsn, 0x1C2D668u);
    EXPECT_EQ(begin.commit_ts, 123456);
    EXPECT_EQ(begin.xid, 791u);

    const auto relation = std::get<pgoutput::Relation>(relation_orders().decode(decoder));
    ASSERT_EQ(relation.table->columns.size(), 2u);
    EXPECT_EQ(relation.table->schema, "public");
    EXPECT_EQ(relation.table->name, "orders");
    EXPECT_EQ(relation.table->replica_identity, 'd');
    EXPECT_TRUE(relation.table->columns[0].part_of_key);
    EXPECT_EQ(relation.table->columns[0].type_oid, 20u);
    EXPECT_FALSE(relation.table->columns[1].part_of_key);

    const auto insert = change_of(Frame('I').i32(kOrders).u8('N').i16(2).text("1").null().decode(decoder));
    EXPECT_EQ(insert.op, core::Op::Insert);
    EXPECT_EQ(insert.table->name, "orders");
    EXPECT_FALSE(insert.before);
    ASSERT_TRUE(insert.after);
    EXPECT_EQ((*insert.after)[0].first, "id");
    EXPECT_EQ((*insert.after)[0].second.text, "1");
    EXPECT_EQ((*insert.after)[1].second.kind, core::Value::Kind::Null);

    // Primary key changed: old key tuple ('K'), then the new row with an unchanged TOAST value.
    const auto pk_update = change_of(Frame('U').i32(kOrders).u8('K').i16(2).text("1").null()
                                         .u8('N').i16(2).text("2").unchanged().decode(decoder));
    EXPECT_EQ(pk_update.op, core::Op::Update);
    EXPECT_TRUE(pk_update.before_is_key_only);
    EXPECT_EQ((*pk_update.before)[0].second.text, "1");
    EXPECT_EQ((*pk_update.after)[0].second.text, "2");
    EXPECT_EQ((*pk_update.after)[1].second.kind, core::Value::Kind::UnchangedToast);

    const auto plain_update = change_of(Frame('U').i32(kOrders).u8('N').i16(2).text("2").text("x").decode(decoder));
    EXPECT_FALSE(plain_update.before);
    EXPECT_EQ((*plain_update.after)[1].second.text, "x");

    const auto full_update = change_of(Frame('U').i32(kOrders).u8('O').i16(2).text("2").text("x")
                                           .u8('N').i16(2).text("2").text("y").decode(decoder));
    EXPECT_FALSE(full_update.before_is_key_only);
    EXPECT_EQ((*full_update.before)[1].second.text, "x");

    const auto del = change_of(Frame('D').i32(kOrders).u8('K').i16(2).text("2").null().decode(decoder));
    EXPECT_EQ(del.op, core::Op::Delete);
    EXPECT_EQ((*del.before)[0].second.text, "2");
    EXPECT_FALSE(del.after);

    const auto truncate = std::get<pgoutput::Truncate>(Frame('T').i32(1).u8(2).i32(kOrders).decode(decoder));
    ASSERT_EQ(truncate.tables.size(), 1u);
    EXPECT_EQ(truncate.tables[0]->name, "orders");
    EXPECT_EQ(truncate.options, 2u);

    Frame watermark('M');
    watermark.u8(1).i64(0x1C2CEC0).str("recordflow").i32(5);
    const std::string bytes = watermark.bytes() + "L:abc";
    const auto message = std::get<pgoutput::LogicalMessage>(decoder.decode(bytes.data(), bytes.size()));
    EXPECT_TRUE(message.transactional);
    EXPECT_EQ(message.lsn, 0x1C2CEC0u);
    EXPECT_EQ(message.prefix, "recordflow");
    EXPECT_EQ(message.content, "L:abc");
    // The content length says 5 but no bytes follow: malformed.
    EXPECT_THROW(watermark.decode(decoder), std::runtime_error);

    const auto commit = std::get<pgoutput::Commit>(Frame('C').u8(0).i64(0x1C2D668).i64(0x1C2D698).i64(123456).decode(decoder));
    EXPECT_EQ(commit.commit_lsn, 0x1C2D668u);
    EXPECT_EQ(commit.end_lsn, 0x1C2D698u);

    // A change for a relation that was never announced keeps its OID visible.
    const auto unknown = change_of(Frame('I').i32(999).u8('N').i16(1).text("1").decode(decoder));
    EXPECT_EQ(unknown.table->name, "oid=999");

    EXPECT_EQ(std::get<pgoutput::Other>(Frame('O').i64(1).str("origin").decode(decoder)).type, 'O');
    EXPECT_THROW(Frame('I').i32(kOrders).u8('N').i16(2).text("1").decode(decoder), std::runtime_error);

    // reset() forgets relations, like a new replication session.
    decoder.reset();
    EXPECT_EQ(change_of(Frame('I').i32(kOrders).u8('N').i16(0).decode(decoder)).table->name, "oid=16386");
}

// [U2] versions are (commit LSN, seq): seq restarts per transaction, versions
// grow across transactions in commit order, and a replayed transaction gets
// the same versions again.
TEST(TransactionAssembler, AssignsDeterministicVersionsAndKeys) {
    pgoutput::PgoutputDecoder decoder;
    core::TransactionAssembler assembler;
    relation_orders().decode(decoder);
    assembler.set_primary_key("public.orders", {"id"});

    const auto insert = change_of(Frame('I').i32(kOrders).u8('N').i16(2).text("1").text("a").decode(decoder));
    const auto update = change_of(Frame('U').i32(kOrders).u8('N').i16(2).text("1").text("b").decode(decoder));
    const auto del = change_of(Frame('D').i32(kOrders).u8('K').i16(2).text("1").null().decode(decoder));
    const auto truncate = std::get<pgoutput::Truncate>(Frame('T').i32(1).u8(0).i32(kOrders).decode(decoder));

    EXPECT_THROW(assembler.change(insert), std::runtime_error);  // outside a transaction

    auto run_first_transaction = [&] {
        std::vector<core::ChangeEvent> events;
        assembler.begin({.final_lsn = 0x200, .commit_ts = 1, .xid = 7});
        events.push_back(assembler.change(insert));
        events.push_back(assembler.change(update));
        events.push_back(assembler.change(del));
        for (auto& e : assembler.truncate(truncate)) events.push_back(e);
        assembler.commit({.flags = 0, .commit_lsn = 0x200, .end_lsn = 0x230, .commit_ts = 1});
        return events;
    };

    const auto first = run_first_transaction();
    ASSERT_EQ(first.size(), 4u);
    for (std::size_t i = 0; i < first.size(); ++i) {
        EXPECT_EQ(first[i].version, (core::Version{0x200, i}));
        EXPECT_EQ(first[i].xid, 7u);
        EXPECT_EQ(first[i].source, core::EventSource::Log);
    }
    EXPECT_EQ(first[0].key, std::vector<std::string>{"1"});
    EXPECT_EQ(first[2].key, std::vector<std::string>{"1"});  // delete: from the old key
    EXPECT_EQ(first[0].key_columns, std::vector<std::string>{"id"});
    EXPECT_EQ(first[3].op, core::Op::Truncate);

    assembler.begin({.final_lsn = 0x300, .commit_ts = 2, .xid = 8});
    const auto later = assembler.change(insert);
    assembler.commit({.flags = 0, .commit_lsn = 0x300, .end_lsn = 0x330, .commit_ts = 2});
    EXPECT_EQ(later.version, (core::Version{0x300, 0}));
    EXPECT_GT(later.version, first.back().version);

    // Replaying the first transaction (after a restart) yields identical versions.
    const auto replay = run_first_transaction();
    for (std::size_t i = 0; i < first.size(); ++i) EXPECT_EQ(replay[i].version, first[i].version);

    // Recently committed xids, with wraparound-aware forgetting.
    EXPECT_EQ(assembler.recent_xids(), (std::deque<core::Xid>{7, 8, 7}));
    assembler.forget_xids_before(8);
    EXPECT_EQ(assembler.recent_xids(), (std::deque<core::Xid>{8}));
    EXPECT_TRUE(core::xid_precedes(0xFFFFFFF0u, 5u));
    EXPECT_FALSE(core::xid_precedes(5u, 0xFFFFFFF0u));
}
