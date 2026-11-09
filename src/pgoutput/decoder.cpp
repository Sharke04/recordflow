#include "pgoutput/decoder.hpp"

#include <stdexcept>
#include <string>
#include <utility>

namespace recordflow::pgoutput {

using core::Op;
using core::Row;
using core::Value;

Message PgoutputDecoder::decode(const char* data, std::size_t len) {
    if (len == 0) throw std::runtime_error("empty pgoutput message");

    ByteReader buf(data, len);
    const char type = static_cast<char>(buf.get());
    switch (type) {
        case 'B': {
            Begin begin;
            begin.final_lsn = static_cast<core::Lsn>(buf.getLong());
            begin.commit_ts = buf.getLong();
            begin.xid = static_cast<core::Xid>(buf.getInt());
            return begin;
        }
        case 'C': {
            Commit commit;
            commit.flags = buf.get();
            commit.commit_lsn = static_cast<core::Lsn>(buf.getLong());
            commit.end_lsn = static_cast<core::Lsn>(buf.getLong());
            commit.commit_ts = buf.getLong();
            return commit;
        }
        case 'R':
            return decode_relation(buf);
        case 'I':
            return decode_change(buf, Op::Insert);
        case 'U':
            return decode_change(buf, Op::Update);
        case 'D':
            return decode_change(buf, Op::Delete);
        case 'T':
            return decode_truncate(buf);
        case 'M': {
            // Protocol version 1: no xid, since transactions are never streamed.
            LogicalMessage message;
            message.transactional = (buf.get() & 0x01) != 0;
            message.lsn = static_cast<core::Lsn>(buf.getLong());
            message.prefix = buf.getString();
            message.content = buf.getCountedBytes();
            return message;
        }
        default:
            return Other{type};
    }
}

void PgoutputDecoder::reset() {
    relations_.clear();
}

std::shared_ptr<const TableInfo> PgoutputDecoder::relation(std::uint32_t oid) const {
    const auto it = relations_.find(oid);
    if (it != relations_.end()) return it->second;
    // No Relation message for this OID: keep the OID visible in the name.
    auto placeholder = std::make_shared<TableInfo>();
    placeholder->oid = oid;
    placeholder->name = "oid=" + std::to_string(oid);
    return placeholder;
}

Relation PgoutputDecoder::decode_relation(ByteReader& buf) {
    auto table = std::make_shared<TableInfo>();
    table->oid = static_cast<std::uint32_t>(buf.getInt());
    table->schema = buf.getString();
    table->name = buf.getString();
    table->replica_identity = static_cast<char>(buf.get());
    const std::int16_t ncols = buf.getShort();
    if (ncols > 0) table->columns.reserve(static_cast<std::size_t>(ncols));
    for (std::int16_t i = 0; i < ncols; ++i) {
        ColumnInfo column;
        column.part_of_key = (buf.get() & 0x01) != 0;
        column.name = buf.getString();
        column.type_oid = static_cast<std::uint32_t>(buf.getInt());
        column.type_mod = buf.getInt();
        table->columns.push_back(std::move(column));
    }
    relations_[table->oid] = table;
    return Relation{std::move(table)};
}

Change PgoutputDecoder::decode_change(ByteReader& buf, Op op) {
    Change change;
    change.op = op;
    change.table = relation(static_cast<std::uint32_t>(buf.getInt()));

    char kind = static_cast<char>(buf.get());
    if (op != Op::Insert && (kind == 'K' || kind == 'O')) {
        change.before_is_key_only = kind == 'K';
        change.before = decode_tuple(buf, *change.table);
        if (op == Op::Delete) return change;
        kind = static_cast<char>(buf.get());
    }
    if (kind != 'N') {
        throw std::runtime_error(std::string("unexpected tuple marker '") + kind + "'");
    }
    change.after = decode_tuple(buf, *change.table);
    return change;
}

Truncate PgoutputDecoder::decode_truncate(ByteReader& buf) const {
    Truncate truncate;
    const std::int32_t count = buf.getInt();
    truncate.options = buf.get();
    for (std::int32_t i = 0; i < count; ++i) {
        truncate.tables.push_back(relation(static_cast<std::uint32_t>(buf.getInt())));
    }
    return truncate;
}

Row PgoutputDecoder::decode_tuple(ByteReader& buf, const TableInfo& table) const {
    const std::int16_t ncols = buf.getShort();
    Row row;
    if (ncols > 0) row.reserve(static_cast<std::size_t>(ncols));
    for (std::int16_t i = 0; i < ncols; ++i) {
        std::string name = static_cast<std::size_t>(i) < table.columns.size()
                               ? table.columns[static_cast<std::size_t>(i)].name
                               : "col_" + std::to_string(i);
        Value value;
        switch (static_cast<char>(buf.get())) {
            case 't':
                value.kind = Value::Kind::Text;
                value.text = buf.getCountedBytes();
                break;
            case 'u':
                value.kind = Value::Kind::UnchangedToast;
                break;
            case 'n':
                break;
            default:
                throw std::runtime_error("unknown tuple value kind");
        }
        row.emplace_back(std::move(name), std::move(value));
    }
    return row;
}

}  // namespace recordflow::pgoutput
