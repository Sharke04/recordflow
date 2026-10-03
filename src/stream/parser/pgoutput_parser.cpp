#include "stream/parser/pgoutput_parser.hpp"

#include <chrono>
#include <cstdio>
#include <exception>
#include <format>
#include <string>
#include <utility>

#include "stream/printer/change_event_printer.hpp"

namespace pgoutput {

namespace {

ChangeEvent event_for(Op op, const TableInfo& rel) {
    ChangeEvent event;
    event.op = op;
    event.schema = rel.schema;
    event.table = rel.name;
    for (const auto& column : rel.columns) {
        if (column.part_of_key)
            event.key_columns.push_back(column.name);
    }
    return event;
}

}

TableInfo WalMessageDecoder::decodeRelation(pgwire::ByteReader& buf) const {
    buf.get();
    TableInfo t;
    t.oid = static_cast<std::uint32_t>(buf.getInt());
    t.schema = buf.getString();
    t.name = buf.getString();
    t.replica_identity = static_cast<char>(buf.get());
    const std::int16_t ncols = buf.getShort();
    if (ncols > 0) t.columns.reserve(static_cast<std::size_t>(ncols));
    for (std::int16_t i = 0; i < ncols; ++i) {
        ColumnInfo c;
        c.part_of_key = (buf.get() & 0x01) != 0;
        c.name = buf.getString();
        c.type_oid = static_cast<std::uint32_t>(buf.getInt());
        c.type_mod = buf.getInt();
        t.columns.push_back(std::move(c));
    }
    return t;
}

ChangeEvent WalMessageDecoder::decodeInsert(pgwire::ByteReader& buf, const TableInfo& rel) const {
    buf.get();
    buf.getInt();
    buf.get();
    ChangeEvent event = event_for(Op::Insert, rel);
    event.after = decodeTuple(buf, rel);
    return event;
}

ChangeEvent WalMessageDecoder::decodeUpdate(pgwire::ByteReader& buf, const TableInfo& rel) const {
    buf.get();
    buf.getInt();
    ChangeEvent event = event_for(Op::Update, rel);
    const char kind = static_cast<char>(buf.get());
    if (kind == 'K' || kind == 'O') {
        event.before = decodeTuple(buf, rel);
        buf.get();
    }
    event.after = decodeTuple(buf, rel);
    return event;
}

ChangeEvent WalMessageDecoder::decodeDelete(pgwire::ByteReader& buf, const TableInfo& rel) const {
    buf.get();
    buf.getInt();
    buf.get();
    ChangeEvent event = event_for(Op::Delete, rel);
    event.before = decodeTuple(buf, rel);
    return event;
}

Row WalMessageDecoder::decodeTuple(pgwire::ByteReader& buf, const TableInfo& rel) const {
    const std::int16_t ncols = buf.getShort();
    Row row;
    if (ncols > 0) row.reserve(static_cast<std::size_t>(ncols));
    for (std::int16_t i = 0; i < ncols; ++i) {
        std::string col = (static_cast<std::size_t>(i) < rel.columns.size())
                              ? rel.columns[i].name
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
            default:
                break;
        }
        row.emplace_back(std::move(col), std::move(value));
    }
    return row;
}

}

namespace {

using namespace pgoutput;

std::string format_lsn(std::uint64_t lsn) {
    const auto high = static_cast<std::uint32_t>(lsn >> 32);
    const auto low = static_cast<std::uint32_t>(lsn);
    return std::format("{:X}/{:X}", high, low);
}

std::string format_timestamp(std::int64_t micros) {
    const auto t = kPostgresEpoch + std::chrono::microseconds{micros};
    return std::format("{:%F %T}Z", t);
}

}

std::string PgoutputParser::relation_label(std::uint32_t oid) const {
    const auto it = relations_.find(oid);
    if (it != relations_.end()) {
        return it->second.schema + "." + it->second.name;
    }
    return "oid=" + std::to_string(oid);
}

ParsedMessage PgoutputParser::handle_message(const char* data, std::size_t len) {
    if (len == 0)
        return {};

    try {
        pgwire::ByteReader buf(data, len);
        ParsedMessage parsed;
        parsed.type = decoder_.peekType(buf);
        switch (parsed.type) {
            case 'R': {
                TableInfo t = decoder_.decodeRelation(buf);
                relations_[t.oid] = t;
                parsed.relation = std::move(t);
                break;
            }
            case 'I':
            case 'U':
            case 'D': {
                static const TableInfo kUnknown;
                const std::uint32_t oid = decoder_.peekOid(buf);
                const auto it = relations_.find(oid);
                const TableInfo& rel =
                    (it != relations_.end()) ? it->second : kUnknown;

                ChangeEvent event =
                    (parsed.type == 'I')   ? decoder_.decodeInsert(buf, rel)
                    : (parsed.type == 'U') ? decoder_.decodeUpdate(buf, rel)
                                    : decoder_.decodeDelete(buf, rel);

                // No R message has been seen for this OID, so the table name
                // is unknown. Fall back to the OID so the event still says
                // which relation it came from.
                if (rel.oid == 0) {
                    event.table = relation_label(oid);
                }
                event.xid = transaction_.xid;
                event.lsn = transaction_.lsn;
                event.commit_ts = transaction_.commit_ts;
                parsed.events.push_back(std::move(event));
                break;
            }
            case 'B': {
                buf.get();
                const auto lsn = static_cast<std::uint64_t>(buf.getLong());
                const std::int64_t ts = buf.getLong();
                const auto xid = static_cast<std::uint32_t>(buf.getInt());
                transaction_ = {xid, lsn, ts};
                std::printf("BEGIN    xid=%u lsn=%s ts=%s\n", xid,
                            format_lsn(lsn).c_str(), format_timestamp(ts).c_str());
                break;
            }
            case 'C': {
                buf.get();
                buf.get();
                const auto commit_lsn = static_cast<std::uint64_t>(buf.getLong());
                const auto end_lsn = static_cast<std::uint64_t>(buf.getLong());
                const std::int64_t ts = buf.getLong();
                std::printf("COMMIT   lsn=%s end=%s ts=%s\n",
                            format_lsn(commit_lsn).c_str(),
                            format_lsn(end_lsn).c_str(),
                            format_timestamp(ts).c_str());
                parsed.commit_end_lsn = end_lsn;
                break;
            }
            case 'T': {
                buf.get();
                const std::int32_t nrel = buf.getInt();
                buf.get();
                for (std::int32_t i = 0; i < nrel; ++i) {
                    const auto oid = static_cast<std::uint32_t>(buf.getInt());

                    ChangeEvent event;
                    event.op = Op::Truncate;
                    const auto it = relations_.find(oid);
                    if (it != relations_.end()) {
                        event.schema = it->second.schema;
                        event.table = it->second.name;
                    } else {
                        event.table = relation_label(oid);
                    }
                    event.xid = transaction_.xid;
                    event.lsn = transaction_.lsn;
                    event.commit_ts = transaction_.commit_ts;
                    parsed.events.push_back(std::move(event));
                }
                break;
            }
            case 'O':
                std::printf("ORIGIN   (not decoded)\n");
                break;
            case 'Y':
                std::printf("TYPE     (not decoded)\n");
                break;
            default:
                std::printf("?        message type '%c' (0x%02x)\n", parsed.type,
                            static_cast<unsigned char>(parsed.type));
                break;
        }
        std::fflush(stdout);
        return parsed;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "parse error: %s\n", e.what());
        return {};
    }
}
