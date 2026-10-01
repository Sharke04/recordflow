#include "pg/parser/pgoutput_parser.hpp"

#include <chrono>
#include <cstdio>
#include <exception>
#include <format>
#include <string>
#include <utility>

namespace pgoutput {

TableInfo WalMessageDecoder::decodeRelation(pgwire::ByteCursor& buf) const {
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

DecodedDml WalMessageDecoder::decodeInsert(pgwire::ByteCursor& buf, const TableInfo& rel) const {
    buf.get();
    buf.getInt();
    buf.get();
    return {std::nullopt, decodeTuple(buf, rel)};
}

DecodedDml WalMessageDecoder::decodeUpdate(pgwire::ByteCursor& buf, const TableInfo& rel) const {
    buf.get();
    buf.getInt();
    std::optional<Row> before;
    const char kind = static_cast<char>(buf.get());
    if (kind == 'K' || kind == 'O') {
        before = decodeTuple(buf, rel);
        buf.get();
    }
    return {std::move(before), decodeTuple(buf, rel)};
}

DecodedDml WalMessageDecoder::decodeDelete(pgwire::ByteCursor& buf, const TableInfo& rel) const {
    buf.get();
    buf.getInt();
    buf.get();
    return {decodeTuple(buf, rel), std::nullopt};
}

Row WalMessageDecoder::decodeTuple(pgwire::ByteCursor& buf, const TableInfo& rel) const {
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
    char b[24];
    std::snprintf(b, sizeof b, "%X/%X", static_cast<unsigned>(lsn >> 32),
                  static_cast<unsigned>(lsn));
    return b;
}

std::string format_timestamp(std::int64_t micros) {
    using namespace std::chrono;
    constexpr seconds kPostgresEpoch{946684800};
    const sys_time<microseconds> t{microseconds{micros} + kPostgresEpoch};
    return std::format("{:%F %T}Z", t);
}

const char* replica_identity_name(char c) {
    switch (c) {
        case 'd': return "default";
        case 'n': return "nothing";
        case 'f': return "full";
        case 'i': return "index";
        default:  return "?";
    }
}

std::string format_row(const Row& row) {
    std::string s = "(";
    s.reserve(row.size() * 16 + 2);
    for (std::size_t i = 0; i < row.size(); ++i) {
        if (i) s += ", ";
        s += row[i].first;
        s += '=';
        switch (row[i].second.kind) {
            case Value::Kind::Text: s += row[i].second.text; break;
            case Value::Kind::UnchangedToast: s += "<unchanged>"; break;
            case Value::Kind::Null: s += "NULL"; break;
        }
    }
    s += ')';
    return s;
}

}

std::string PgoutputParser::relation_label(std::uint32_t oid) const {
    const auto it = relations_.find(oid);
    if (it != relations_.end()) {
        return it->second.schema + "." + it->second.name;
    }
    return "oid=" + std::to_string(oid);
}

pgoutput::ParsedMessage PgoutputParser::handle_message(const char* data,
                                                      std::size_t len) {
    if (len == 0) {
        return {};
    }
    try {
        pgwire::ByteCursor buf(data, len);
        pgoutput::ParsedMessage parsed;
        const char type = decoder_.peekType(buf);
        parsed.type = type;
        switch (type) {
            case 'R': {
                pgoutput::TableInfo t = decoder_.decodeRelation(buf);
                std::printf("RELATION %s.%s oid=%u identity=%s cols=[",
                            t.schema.c_str(), t.name.c_str(), t.oid,
                            replica_identity_name(t.replica_identity));
                for (std::size_t i = 0; i < t.columns.size(); ++i) {
                    std::printf("%s%s%s", i ? ", " : "",
                                t.columns[i].part_of_key ? "*" : "",
                                t.columns[i].name.c_str());
                }
                std::printf("]\n");
                relations_[t.oid] = std::move(t);
                break;
            }
            case 'I':
            case 'U':
            case 'D': {
                static const pgoutput::TableInfo kUnknown;
                const std::uint32_t oid = decoder_.peekOid(buf);
                const auto it = relations_.find(oid);
                const pgoutput::TableInfo& rel =
                    (it != relations_.end()) ? it->second : kUnknown;
                const std::string label = relation_label(oid);

                if (type == 'I') {
                    const DecodedDml dml = decoder_.decodeInsert(buf, rel);
                    std::printf("INSERT   %s %s\n", label.c_str(),
                                format_row(*dml.after).c_str());
                } else if (type == 'U') {
                    const DecodedDml dml = decoder_.decodeUpdate(buf, rel);
                    if (dml.before) {
                        std::printf("UPDATE   %s old=%s new=%s\n", label.c_str(),
                                    format_row(*dml.before).c_str(),
                                    format_row(*dml.after).c_str());
                    } else {
                        std::printf("UPDATE   %s new=%s\n", label.c_str(),
                                    format_row(*dml.after).c_str());
                    }
                } else {
                    const DecodedDml dml = decoder_.decodeDelete(buf, rel);
                    std::printf("DELETE   %s old=%s\n", label.c_str(),
                                format_row(*dml.before).c_str());
                }
                break;
            }
            case 'B': {
                buf.get();
                const auto lsn = static_cast<std::uint64_t>(buf.getLong());
                const std::int64_t ts = buf.getLong();
                const auto xid = static_cast<std::uint32_t>(buf.getInt());
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
                std::string list;
                for (std::int32_t i = 0; i < nrel; ++i) {
                    if (i) list += ", ";
                    list += relation_label(static_cast<std::uint32_t>(buf.getInt()));
                }
                std::printf("TRUNCATE %s\n", list.c_str());
                break;
            }
            case 'O':
                std::printf("ORIGIN   (not decoded)\n");
                break;
            case 'Y':
                std::printf("TYPE     (not decoded)\n");
                break;
            default:
                std::printf("?        message type '%c' (0x%02x)\n", type,
                            static_cast<unsigned char>(type));
                break;
        }
        std::fflush(stdout);
        return parsed;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "parse error: %s\n", e.what());
        return {};
    }
}
