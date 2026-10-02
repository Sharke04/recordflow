#include "stream/publisher/change_event.hpp"

#include <chrono>
#include <cstdio>
#include <format>
#include <string_view>

namespace pgoutput {

namespace {

void append_escaped(std::string& out, std::string_view s) {
    out += '"';
    for (const char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            case '\b': out += "\\b"; break;
            case '\f': out += "\\f"; break;
            default:
                // Bytes above 0x1f are emitted as-is, which keeps multi-byte
                // UTF-8 sequences intact. This is why the replication
                // connection pins client_encoding to UTF8.
                if (static_cast<unsigned char>(c) < 0x20) {
                    char esc[7];
                    std::snprintf(esc, sizeof esc, "\\u%04x",
                                  static_cast<unsigned char>(c));
                    out += esc;
                } else {
                    out += c;
                }
        }
    }
    out += '"';
}

const char* op_code(Op op) {
    switch (op) {
        case Op::Insert:   return "c";
        case Op::Update:   return "u";
        case Op::Delete:   return "d";
        case Op::Truncate: return "t";
    }
    return "?";
}

std::string format_lsn(std::uint64_t lsn) {
    char buf[24];
    std::snprintf(buf, sizeof buf, "%X/%X", static_cast<unsigned>(lsn >> 32),
                  static_cast<unsigned>(lsn));
    return buf;
}

std::string format_timestamp(std::int64_t micros) {
    using namespace std::chrono;
    constexpr seconds kPostgresEpoch{946684800};
    const sys_time<microseconds> t{microseconds{micros} + kPostgresEpoch};
    return std::format("{:%FT%T}Z", t);
}

// Unchanged-TOAST columns are omitted rather than written as null, and their
// names are reported separately, so a consumer can tell "this column was not
// sent" from "this column is NULL".
void append_row(std::string& out, const Row& row) {
    out += '{';
    bool first = true;
    for (const auto& [name, value] : row) {
        if (value.kind == Value::Kind::UnchangedToast) {
            continue;
        }
        if (!first) {
            out += ',';
        }
        first = false;
        append_escaped(out, name);
        out += ':';
        if (value.kind == Value::Kind::Null) {
            out += "null";
        } else {
            append_escaped(out, value.text);
        }
    }
    out += '}';
}

bool append_unchanged(std::string& out, std::string_view field, const Row& row) {
    bool any = false;
    for (const auto& [name, value] : row) {
        if (value.kind != Value::Kind::UnchangedToast) {
            continue;
        }
        if (!any) {
            out += ",\"";
            out += field;
            out += "\":[";
            any = true;
        } else {
            out += ',';
        }
        append_escaped(out, name);
    }
    if (any) {
        out += ']';
    }
    return any;
}

}

std::string ChangeEvent::key() const {
    std::string key = schema.empty() ? table : schema + "." + table;

    const Row* row = after ? &*after : (before ? &*before : nullptr);
    if (row == nullptr || key_columns.empty()) {
        return key;
    }

    key += ':';
    bool first = true;
    for (const auto& column : key_columns) {
        if (!first) {
            key += '|';
        }
        first = false;
        for (const auto& [name, value] : *row) {
            if (name == column) {
                key += value.text;
                break;
            }
        }
    }
    return key;
}

std::string ChangeEvent::json() const {
    std::string out;
    out.reserve(256);

    out += "{\"op\":\"";
    out += op_code(op);
    out += "\",\"source\":{\"schema\":";
    append_escaped(out, schema);
    out += ",\"table\":";
    append_escaped(out, table);
    out += ",\"lsn\":\"";
    out += format_lsn(lsn);
    out += "\",\"xid\":";
    out += std::to_string(xid);
    out += ",\"ts\":\"";
    out += format_timestamp(commit_ts);
    out += "\"}";

    if (before) {
        out += ",\"before\":";
        append_row(out, *before);
    }
    if (after) {
        out += ",\"after\":";
        append_row(out, *after);
    }
    if (before) {
        append_unchanged(out, "unchanged_before", *before);
    }
    if (after) {
        append_unchanged(out, "unchanged_after", *after);
    }

    out += '}';
    return out;
}

}
