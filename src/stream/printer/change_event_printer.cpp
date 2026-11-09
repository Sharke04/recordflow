#include "stream/printer/change_event_printer.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <format>
#include <optional>
#include <string>
#include <vector>

namespace recordflow {

namespace {

using namespace core;
using pgoutput::kPostgresEpoch;

const char* op_name(Op op) {
    switch (op) {
        case Op::Unknown:  return "?";
        case Op::Insert:   return "INSERT";
        case Op::Update:   return "UPDATE";
        case Op::Delete:   return "DELETE";
        case Op::Truncate: return "TRUNCATE";
        case Op::Read:     return "READ";
    }
    return "?";
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

std::string format_timestamp(std::int64_t micros) {
    const auto t = kPostgresEpoch + std::chrono::microseconds{micros};
    return std::format("{:%F %T}Z", t);
}

std::string format_key_columns(const std::vector<std::string>& columns) {
    std::string s = "[";
    const char* sep = "";
    for (const auto& column : columns) {
        s += sep;
        s += column;
        sep = ", ";
    }
    s += ']';
    return s;
}

std::string format_value(const Value& value) {
    switch (value.kind) {
        case Value::Kind::Text:           return value.text;
        case Value::Kind::UnchangedToast: return "<unchanged>";
        case Value::Kind::Null:           return "NULL";
    }
    return {};
}

std::string format_row(const std::optional<Row>& row) {
    if (!row)
        return "none";

    std::string s = "(";
    const char* sep = "";
    for (const auto& [name, value] : *row) {
        s += sep;
        s += name;
        s += '=';
        s += format_value(value);
        sep = ", ";
    }
    s += ')';
    return s;
}

}

void print_change_event(const core::ChangeEvent& event) {
    const std::string table =
        event.schema_name.empty() ? event.table : event.schema_name + "." + event.table;

    std::printf("ChangeEvent\n");
    std::printf("  op          = %s\n", op_name(event.op));
    std::printf("  table       = %s\n", table.c_str());
    std::printf("  source      = %s\n", event.source == core::EventSource::Log ? "log" : "snapshot");
    std::printf("  key_columns = %s\n", format_key_columns(event.key_columns).c_str());
    std::printf("  key         = %s\n", format_key_columns(event.key).c_str());
    std::printf("  xid         = %u\n", event.xid);
    std::printf("  version     = %s\n", event.version.str().c_str());
    std::printf("  commit_ts   = %s\n", format_timestamp(event.commit_ts).c_str());
    std::printf("  before      = %s\n", format_row(event.before).c_str());
    std::printf("  after       = %s\n", format_row(event.after).c_str());
    std::fflush(stdout);
}

void print_begin(const pgoutput::Begin& begin) {
    std::printf("BEGIN    xid=%u lsn=%s ts=%s\n", begin.xid, core::format_lsn(begin.final_lsn).c_str(),
                format_timestamp(begin.commit_ts).c_str());
    std::fflush(stdout);
}

void print_commit(const pgoutput::Commit& commit) {
    std::printf("COMMIT   lsn=%s end=%s ts=%s\n", core::format_lsn(commit.commit_lsn).c_str(),
                core::format_lsn(commit.end_lsn).c_str(), format_timestamp(commit.commit_ts).c_str());
    std::fflush(stdout);
}

void print_other(const pgoutput::Other& other) {
    switch (other.type) {
        case 'O':
            std::printf("ORIGIN   (not decoded)\n");
            break;
        case 'Y':
            std::printf("TYPE     (not decoded)\n");
            break;
        default:
            std::printf("?        message type '%c' (0x%02x)\n", other.type,
                        static_cast<unsigned char>(other.type));
            break;
    }
    std::fflush(stdout);
}

void print_relation(const pgoutput::TableInfo& table) {
    std::printf("RELATION %s.%s oid=%u identity=%s cols=[",
                table.schema.c_str(), table.name.c_str(), table.oid,
                replica_identity_name(table.replica_identity));
    for (std::size_t i = 0; i < table.columns.size(); ++i) {
        std::printf("%s%s%s", i ? ", " : "",
                    table.columns[i].part_of_key ? "*" : "",
                    table.columns[i].name.c_str());
    }
    std::printf("]\n");
    std::fflush(stdout);
}

}  // namespace recordflow
