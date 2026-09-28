#include "stream/printer/change_event_printer.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <format>
#include <optional>
#include <string>
#include <vector>

namespace {

using namespace pgoutput;

const char* op_name(Op op) {
    switch (op) {
        case Op::Unknown:  return "?";
        case Op::Insert:   return "INSERT";
        case Op::Update:   return "UPDATE";
        case Op::Delete:   return "DELETE";
        case Op::Truncate: return "TRUNCATE";
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

std::string format_lsn(std::uint64_t lsn) {
    const auto high = static_cast<std::uint32_t>(lsn >> 32);
    const auto low = static_cast<std::uint32_t>(lsn);
    return std::format("{:X}/{:X}", high, low);
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

void print_change_event(const pgoutput::ChangeEvent& event) {
    const std::string table =
        event.schema.empty() ? event.table : event.schema + "." + event.table;

    std::printf("ChangeEvent\n");
    std::printf("  op          = %s\n", op_name(event.op));
    std::printf("  table       = %s\n", table.c_str());
    std::printf("  key_columns = %s\n", format_key_columns(event.key_columns).c_str());
    std::printf("  xid         = %u\n", event.xid);
    std::printf("  lsn         = %s\n", format_lsn(event.lsn).c_str());
    std::printf("  commit_ts   = %s\n", format_timestamp(event.commit_ts).c_str());
    std::printf("  before      = %s\n", format_row(event.before).c_str());
    std::printf("  after       = %s\n", format_row(event.after).c_str());
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
