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
        case Op::Insert:   return "INSERT";
        case Op::Update:   return "UPDATE";
        case Op::Delete:   return "DELETE";
        case Op::Truncate: return "TRUNCATE";
    }
    return "?";
}

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

std::string format_row(const std::optional<Row>& row) {
    if (!row) {
        return "none";
    }
    std::string s = "(";
    const char* sep = "";
    for (const auto& [name, value] : *row) {
        s += sep;
        s += name;
        s += '=';
        switch (value.kind) {
            case Value::Kind::Text: s += value.text; break;
            case Value::Kind::UnchangedToast: s += "<unchanged>"; break;
            case Value::Kind::Null: s += "NULL"; break;
        }
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
