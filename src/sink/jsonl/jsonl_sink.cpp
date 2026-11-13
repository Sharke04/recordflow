#include "sink/jsonl/jsonl_sink.hpp"

#include <nlohmann/json.hpp>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <format>
#include <stdexcept>

namespace recordflow::sink::jsonl {

namespace {

using json = nlohmann::ordered_json;

const char* op_code(core::Op op) {
    switch (op) {
        case core::Op::Insert:   return "c";
        case core::Op::Update:   return "u";
        case core::Op::Delete:   return "d";
        case core::Op::Read:     return "r";
        case core::Op::Truncate: return "t";
        case core::Op::Unknown:  break;
    }
    return "?";
}

// "2026-11-22T18:00:00.123456Z"
std::string format_commit_ts(std::int64_t postgres_micros) {
    const std::chrono::sys_time<std::chrono::microseconds> t{
        std::chrono::microseconds{postgres_micros + core::kPostgresEpochUnixMicros}};
    return std::format("{:%FT%T}Z", t);
}

// Unchanged TOAST values are left out: their value is not in the stream, and
// a consumer applying the row as an upsert keeps what it has. Fields and
// columns keep their order (ordered_json).
json row_json(const std::optional<core::Row>& row) {
    if (!row) return nullptr;
    json object = json::object();
    for (const auto& [name, value] : *row) {
        switch (value.kind) {
            case core::Value::Kind::Text:           object[name] = value.text; break;
            case core::Value::Kind::Null:           object[name] = nullptr; break;
            case core::Value::Kind::UnchangedToast: break;
        }
    }
    return object;
}

}  // namespace

PrepareResult JsonlSink::prepare(const core::TableSchema&) {
    return PrepareResult{};  // no target to create or compare
}

std::string JsonlSink::to_json_line(const core::ChangeEvent& event) {
    json key = nullptr;
    if (!event.key.empty()) {
        key = json::object();
        for (std::size_t i = 0; i < event.key_columns.size() && i < event.key.size(); ++i) {
            key[event.key_columns[i]] = event.key[i];
        }
    }

    json line;
    line["op"] = op_code(event.op);
    line["source"] = event.source == core::EventSource::Log ? "log" : "snapshot";
    line["schema"] = event.schema_name;
    line["table"] = event.table;
    line["key"] = key;
    line["before"] = row_json(event.before);
    line["after"] = row_json(event.after);
    line["version"] = {{"lsn", core::format_lsn(event.version.commit_lsn)}, {"seq", event.version.seq}};
    line["commit_ts"] = format_commit_ts(event.commit_ts);
    line["xid"] = event.xid;
    return line.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
}

std::optional<core::Lsn> JsonlSink::flush(bool include_open) {
    TxBuffer::Drained drained = buffer_.drain(include_open);
    for (const auto& event : drained.events) {
        const std::string line = to_json_line(event) + '\n';
        if (std::fwrite(line.data(), 1, line.size(), out_) != line.size()) {
            throw std::runtime_error(std::format("cannot write JSON Lines output: {}", std::strerror(errno)));
        }
    }
    if (std::fflush(out_) != 0) {
        throw std::runtime_error(std::format("cannot write JSON Lines output: {}", std::strerror(errno)));
    }
    return drained.last_commit_end;
}

}  // namespace recordflow::sink::jsonl
