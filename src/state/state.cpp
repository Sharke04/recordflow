#include "state/state.hpp"

#include <nlohmann/json.hpp>

#include <format>

namespace recordflow::state {

namespace {

using json = nlohmann::ordered_json;

json lsn_json(const std::optional<core::Lsn>& lsn) {
    if (!lsn) return nullptr;
    return core::format_lsn(*lsn);
}

const json& field(const json& object, const char* name, const char* where) {
    const auto it = object.find(name);
    if (it == object.end()) throw StateFormatError(std::format("missing field \"{}{}\"", where, name));
    return *it;
}

std::string string_field(const json& object, const char* name, const char* where) {
    const json& value = field(object, name, where);
    if (!value.is_string()) throw StateFormatError(std::format("field \"{}{}\" must be a string", where, name));
    return value.get<std::string>();
}

std::optional<core::Lsn> lsn_field(const json& object, const char* name, const char* where, bool nullable) {
    const json& value = field(object, name, where);
    if (value.is_null() && nullable) return std::nullopt;
    if (!value.is_string()) throw StateFormatError(std::format("field \"{}{}\" must be an LSN", where, name));
    const auto lsn = core::parse_lsn(value.get<std::string>());
    if (!lsn) throw StateFormatError(std::format("field \"{}{}\" is not an LSN: {}", where, name, value.dump()));
    return lsn;
}

std::vector<std::string> key_field(const json& value, const char* name) {
    if (!value.is_array()) throw StateFormatError(std::format("field \"snapshot.{}\" must be an array", name));
    std::vector<std::string> key;
    for (const json& part : value) {
        if (!part.is_string()) {
            throw StateFormatError(std::format("field \"snapshot.{}\" must hold strings", name));
        }
        key.push_back(part.get<std::string>());
    }
    return key;
}

SnapshotStatus parse_status(const std::string& text) {
    if (text == "pending") return SnapshotStatus::Pending;
    if (text == "running") return SnapshotStatus::Running;
    if (text == "done") return SnapshotStatus::Done;
    throw StateFormatError(std::format("unknown snapshot.status \"{}\"", text));
}

}  // namespace

std::string_view to_string(SnapshotStatus status) {
    switch (status) {
        case SnapshotStatus::Pending: return "pending";
        case SnapshotStatus::Running: return "running";
        case SnapshotStatus::Done:    return "done";
    }
    return "?";
}

std::string to_json(const State& state) {
    const SnapshotProgress& snapshot = state.snapshot;
    json doc = {
        {"format_version", state.format_version},
        {"source", {{"system_id", state.source.system_id},
                    {"slot", state.source.slot},
                    {"table", state.source.table}}},
        {"target", {{"table", state.target.table}, {"table_uuid", state.target.table_uuid}}},
        {"last_flushed_lsn", core::format_lsn(state.last_flushed_lsn)},
        {"snapshot", {{"status", std::string(to_string(snapshot.status))},
                      {"start_lsn", lsn_json(snapshot.start_lsn)},
                      {"max_pk", snapshot.max_pk ? json(*snapshot.max_pk) : json(nullptr)},
                      {"last_pk", snapshot.last_pk},
                      {"version_lsn", lsn_json(snapshot.version_lsn)}}},
    };
    return doc.dump(2) + "\n";
}

State from_json(std::string_view text) {
    const json doc = json::parse(text, nullptr, /*allow_exceptions=*/false);
    if (doc.is_discarded()) throw StateFormatError("not valid JSON");
    if (!doc.is_object()) throw StateFormatError("not a JSON object");

    State state;
    const json& version = field(doc, "format_version", "");
    if (!version.is_number_integer() || version.get<int>() != kFormatVersion) {
        throw StateFormatError(std::format("unknown format_version {} (this build reads {})", version.dump(),
                                           kFormatVersion));
    }
    state.format_version = kFormatVersion;

    const json& source = field(doc, "source", "");
    if (!source.is_object()) throw StateFormatError("field \"source\" must be an object");
    state.source.system_id = string_field(source, "system_id", "source.");
    state.source.slot = string_field(source, "slot", "source.");
    state.source.table = string_field(source, "table", "source.");

    const json& target = field(doc, "target", "");
    if (!target.is_object()) throw StateFormatError("field \"target\" must be an object");
    state.target.table = string_field(target, "table", "target.");
    state.target.table_uuid = string_field(target, "table_uuid", "target.");

    state.last_flushed_lsn = *lsn_field(doc, "last_flushed_lsn", "", /*nullable=*/false);

    const json& snapshot = field(doc, "snapshot", "");
    if (!snapshot.is_object()) throw StateFormatError("field \"snapshot\" must be an object");
    state.snapshot.status = parse_status(string_field(snapshot, "status", "snapshot."));
    state.snapshot.start_lsn = lsn_field(snapshot, "start_lsn", "snapshot.", /*nullable=*/true);
    const json& max_pk = field(snapshot, "max_pk", "snapshot.");
    if (!max_pk.is_null()) state.snapshot.max_pk = key_field(max_pk, "max_pk");
    state.snapshot.last_pk = key_field(field(snapshot, "last_pk", "snapshot."), "last_pk");
    state.snapshot.version_lsn = lsn_field(snapshot, "version_lsn", "snapshot.", /*nullable=*/true);
    return state;
}

}  // namespace recordflow::state
