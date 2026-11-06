#include "config/fields.hpp"

#include <cctype>
#include <format>
#include <limits>
#include <utility>

#include "runtime/errors.hpp"

namespace recordflow::config {

namespace {

template <typename Access>
Field string_field(std::string_view section, std::string_view key, Access access,
                   bool secret = false, bool file_allowed = true) {
    Field field{section, key, FieldKind::String, secret, file_allowed, {}, {}, {}, {}};
    field.set = [access](Config& c, const std::string& text, std::int64_t, Source source) {
        auto& setting = access(c);
        setting.value = text;
        setting.source = source;
    };
    field.show = [access](const Config& c) { return access(c).value; };
    field.source = [access](const Config& c) { return access(c).source; };
    return field;
}

template <typename Access>
Field integer_field(std::string_view section, std::string_view key, Access access) {
    Field field{section, key, FieldKind::Integer, false, true, {}, {}, {}, {}};
    const std::string name = std::format("{}.{}", section, key);
    field.set = [access, name](Config& c, const std::string&, std::int64_t number, Source source) {
        if (number < std::numeric_limits<int>::min() || number > std::numeric_limits<int>::max()) {
            throw ConfigError(std::format("{}: {} is out of range", name, number));
        }
        auto& setting = access(c);
        setting.value = static_cast<int>(number);
        setting.source = source;
    };
    field.show = [access](const Config& c) { return std::to_string(access(c).value); };
    field.source = [access](const Config& c) { return access(c).source; };
    return field;
}

template <typename E, typename Access>
Field enum_field(std::string_view section, std::string_view key, Access access,
                 std::vector<std::pair<std::string_view, E>> names) {
    Field field{section, key, FieldKind::Enum, false, true, {}, {}, {}, {}};
    for (const auto& [text, value] : names) field.choices.push_back(text);
    const std::string name = std::format("{}.{}", section, key);
    field.set = [access, names, name](Config& c, const std::string& text, std::int64_t,
                                      Source source) {
        for (const auto& [candidate, value] : names) {
            if (candidate == text) {
                auto& setting = access(c);
                setting.value = value;
                setting.source = source;
                return;
            }
        }
        std::string allowed;
        for (const auto& [candidate, value] : names) {
            allowed += allowed.empty() ? "" : " | ";
            allowed += candidate;
        }
        throw ConfigError(std::format("{}: invalid value '{}', expected {}", name, text, allowed));
    };
    field.show = [access, names](const Config& c) {
        for (const auto& [candidate, value] : names) {
            if (value == access(c).value) return std::string(candidate);
        }
        return std::string("?");
    };
    field.source = [access](const Config& c) { return access(c).source; };
    return field;
}

#define RF_ACCESS(section, key) [](auto& c) -> auto& { return c.section.key; }

std::vector<Field> make_fields() {
    std::vector<Field> f;

    f.push_back(string_field("source", "conninfo", RF_ACCESS(source, conninfo)));
    f.push_back(string_field("source", "slot", RF_ACCESS(source, slot)));
    f.push_back(string_field("source", "publication", RF_ACCESS(source, publication)));
    f.push_back(string_field("source", "table", RF_ACCESS(source, table)));

    f.push_back(string_field("clickhouse", "host", RF_ACCESS(clickhouse, host)));
    f.push_back(integer_field("clickhouse", "port", RF_ACCESS(clickhouse, port)));
    f.push_back(string_field("clickhouse", "database", RF_ACCESS(clickhouse, database)));
    f.push_back(string_field("clickhouse", "user", RF_ACCESS(clickhouse, user)));
    f.push_back(string_field("clickhouse", "password", RF_ACCESS(clickhouse, password),
                             /*secret=*/true, /*file_allowed=*/false));
    f.push_back(enum_field<Compression>("clickhouse", "compression", RF_ACCESS(clickhouse, compression),
                                        {{"lz4", Compression::Lz4},
                                         {"zstd", Compression::Zstd},
                                         {"none", Compression::None}}));
    f.push_back(string_field("clickhouse", "table", RF_ACCESS(clickhouse, table)));

    f.push_back(string_field("state", "data_dir", RF_ACCESS(state, data_dir)));

    f.push_back(integer_field("batch", "max_rows", RF_ACCESS(batch, max_rows)));
    f.push_back(integer_field("batch", "max_mb", RF_ACCESS(batch, max_mb)));
    f.push_back(integer_field("batch", "max_delay_ms", RF_ACCESS(batch, max_delay_ms)));

    f.push_back(enum_field<SnapshotOnStart>("snapshot", "on_start", RF_ACCESS(snapshot, on_start),
                                            {{"always", SnapshotOnStart::Always},
                                             {"if_incomplete", SnapshotOnStart::IfIncomplete}}));
    f.push_back(integer_field("snapshot", "chunk_size", RF_ACCESS(snapshot, chunk_size)));
    f.push_back(integer_field("snapshot", "chunk_delay_ms", RF_ACCESS(snapshot, chunk_delay_ms)));
    f.push_back(integer_field("snapshot", "window_timeout_s", RF_ACCESS(snapshot, window_timeout_s)));

    f.push_back(enum_field<SinkKind>("output", "sink", RF_ACCESS(output, sink),
                                     {{"clickhouse", SinkKind::ClickHouse},
                                      {"jsonl", SinkKind::Jsonl}}));

    f.push_back(enum_field<log::Level>("log", "level", RF_ACCESS(log, level),
                                       {{"debug", log::Level::Debug},
                                        {"info", log::Level::Info},
                                        {"warn", log::Level::Warn},
                                        {"error", log::Level::Error}}));

    f.push_back(integer_field("retry", "initial_ms", RF_ACCESS(retry, initial_ms)));
    f.push_back(integer_field("retry", "max_ms", RF_ACCESS(retry, max_ms)));
    f.push_back(integer_field("retry", "slot_busy_timeout_s", RF_ACCESS(retry, slot_busy_timeout_s)));

    f.push_back(integer_field("shutdown", "timeout_s", RF_ACCESS(shutdown, timeout_s)));

    return f;
}

#undef RF_ACCESS

std::string upper(std::string_view text) {
    std::string out(text);
    for (char& ch : out) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    return out;
}

}  // namespace

std::string Field::name() const {
    return std::format("{}.{}", section, key);
}

std::string Field::env_name() const {
    return std::format("RECORDFLOW_{}_{}", upper(section), upper(key));
}

std::string Field::allowed_values() const {
    std::string out;
    for (const auto& choice : choices) {
        out += out.empty() ? "" : " | ";
        out += choice;
    }
    return out;
}

const std::vector<Field>& fields() {
    static const std::vector<Field> all = make_fields();
    return all;
}

const Field* find_field(std::string_view section, std::string_view key) {
    for (const Field& field : fields()) {
        if (field.section == section && field.key == key) return &field;
    }
    return nullptr;
}

bool is_section(std::string_view section) {
    for (const Field& field : fields()) {
        if (field.section == section) return true;
    }
    return false;
}

const char* source_name(Source source) {
    switch (source) {
        case Source::Default: return "default";
        case Source::File:    return "file";
        case Source::Env:     return "env";
        case Source::Cli:     return "cli";
    }
    return "?";
}

}  // namespace recordflow::config
