#include "config/config_loader.hpp"

#include <toml++/toml.hpp>

#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <sstream>

#include "config/fields.hpp"
#include "config/validate.hpp"
#include "runtime/errors.hpp"

namespace recordflow::config {

namespace {

[[noreturn]] void fail_at(const std::string& origin, const toml::source_region& where,
                          const std::string& message) {
    throw ConfigError(std::format("{}:{}: {}", origin, where.begin.line, message));
}

void apply_value(Config& config, const Field& field, const toml::node& node,
                 const std::string& origin) {
    try {
        if (field.kind == FieldKind::Integer) {
            const auto* integer = node.as_integer();
            if (integer == nullptr) fail_at(origin, node.source(), field.name() + ": expected an integer");
            field.set(config, {}, integer->get(), Source::File);
        } else {
            const auto* text = node.as_string();
            if (text == nullptr) fail_at(origin, node.source(), field.name() + ": expected a string");
            field.set(config, text->get(), 0, Source::File);
        }
    } catch (const ConfigError& e) {
        const std::string message = e.what();
        if (message.starts_with(origin + ":")) throw;
        fail_at(origin, node.source(), message);
    }
}

}  // namespace

void apply_toml(Config& config, std::string_view text, const std::string& origin) {
    toml::table document;
    try {
        document = toml::parse(text, origin);
    } catch (const toml::parse_error& e) {
        fail_at(origin, e.source(), std::string(e.description()));
    }

    for (auto&& [section_key, section_node] : document) {
        const std::string section(section_key.str());
        const auto* table = section_node.as_table();
        if (table == nullptr) {
            fail_at(origin, section_key.source(),
                    std::format("'{}' is not a section; keys belong in sections such as [source]", section));
        }
        if (!is_section(section)) {
            fail_at(origin, section_key.source(), std::format("unknown section [{}]", section));
        }

        for (auto&& [key, node] : *table) {
            const Field* field = find_field(section, key.str());
            if (field == nullptr) {
                fail_at(origin, key.source(), std::format("unknown key '{}.{}'", section, key.str()));
            }
            if (!field->file_allowed) {
                fail_at(origin, key.source(),
                        std::format("'{}' must not be stored in the file; set {} or {}_FILE instead",
                                    field->name(), field->env_name(), field->env_name()));
            }
            apply_value(config, *field, node, origin);
        }
    }
}

void apply_toml_file(Config& config, const std::string& path) {
    std::ifstream file(path);
    if (!file) throw ConfigError("cannot open config file: " + path);
    std::ostringstream text;
    text << file.rdbuf();
    apply_toml(config, text.str(), path);
}

EnvLookup process_environment() {
    return [](const std::string& name) -> std::optional<std::string> {
        if (const char* value = std::getenv(name.c_str())) return std::string(value);
        return std::nullopt;
    };
}

void apply_env(Config& config, const EnvLookup& env) {
    for (const Field& field : fields()) {
        const std::string name = field.env_name();
        const std::string file_name = name + "_FILE";
        std::optional<std::string> value = env(name);
        const std::optional<std::string> file = env(file_name);

        if (value && file) {
            throw ConfigError(std::format("set either {} or {}, not both", name, file_name));
        }
        std::string origin = name;
        if (file) {
            std::ifstream in(*file, std::ios::binary);
            if (!in) throw ConfigError(std::format("{}: cannot read {}", file_name, *file));
            std::ostringstream text;
            text << in.rdbuf();
            value = text.str();
            if (value->ends_with("\r\n")) {
                value->resize(value->size() - 2);
            } else if (value->ends_with('\n')) {
                value->pop_back();
            }
            origin = file_name;
        }
        if (!value) continue;

        try {
            if (field.kind == FieldKind::Integer) {
                std::int64_t number = 0;
                const char* begin = value->data();
                const char* end = begin + value->size();
                const auto [ptr, ec] = std::from_chars(begin, end, number);
                if (ec != std::errc() || ptr != end || value->empty()) {
                    throw ConfigError(std::format("{}: expected an integer, got '{}'", field.name(), *value));
                }
                field.set(config, {}, number, Source::Env);
            } else {
                field.set(config, *value, 0, Source::Env);
            }
        } catch (const ConfigError& e) {
            throw ConfigError(std::format("{}: {}", origin, e.what()));
        }
    }
}

Config load(const LoadOptions& options) {
    Config config;
    if (options.config_path) {
        apply_toml_file(config, *options.config_path);
    } else if (std::filesystem::exists(options.default_path)) {
        apply_toml_file(config, options.default_path);
    }
    apply_env(config, options.env);
    validate(config);
    return config;
}

}  // namespace recordflow::config
