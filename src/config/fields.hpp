#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

#include "config/config.hpp"

// One entry per configuration key. The TOML loader, the environment overrides
// and the effective-config printout all work from this list, so a key exists
// in every layer or in none.
namespace recordflow::config {

enum class FieldKind { String, Integer, Enum };

struct Field {
    std::string_view section;
    std::string_view key;
    FieldKind kind;
    bool secret = false;        // masked in every printout
    bool file_allowed = true;   // false: only from the environment

    // Sets the value: `text` for String/Enum fields, `number` for Integer
    // fields. Throws ConfigError (without location) for an invalid value.
    std::function<void(Config&, const std::string& text, std::int64_t number, Source)> set;
    std::function<std::string(const Config&)> show;
    std::function<Source(const Config&)> source;

    std::string name() const;            // "section.key"
    std::string env_name() const;        // "RECORDFLOW_SECTION_KEY"
    std::string allowed_values() const;  // "always | if_incomplete" for enums
    std::vector<std::string_view> choices;
};

const std::vector<Field>& fields();
const Field* find_field(std::string_view section, std::string_view key);
bool is_section(std::string_view section);

const char* source_name(Source source);

}  // namespace recordflow::config
