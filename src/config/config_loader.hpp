#pragma once

#include <string>
#include <string_view>

#include "config/config.hpp"

namespace recordflow::config {

// Applies a TOML document on top of `config` (values become Source::File).
// Unknown sections or keys, wrong types and keys that must not appear in a
// file throw ConfigError as "<origin>:<line>: <message>".
void apply_toml(Config& config, std::string_view text, const std::string& origin);

// Reads and applies a TOML file. A file that cannot be read is a ConfigError.
void apply_toml_file(Config& config, const std::string& path);

}  // namespace recordflow::config
