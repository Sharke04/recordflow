#pragma once

#include <functional>
#include <optional>
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

// Returns the value of an environment variable, or nothing if it is not set.
using EnvLookup = std::function<std::optional<std::string>(const std::string& name)>;

EnvLookup process_environment();

// Applies RECORDFLOW_<SECTION>_<KEY> and RECORDFLOW_<SECTION>_<KEY>_FILE
// (value read from that file, trailing newline removed) for every key.
// Values become Source::Env. Setting both variants of one key is an error.
void apply_env(Config& config, const EnvLookup& env);

struct LoadOptions {
    // --config: when set, the file must exist.
    std::optional<std::string> config_path;
    // Used when config_path is not set, but only if the file exists; without
    // it, the configuration comes from the environment alone.
    std::string default_path = "recordflow.toml";
    EnvLookup env = process_environment();
};

// Defaults, then the TOML file, then the environment.
Config load(const LoadOptions& options);

}  // namespace recordflow::config
