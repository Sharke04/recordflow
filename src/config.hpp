#pragma once

#include <string>
#include <utility>
#include <vector>

using KeyValues = std::vector<std::pair<std::string, std::string>>;

struct SourceConfig {
    KeyValues params;
};

struct Config {
    SourceConfig source;
};

Config load_config(const std::string& path = "recordflow.conf");
