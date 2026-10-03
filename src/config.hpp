#pragma once

#include <string>
#include <utility>
#include <vector>

using KeyValues = std::vector<std::pair<std::string, std::string>>;

struct Config {
    KeyValues params;
};

Config load_config(const std::string& path = "recordflow.conf");
