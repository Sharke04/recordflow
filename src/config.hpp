#pragma once

#include <string>
#include <utility>
#include <vector>

struct SourceConfig {
    std::vector<std::pair<std::string, std::string>> params;
};

SourceConfig load_config(const std::string& path = "recordflow.conf");
