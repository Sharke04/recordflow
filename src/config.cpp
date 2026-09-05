#include "config.hpp"

#include <fstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

std::string trim(const std::string& s) {
    const auto first = s.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }
    const auto last = s.find_last_not_of(" \t\r\n");
    return s.substr(first, last - first + 1);
}

}

SourceConfig load_config(const std::string& path) {
    std::ifstream file(path);
    if (!file) {
        throw std::runtime_error("cannot open config file: " + path);
    }

    SourceConfig config;
    std::string line;
    while (std::getline(file, line)) {
        const std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') {
            continue;
        }

        const auto eq = trimmed.find('=');
        if (eq == std::string::npos) {
            throw std::runtime_error(
                "malformed config line (expected key=value): " + line);
        }

        std::string key = trim(trimmed.substr(0, eq));
        std::string value = trim(trimmed.substr(eq + 1));
        if (key.empty()) {
            throw std::runtime_error("malformed config line (empty key): " + line);
        }

        config.params.emplace_back(std::move(key), std::move(value));
    }

    return config;
}
