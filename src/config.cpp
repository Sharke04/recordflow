#include "config.hpp"

#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace {

constexpr std::string_view kWhitespace = " \t\r\n";

std::string_view trim(std::string_view s) {
    const auto first = s.find_first_not_of(kWhitespace);
    if (first == std::string_view::npos) {
        return {};
    }
    return s.substr(first, s.find_last_not_of(kWhitespace) - first + 1);
}

void fail(const std::string& path, int line_no, const std::string& what) {
    throw std::runtime_error(path + ":" + std::to_string(line_no) + ": " + what);
}

}

Config load_config(const std::string& path) {
    std::ifstream file(path);
    if (!file) {
        throw std::runtime_error("cannot open config file: " + path);
    }

    Config config;
    std::string line;
    for (int line_no = 1; std::getline(file, line); ++line_no) {
        const std::string_view trimmed = trim(line);
        if (trimmed.empty() || trimmed.front() == '#') {
            continue;
        }

        const auto eq = trimmed.find('=');
        if (eq == std::string_view::npos) {
            fail(path, line_no, "expected key=value");
        }

        const std::string_view key = trim(trimmed.substr(0, eq));
        if (key.empty()) {
            fail(path, line_no, "empty key");
        }
        const std::string_view value = trim(trimmed.substr(eq + 1));
        config.params.emplace_back(key, value);
    }

    if (config.params.empty()) {
        throw std::runtime_error("no connection parameters in config file: " +
                                 path);
    }

    return config;
}
