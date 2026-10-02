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

// Keys are routed by prefix: `kafka.` and `sink.` are recordflow's own
// namespaces, everything else is a libpq connection keyword.
constexpr std::string_view kKafkaPrefix = "kafka.";
constexpr std::string_view kSinkPrefix = "sink.";

bool strip_prefix(std::string_view& s, std::string_view prefix) {
    if (s.substr(0, prefix.size()) != prefix) {
        return false;
    }
    s.remove_prefix(prefix.size());
    return true;
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

        std::string_view key = trim(trimmed.substr(0, eq));
        if (key.empty()) {
            fail(path, line_no, "empty key");
        }
        const std::string_view value = trim(trimmed.substr(eq + 1));

        if (strip_prefix(key, kKafkaPrefix)) {
            if (key.empty()) {
                fail(path, line_no, "expected a property name after 'kafka.'");
            }
            config.sink.properties.emplace_back(key, value);
        } else if (strip_prefix(key, kSinkPrefix)) {
            if (key == "topic") {
                config.sink.topic = value;
            } else {
                fail(path, line_no, "unknown sink setting 'sink." +
                                        std::string(key) + "'");
            }
        } else {
            config.source.params.emplace_back(key, value);
        }
    }

    if (config.source.params.empty()) {
        throw std::runtime_error("no connection parameters in config file: " +
                                 path);
    }
    if (config.sink.topic.empty()) {
        throw std::runtime_error("sink.topic is required in config file: " +
                                 path);
    }

    return config;
}
