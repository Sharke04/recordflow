#include "runtime/log.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>

namespace recordflow::log {

namespace {

std::atomic<Level> g_level{Level::Info};

const char* level_name(Level level) {
    switch (level) {
        case Level::Debug: return "DEBUG";
        case Level::Info:  return "INFO ";
        case Level::Warn:  return "WARN ";
        case Level::Error: return "ERROR";
    }
    return "?    ";
}

std::string_view trim_trailing_whitespace(std::string_view text) {
    while (!text.empty() && (text.back() == '\n' || text.back() == '\r' || text.back() == ' ')) {
        text.remove_suffix(1);
    }
    return text;
}

}  // namespace

void set_level(Level level) {
    g_level.store(level);
}

bool enabled(Level level) {
    return level >= g_level.load();
}

void write(Level level, std::string_view message) {
    const auto now = std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now());
    const std::string line = std::format("{:%Y-%m-%dT%H:%M:%S}Z {} {}\n", now, level_name(level),
                                         trim_trailing_whitespace(message));
    std::fputs(line.c_str(), stderr);
}

}  // namespace recordflow::log
