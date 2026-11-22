#pragma once

#include <format>
#include <string_view>
#include <utility>

// Minimal logger: one line per message on stderr, "<UTC time> <LEVEL> <message>".
// Deliberately small so it can be swapped for another backend later.
namespace recordflow::log {

enum class Level { Debug, Info, Warn, Error };

void set_level(Level level);
bool enabled(Level level);
void write(Level level, std::string_view message);

template <typename... Args>
void debug(std::format_string<Args...> fmt, Args&&... args) {
    if (enabled(Level::Debug)) write(Level::Debug, std::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
void info(std::format_string<Args...> fmt, Args&&... args) {
    if (enabled(Level::Info)) write(Level::Info, std::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
void warn(std::format_string<Args...> fmt, Args&&... args) {
    if (enabled(Level::Warn)) write(Level::Warn, std::format(fmt, std::forward<Args>(args)...));
}

template <typename... Args>
void error(std::format_string<Args...> fmt, Args&&... args) {
    if (enabled(Level::Error)) write(Level::Error, std::format(fmt, std::forward<Args>(args)...));
}

}  // namespace recordflow::log
