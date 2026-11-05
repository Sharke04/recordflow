#pragma once

#include <chrono>

#include "core/change_event.hpp"
#include "pgoutput/decoder.hpp"

namespace pgoutput {

// Postgres timestamps count microseconds since 2000-01-01 00:00:00 UTC
inline constexpr std::chrono::sys_days kPostgresEpoch {
    std::chrono::year{2000} / std::chrono::January / 1
};

}

void print_change_event(const pgoutput::ChangeEvent& event);
void print_relation(const pgoutput::TableInfo& table);
