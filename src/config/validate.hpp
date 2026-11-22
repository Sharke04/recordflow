#pragma once

#include "config/config.hpp"

namespace recordflow::config {

// Checks required keys, ranges and formats (decision 6.5). Throws one
// ConfigError that lists every problem found.
void validate(const Config& config);

}  // namespace recordflow::config
