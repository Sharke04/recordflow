#pragma once

#include <string>
#include <string_view>

#include "config/config.hpp"

namespace recordflow::config {

// The effective configuration in TOML form, one key per line with the layer it
// came from. Secrets and passwords inside the conninfo are replaced by ****.
std::string describe(const Config& config);

// Replaces the password in a libpq conninfo (key=value or URI form) by ****.
std::string mask_conninfo(std::string_view conninfo);

}  // namespace recordflow::config
