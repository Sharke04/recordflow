#pragma once

#include <string>
#include <utility>
#include <vector>

namespace recordflow::pg {

using KeyValues = std::vector<std::pair<std::string, std::string>>;

enum class ConnectionKind { Replication, Query };

// Session settings both connections must share, so that text values from
// pgoutput and from chunk SELECTs are formatted identically (decision 2.4).
inline const std::vector<std::pair<std::string, std::string>>& required_gucs() {
    static const std::vector<std::pair<std::string, std::string>> gucs{
        {"TimeZone", "UTC"},
        {"DateStyle", "ISO"},
        {"extra_float_digits", "1"},
        {"bytea_output", "hex"},
    };
    return gucs;
}

// Turns the user's libpq conninfo (key=value or URI) into connection
// parameters for one of the two connections (decisions 6.2 and 8.2):
// adds `replication=database` for the replication connection, appends the
// required GUCs to `options`, and defaults client_encoding to UTF8 and
// application_name to "recordflow". Throws ConfigError if the conninfo cannot
// be parsed, sets `replication`, overrides a required GUC or asks for a
// client_encoding other than UTF8.
KeyValues build_connection_params(const std::string& conninfo, ConnectionKind kind);

}  // namespace recordflow::pg
