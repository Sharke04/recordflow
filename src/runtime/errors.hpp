#pragma once

#include <stdexcept>
#include <string>
#include <utility>

// Error classes from decision 8.1. Adapters translate library errors into these
// types; the engine retries TransientError and main maps the rest to exit codes.
namespace recordflow {

enum class ExitCode : int {
    Ok = 0,            // normal end (graceful shutdown)
    Unexpected = 1,    // unexpected error
    Config = 2,        // configuration or usage error
    SchemaChange = 3,  // source schema change, intervention required
    Intervention = 4,  // slot, publication, privileges, identity or state file
};

enum class Resource { PostgresReplication, PostgresQuery, ClickHouse };

class TransientError : public std::runtime_error {
public:
    TransientError(Resource resource, const std::string& what)
        : std::runtime_error(what), resource_(resource) {}
    Resource resource() const { return resource_; }

private:
    Resource resource_;
};

class InterventionError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

class SchemaChangeError : public std::runtime_error {
public:
    SchemaChangeError(const std::string& what, std::string diff)
        : std::runtime_error(what), diff_(std::move(diff)) {}
    const std::string& diff() const { return diff_; }

private:
    std::string diff_;
};

class ConfigError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

inline ExitCode exit_code_for(const std::exception& error) {
    if (dynamic_cast<const ConfigError*>(&error)) return ExitCode::Config;
    if (dynamic_cast<const SchemaChangeError*>(&error)) return ExitCode::SchemaChange;
    if (dynamic_cast<const InterventionError*>(&error)) return ExitCode::Intervention;
    return ExitCode::Unexpected;
}

}  // namespace recordflow
