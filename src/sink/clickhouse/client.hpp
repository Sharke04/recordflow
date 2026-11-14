#pragma once

#include <clickhouse/block.h>
#include <clickhouse/client.h>

#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include "config/config.hpp"

// The one ClickHouse connection of the sink (decision 3), with library errors
// translated into the classes of decision 8.1.
namespace recordflow::sink::clickhouse {

// A server error that retrying will not fix (decision 8.1: unexpected, exit 1
// unless the caller handles the code, e.g. UNKNOWN_DATABASE at startup).
class ServerError : public std::runtime_error {
public:
    ServerError(int code, const std::string& what) : std::runtime_error(what), code_(code) {}
    int code() const { return code_; }

private:
    int code_;
};

// Server error codes that mean "try again later" (decision 8.1); everything
// else is unexpected. The single place where this list lives.
bool is_transient_code(int code);

::clickhouse::ClientOptions client_options(const config::ClickHouseSection& section);

class Client {
public:
    // Does not connect; the first call does (and so do calls after ResetConnection).
    explicit Client(::clickhouse::ClientOptions options);

    // Each call throws TransientError (network errors, transient server codes),
    // ServerError (other server errors) or std::runtime_error.
    void Execute(const std::string& sql);
    void Select(const std::string& sql, const std::function<void(const ::clickhouse::Block&)>& on_block);
    void Insert(const std::string& table, const ::clickhouse::Block& block);

    // Drops the connection, e.g. after a TransientError; the next call reconnects.
    void ResetConnection();

private:
    template <typename Call>
    void call(std::string_view what, Call&& body);

    ::clickhouse::ClientOptions options_;
    std::unique_ptr<::clickhouse::Client> client_;
};

}  // namespace recordflow::sink::clickhouse
