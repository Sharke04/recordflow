#include "sink/clickhouse/client.hpp"

#include <clickhouse/error_codes.h>
#include <clickhouse/exceptions.h>

#include <chrono>
#include <format>
#include <system_error>
#include <utility>

#include "runtime/errors.hpp"

namespace recordflow::sink::clickhouse {

namespace ch = ::clickhouse;

bool is_transient_code(int code) {
    switch (code) {
        case ch::ErrorCodes::TIMEOUT_EXCEEDED:
        case ch::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES:
        case ch::ErrorCodes::MEMORY_LIMIT_EXCEEDED:
        case ch::ErrorCodes::TOO_MANY_PARTS:
        case ch::ErrorCodes::SOCKET_TIMEOUT:
        case ch::ErrorCodes::NETWORK_ERROR:
            return true;
        default:
            return false;
    }
}

ch::ClientOptions client_options(const config::ClickHouseSection& section) {
    ch::CompressionMethod compression = ch::CompressionMethod::None;
    switch (section.compression.value) {
        case config::Compression::Lz4:  compression = ch::CompressionMethod::LZ4; break;
        case config::Compression::Zstd: compression = ch::CompressionMethod::ZSTD; break;
        case config::Compression::None: compression = ch::CompressionMethod::None; break;
    }

    ch::ClientOptions options;
    options.SetHost(section.host.value)
        .SetPort(static_cast<unsigned int>(section.port.value))
        .SetUser(section.user.value)
        .SetPassword(section.password.value)
        .SetDefaultDatabase(section.database.value)
        .SetCompressionMethod(compression)
        .SetPingBeforeQuery(true)
        // Retries are ours (decision 8.3), not the library's blocking ones.
        .SetSendRetries(0)
        .SetRetryTimeout(std::chrono::seconds(0))
        .SetConnectionConnectTimeout(std::chrono::seconds(5))
        .SetConnectionRecvTimeout(std::chrono::seconds(60))
        .SetConnectionSendTimeout(std::chrono::seconds(60));
    return options;
}

Client::Client(ch::ClientOptions options) : options_(std::move(options)) {}

template <typename Call>
void Client::call(std::string_view what, Call&& body) {
    const auto context = [&](std::string_view message) {
        return std::format("ClickHouse {}:{}: {}: {}", options_.host, options_.port, what, message);
    };
    try {
        if (!client_) client_ = std::make_unique<ch::Client>(options_);
        body(*client_);
    } catch (const ch::ServerException& e) {
        const std::string message = context(std::format("{} (code {})", e.what(), e.GetCode()));
        if (is_transient_code(e.GetCode())) throw TransientError(Resource::ClickHouse, message);
        throw ServerError(e.GetCode(), message);
    } catch (const std::system_error& e) {
        // Socket errors: refused, reset, timed out, name resolution.
        throw TransientError(Resource::ClickHouse, context(e.what()));
    } catch (const ch::ProtocolError& e) {
        // A connection cut in the middle of a packet or a failed handshake.
        throw TransientError(Resource::ClickHouse, context(e.what()));
    } catch (const ch::Error& e) {
        throw std::runtime_error(context(e.what()));
    }
}

void Client::Execute(const std::string& sql) {
    call("query failed", [&](ch::Client& client) { client.Execute(ch::Query(sql)); });
}

void Client::Select(const std::string& sql, const std::function<void(const ch::Block&)>& on_block) {
    call("query failed", [&](ch::Client& client) { client.Select(sql, on_block); });
}

void Client::Insert(const std::string& table, const ch::Block& block) {
    call(std::format("insert into {} failed", table), [&](ch::Client& client) { client.Insert(table, block); });
}

void Client::ResetConnection() {
    client_.reset();
}

}  // namespace recordflow::sink::clickhouse
