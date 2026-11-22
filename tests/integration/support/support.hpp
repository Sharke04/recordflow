#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace clickhouse {
class Client;
}

struct pg_conn;

namespace recordflow::it {

// Integration tests run only with RECORDFLOW_IT=1 and a running docker compose.
bool integration_enabled();

// Value of a setting: the environment first, then the repository's .env file,
// then the fallback.
std::string setting(const std::string& key, const std::string& fallback);

// ClickHouse client authenticated as the compose administrator.
std::unique_ptr<clickhouse::Client> clickhouse_admin();

// PostgreSQL connection authenticated as the compose superuser.
class PgAdmin {
public:
    PgAdmin();
    ~PgAdmin();
    PgAdmin(const PgAdmin&) = delete;
    PgAdmin& operator=(const PgAdmin&) = delete;

    void exec(const std::string& sql);
    std::vector<std::vector<std::string>> query(const std::string& sql);

private:
    pg_conn* conn_ = nullptr;
};

// Name that is unique across tests and runs, e.g. "it_orders_4711_3_9f2c".
std::string unique_name(const std::string& prefix);

// Polls the predicate until it returns true or the timeout expires.
bool wait_until(const std::function<bool()>& predicate,
                std::chrono::milliseconds timeout,
                std::chrono::milliseconds interval = std::chrono::milliseconds(200));

}  // namespace recordflow::it

#define RECORDFLOW_REQUIRE_INTEGRATION()                                         \
    do {                                                                         \
        if (!::recordflow::it::integration_enabled()) {                          \
            GTEST_SKIP() << "set RECORDFLOW_IT=1 and start docker compose to run " \
                            "integration tests";                                 \
        }                                                                        \
    } while (0)
