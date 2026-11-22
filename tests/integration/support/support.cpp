#include "support/support.hpp"

#include <clickhouse/client.h>
#include <libpq-fe.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace recordflow::it {

namespace {

std::map<std::string, std::string> load_dotenv() {
    std::map<std::string, std::string> values;
    std::ifstream in(std::string(RECORDFLOW_SOURCE_DIR) + "/.env");
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line.front() == '#') continue;
        const auto eq = line.find('=');
        if (eq == std::string::npos) continue;
        values[line.substr(0, eq)] = line.substr(eq + 1);
    }
    return values;
}

const std::map<std::string, std::string>& dotenv() {
    static const auto values = load_dotenv();
    return values;
}

}  // namespace

bool integration_enabled() {
    const char* value = std::getenv("RECORDFLOW_IT");
    return value != nullptr && std::string(value) == "1";
}

std::string setting(const std::string& key, const std::string& fallback) {
    if (const char* value = std::getenv(key.c_str())) return value;
    const auto it = dotenv().find(key);
    return it != dotenv().end() ? it->second : fallback;
}

std::unique_ptr<clickhouse::Client> clickhouse_admin() {
    clickhouse::ClientOptions options;
    options.SetHost(setting("RECORDFLOW_IT_CLICKHOUSE_HOST", "localhost"))
        .SetPort(static_cast<uint16_t>(std::stoi(setting("RECORDFLOW_IT_CLICKHOUSE_PORT", "9000"))))
        .SetUser(setting("CLICKHOUSE_ADMIN_USER", "admin"))
        .SetPassword(setting("CLICKHOUSE_ADMIN_PASSWORD", ""));
    return std::make_unique<clickhouse::Client>(options);
}

PgAdmin::PgAdmin() {
    const std::string conninfo =
        "host=" + setting("RECORDFLOW_IT_POSTGRES_HOST", "localhost") +
        " port=" + setting("RECORDFLOW_IT_POSTGRES_PORT", "5432") +
        " dbname=" + setting("POSTGRES_DB", "mydb") +
        " user=" + setting("POSTGRES_ADMIN_USER", "postgres") +
        " password=" + setting("POSTGRES_ADMIN_PASSWORD", "");
    conn_ = PQconnectdb(conninfo.c_str());
    if (PQstatus(conn_) != CONNECTION_OK) {
        const std::string error = PQerrorMessage(conn_);
        PQfinish(conn_);
        conn_ = nullptr;
        throw std::runtime_error("PostgreSQL admin connection failed: " + error);
    }
}

PgAdmin::~PgAdmin() {
    if (conn_ != nullptr) PQfinish(conn_);
}

void PgAdmin::exec(const std::string& sql) {
    query(sql);
}

std::vector<std::vector<std::string>> PgAdmin::query(const std::string& sql) {
    PGresult* result = PQexec(conn_, sql.c_str());
    const auto status = PQresultStatus(result);
    if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
        const std::string error = PQerrorMessage(conn_);
        PQclear(result);
        throw std::runtime_error("PostgreSQL query failed: " + error + "query: " + sql);
    }
    std::vector<std::vector<std::string>> rows;
    for (int r = 0; r < PQntuples(result); ++r) {
        std::vector<std::string> row;
        for (int c = 0; c < PQnfields(result); ++c) {
            row.emplace_back(PQgetisnull(result, r, c) ? "" : PQgetvalue(result, r, c));
        }
        rows.push_back(std::move(row));
    }
    PQclear(result);
    return rows;
}

std::string unique_name(const std::string& prefix) {
    static std::atomic<int> counter{0};
    static const unsigned salt = std::random_device{}();
    std::ostringstream name;
    name << prefix << '_' << ::getpid() << '_' << counter++ << '_' << std::hex << (salt & 0xffff);
    return name.str();
}

bool wait_until(const std::function<bool()>& predicate,
                std::chrono::milliseconds timeout,
                std::chrono::milliseconds interval) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(interval);
    }
    return predicate();
}

}  // namespace recordflow::it
