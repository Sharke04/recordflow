#include "pg/replication_connection.hpp"

#include <libpq-fe.h>

#include <utility>
#include <vector>

#include "pgoutput/wire.hpp"
#include "runtime/errors.hpp"
#include "runtime/log.hpp"

namespace recordflow::pg {

namespace {

bool has_param(const KeyValues& params, const std::string& keyword) {
    for (const auto& param : params) {
        if (param.first == keyword)
            return true;
    }
    return false;
}

PGconn* connect_replication(KeyValues params) {
    // Open a logical replication connection, required for START_REPLICATION
    params.emplace_back("replication", "database");

    if (!has_param(params, "client_encoding")) {
        params.emplace_back("client_encoding", "UTF8");
    }

    std::vector<const char*> keywords, values;
    for (const auto& [keyword, value] : params) {
        keywords.push_back(keyword.c_str());
        values.push_back(value.c_str());
    }

    keywords.push_back(nullptr);
    values.push_back(nullptr);

    return PQconnectdbParams(keywords.data(), values.data(), 0);
}

}  // namespace

CopyData::CopyData(CopyData&& other) noexcept
    : data_(std::exchange(other.data_, nullptr)), size_(std::exchange(other.size_, 0)) {}

CopyData& CopyData::operator=(CopyData&& other) noexcept {
    if (this != &other) {
        if (data_ != nullptr) PQfreemem(data_);
        data_ = std::exchange(other.data_, nullptr);
        size_ = std::exchange(other.size_, 0);
    }
    return *this;
}

CopyData::~CopyData() {
    if (data_ != nullptr) PQfreemem(data_);
}

ReplicationConnection::ReplicationConnection(KeyValues params)
    : conn_(connect_replication(std::move(params))) {
    if (PQstatus(conn_) != CONNECTION_OK) {
        const std::string error = PQerrorMessage(conn_);
        PQfinish(conn_);
        conn_ = nullptr;
        throw TransientError(Resource::PostgresReplication, "connection failed: " + error);
    }
}

ReplicationConnection::~ReplicationConnection() {
    if (conn_ != nullptr) PQfinish(conn_);
}

void ReplicationConnection::start(const std::string& slot, const std::string& publication) {
    const std::string command =
        "START_REPLICATION SLOT " + slot + " LOGICAL 0/0"
        " (proto_version '1', publication_names '\"" + publication + "\"')";

    PGresult* res = PQexec(conn_, command.c_str());
    const bool ok = res && PQresultStatus(res) == PGRES_COPY_BOTH;
    PQclear(res);

    if (!ok)
        throw InterventionError(std::string("could not start replication: ") +
                                PQerrorMessage(conn_));
}

std::optional<CopyData> ReplicationConnection::read() {
    for (;;) {
        char* data = nullptr;
        const int len = PQgetCopyData(conn_, &data, 0);
        if (len > 0) return CopyData(data, static_cast<std::size_t>(len));
        if (len == -1) return std::nullopt;
        if (len == -2) {
            throw TransientError(Resource::PostgresReplication,
                                 std::string("stream read failed: ") + PQerrorMessage(conn_));
        }
    }
}

void ReplicationConnection::send_standby_status(std::uint64_t lsn) {
    constexpr std::uint64_t kNoClientTime = 0;
    constexpr std::uint8_t kNoReplyRequested = 0;

    pgoutput::ByteWriter msg;
    msg.put('r')
       .putLong(lsn)
       .putLong(lsn)
       .putLong(lsn)
       .putLong(kNoClientTime)
       .put(kNoReplyRequested);

    if (PQputCopyData(conn_, msg.data(), static_cast<int>(msg.size())) < 0 ||
        PQflush(conn_) < 0) {
        log::warn("could not send standby status: {}", PQerrorMessage(conn_));
    }
}

}  // namespace recordflow::pg
