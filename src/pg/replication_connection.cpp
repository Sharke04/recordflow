#include "pg/replication_connection.hpp"

#include <libpq-fe.h>

#include <utility>
#include <vector>

#include "pgoutput/wire.hpp"
#include "pg/errors.hpp"
#include "runtime/errors.hpp"
#include "runtime/log.hpp"

namespace recordflow::pg {

namespace {

PGconn* connect(const KeyValues& params) {
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

ReplicationConnection::ReplicationConnection(const KeyValues& params) : conn_(connect(params)) {
    if (PQstatus(conn_) != CONNECTION_OK) {
        try {
            raise(Resource::PostgresReplication, "replication connection failed", conn_);
        } catch (...) {
            PQfinish(conn_);
            conn_ = nullptr;
            throw;
        }
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
    if (res != nullptr && PQresultStatus(res) == PGRES_COPY_BOTH) {
        PQclear(res);
        return;
    }
    try {
        raise(Resource::PostgresReplication, "could not start replication", conn_, res);
    } catch (...) {
        PQclear(res);
        throw;
    }
}

std::optional<CopyData> ReplicationConnection::read() {
    for (;;) {
        char* data = nullptr;
        const int len = PQgetCopyData(conn_, &data, 0);
        if (len > 0) return CopyData(data, static_cast<std::size_t>(len));
        if (len == -1) {
            finish_copy();
            return std::nullopt;
        }
        if (len == -2) raise(Resource::PostgresReplication, "stream read failed", conn_);
    }
}

void ReplicationConnection::finish_copy() {
    // The server ended COPY; an error (e.g. an invalidated slot) arrives as a result.
    while (PGresult* res = PQgetResult(conn_)) {
        if (PQresultStatus(res) == PGRES_FATAL_ERROR) {
            try {
                raise(Resource::PostgresReplication, "replication stream ended with an error", conn_, res);
            } catch (...) {
                PQclear(res);
                throw;
            }
        }
        PQclear(res);
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
