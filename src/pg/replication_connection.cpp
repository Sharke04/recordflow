#include "pg/replication_connection.hpp"

#include <libpq-fe.h>
#include <poll.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <format>

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
    PQsetNoticeReceiver(conn_, &ReplicationConnection::on_notice, this);
}

void ReplicationConnection::on_notice(void* self, const PGresult* notice) {
    auto* connection = static_cast<ReplicationConnection*>(self);
    std::string message = PQresultErrorField(notice, PG_DIAG_MESSAGE_PRIMARY) != nullptr
                              ? PQresultErrorField(notice, PG_DIAG_MESSAGE_PRIMARY)
                              : PQresultErrorMessage(notice);
    // "skipped loading publication ..." (PostgreSQL 18+): the publication was
    // dropped and the server no longer sends the table's changes.
    if (message.starts_with("skipped loading publication")) {
        connection->missing_publication_ = message;
        return;
    }
    while (!message.empty() && message.back() == '\n') message.pop_back();
    log::warn("PostgreSQL replication: {}", message);
}

void ReplicationConnection::check_notices() {
    if (missing_publication_) {
        throw InterventionError(std::format(
            "the server reports \"{}\": the publication was dropped or no longer exists, so changes are not "
            "sent anymore. Recreate it (it must include the table); changes made meanwhile are missing, so then "
            "drop the ClickHouse table and its _current view and start Recordflow for a full snapshot",
            *missing_publication_));
    }
}

ReplicationConnection::~ReplicationConnection() {
    if (conn_ != nullptr) PQfinish(conn_);
}

SystemIdentity ReplicationConnection::identify_system() {
    PGresult* res = PQexec(conn_, "IDENTIFY_SYSTEM");
    if (res == nullptr || PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1 ||
        PQnfields(res) < 4) {
        try {
            raise(Resource::PostgresReplication, "IDENTIFY_SYSTEM failed", conn_, res);
        } catch (...) {
            PQclear(res);
            throw;
        }
    }
    SystemIdentity identity;
    identity.system_id = PQgetvalue(res, 0, 0);
    identity.timeline = static_cast<std::uint32_t>(std::stoul(PQgetvalue(res, 0, 1)));
    identity.xlogpos = core::parse_lsn(PQgetvalue(res, 0, 2)).value_or(0);
    identity.dbname = PQgetisnull(res, 0, 3) ? "" : PQgetvalue(res, 0, 3);
    PQclear(res);
    return identity;
}

std::string ReplicationConnection::escape_identifier(const std::string& name) {
    char* escaped = PQescapeIdentifier(conn_, name.c_str(), name.size());
    if (escaped == nullptr) raise(Resource::PostgresReplication, "cannot escape identifier", conn_);
    std::string result = escaped;
    PQfreemem(escaped);
    return result;
}

std::string ReplicationConnection::escape_literal(const std::string& text) {
    char* escaped = PQescapeLiteral(conn_, text.c_str(), text.size());
    if (escaped == nullptr) raise(Resource::PostgresReplication, "cannot escape literal", conn_);
    std::string result = escaped;
    PQfreemem(escaped);
    return result;
}

void ReplicationConnection::start(const std::string& slot, const std::string& publication,
                                  core::Lsn start_lsn) {
    const std::string command = std::format(
        "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names {}, messages 'true')",
        escape_identifier(slot), core::format_lsn(start_lsn), escape_literal(escape_identifier(publication)));

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

std::optional<CopyData> ReplicationConnection::poll(std::chrono::milliseconds timeout) {
    if (ended_) return std::nullopt;
    for (bool waited = false;; waited = true) {
        char* data = nullptr;
        const int len = PQgetCopyData(conn_, &data, /*async=*/1);
        check_notices();  // before anything after the warning is processed
        if (len > 0) return CopyData(data, static_cast<std::size_t>(len));
        if (len == -1) {
            ended_ = true;
            finish_copy();
            return std::nullopt;
        }
        if (len == -2) raise(Resource::PostgresReplication, "stream read failed", conn_);
        if (waited) return std::nullopt;  // no complete message yet

        pollfd fd{PQsocket(conn_), POLLIN, 0};
        const int ready = ::poll(&fd, 1, static_cast<int>(timeout.count()));
        if (ready < 0) {
            if (errno == EINTR) return std::nullopt;  // a signal; the caller checks its flags
            throw TransientError(Resource::PostgresReplication,
                                 std::string("poll failed: ") + std::strerror(errno));
        }
        if (ready == 0) return std::nullopt;
        if (PQconsumeInput(conn_) == 0) raise(Resource::PostgresReplication, "stream read failed", conn_);
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

void ReplicationConnection::confirm(core::Lsn lsn) {
    confirmed_ = std::max(confirmed_, lsn);
}

void ReplicationConnection::maybe_send_status(std::chrono::steady_clock::time_point now, bool force) {
    if (force || confirmed_ != last_sent_ || now - last_sent_at_ >= kStatusInterval) send_status(now);
}

void ReplicationConnection::send_status(std::chrono::steady_clock::time_point now) {
    // Client clock in microseconds since 2000-01-01, as the protocol expects.
    constexpr std::int64_t kPostgresEpochUnixSeconds = 946684800;
    const auto unix_micros = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    const auto client_time = static_cast<std::uint64_t>(unix_micros - kPostgresEpochUnixSeconds * 1000000);
    constexpr std::uint8_t kNoReplyRequested = 0;

    pgoutput::ByteWriter msg;
    msg.put('r')
       .putLong(confirmed_)
       .putLong(confirmed_)
       .putLong(confirmed_)
       .putLong(client_time)
       .put(kNoReplyRequested);

    if (PQputCopyData(conn_, msg.data(), static_cast<int>(msg.size())) < 0 || PQflush(conn_) < 0) {
        log::warn("could not send standby status: {}", PQerrorMessage(conn_));
        return;
    }
    last_sent_ = confirmed_;
    last_sent_at_ = now;
}

}  // namespace recordflow::pg
