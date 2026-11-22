#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "core/version.hpp"
#include "pg/conninfo.hpp"

struct pg_conn;
struct pg_result;

namespace recordflow::pg {

// One CopyData message from the replication stream; owns the libpq buffer.
class CopyData {
public:
    CopyData(char* data, std::size_t size) : data_(data), size_(size) {}
    CopyData(CopyData&& other) noexcept;
    CopyData& operator=(CopyData&& other) noexcept;
    CopyData(const CopyData&) = delete;
    CopyData& operator=(const CopyData&) = delete;
    ~CopyData();

    const char* data() const { return data_; }
    std::size_t size() const { return size_; }

private:
    char* data_ = nullptr;
    std::size_t size_ = 0;
};

struct SystemIdentity {
    std::string system_id;
    std::uint32_t timeline = 0;
    core::Lsn xlogpos = 0;
    std::string dbname;
};

// A `replication=database` connection: START_REPLICATION, reading CopyData
// frames and sending standby status updates.
class ReplicationConnection {
public:
    // Connects with parameters from build_connection_params(..., Replication);
    // throws TransientError if the server cannot be reached.
    explicit ReplicationConnection(const KeyValues& params);
    ~ReplicationConnection();
    ReplicationConnection(const ReplicationConnection&) = delete;
    ReplicationConnection& operator=(const ReplicationConnection&) = delete;

    // IDENTIFY_SYSTEM: the cluster identity and the current WAL position.
    SystemIdentity identify_system();

    // START_REPLICATION from `start_lsn` (the server starts at the later of it
    // and the slot's confirmed_flush_lsn), protocol 1, with logical messages.
    // Errors are classified by pg/errors (e.g. a missing slot: InterventionError,
    // a slot used by another walsender: SlotBusyError).
    void start(const std::string& slot, const std::string& publication, core::Lsn start_lsn);

    // Returns the next frame if one arrives within `timeout`, otherwise
    // std::nullopt (also when a signal interrupted the wait or the server ended
    // the stream; see ended()). Never blocks longer than `timeout`.
    // Throws InterventionError once the server reports that a publication no
    // longer exists: since PostgreSQL 18 it only warns and skips the changes
    // (decision 8.6).
    std::optional<CopyData> poll(std::chrono::milliseconds timeout);

    // True once the server has ended the COPY stream.
    bool ended() const { return ended_; }

    // Raises the position reported to the server as written, flushed and
    // applied (it never moves backwards). The engine decides when data is safe.
    void confirm(core::Lsn lsn);
    core::Lsn confirmed() const { return confirmed_; }

    // Sends a standby status update if the confirmed position changed, if the
    // last one is older than the status interval, or if `force` (the server
    // asked for a reply). Failures are logged.
    void maybe_send_status(std::chrono::steady_clock::time_point now, bool force = false);

    // Ends the COPY stream (CopyDone) after the last status update and waits
    // up to `timeout` for the server to finish it (decision 8.4, step 6).
    // Failures are logged; the connection is closed by the destructor.
    void finish(std::chrono::milliseconds timeout);

    static constexpr std::chrono::seconds kStatusInterval{10};

private:
    void finish_copy();
    std::string escape_identifier(const std::string& name);
    std::string escape_literal(const std::string& text);

    void send_status(std::chrono::steady_clock::time_point now);
    static void on_notice(void* self, const pg_result* notice);
    void check_notices();

    pg_conn* conn_ = nullptr;
    bool ended_ = false;
    std::optional<std::string> missing_publication_;  // the server's warning text
    core::Lsn confirmed_ = 0;
    core::Lsn last_sent_ = 0;
    std::chrono::steady_clock::time_point last_sent_at_{};
};

}  // namespace recordflow::pg
