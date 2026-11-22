#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "core/version.hpp"
#include "pg/conninfo.hpp"

struct pg_conn;

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
    std::optional<CopyData> poll(std::chrono::milliseconds timeout);

    // True once the server has ended the COPY stream.
    bool ended() const { return ended_; }

    // Reports `lsn` as written, flushed and applied. Failures are logged.
    void send_standby_status(std::uint64_t lsn);

private:
    void finish_copy();
    std::string escape_identifier(const std::string& name);
    std::string escape_literal(const std::string& text);

    pg_conn* conn_ = nullptr;
    bool ended_ = false;
};

}  // namespace recordflow::pg
