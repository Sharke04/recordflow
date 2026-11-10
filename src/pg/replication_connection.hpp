#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

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

    // Errors are classified by pg/errors (e.g. a missing slot: InterventionError,
    // a slot used by another walsender: SlotBusyError).
    void start(const std::string& slot, const std::string& publication);

    // Blocks until the next frame; std::nullopt when the server ended the
    // stream. Throws TransientError if reading fails.
    std::optional<CopyData> read();

    // Reports `lsn` as written, flushed and applied. Failures are logged.
    void send_standby_status(std::uint64_t lsn);

private:
    void finish_copy();

    pg_conn* conn_ = nullptr;
};

}  // namespace recordflow::pg
