#pragma once

#include <string>
#include <string_view>

#include "runtime/errors.hpp"

struct pg_conn;
struct pg_result;

// The one place where libpq failures are classified (decision 8.1).
namespace recordflow::pg {

enum class ErrorClass { Transient, SlotBusy, Intervention, Unexpected };

// The replication slot is used by another walsender: retried until
// retry.slot_busy_timeout_s, then exit code 4.
class SlotBusyError : public TransientError {
public:
    explicit SlotBusyError(const std::string& what)
        : TransientError(Resource::PostgresReplication, what) {}
};

// `sqlstate` may be empty (e.g. a failed connection attempt), then the message
// text and `connection_lost` decide.
ErrorClass classify(std::string_view sqlstate, std::string_view message, bool connection_lost);

// Throws the exception matching the error on `conn` / `result`
// (`result` may be null). `context` says what was being done.
[[noreturn]] void raise(Resource resource, std::string_view context, pg_conn* conn,
                        const pg_result* result = nullptr);

}  // namespace recordflow::pg
