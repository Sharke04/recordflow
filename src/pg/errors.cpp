#include "pg/errors.hpp"

#include <libpq-fe.h>

#include <array>
#include <format>
#include <stdexcept>

namespace recordflow::pg {

namespace {

bool contains(std::string_view text, std::string_view part) {
    return text.find(part) != std::string_view::npos;
}

std::string trimmed(std::string text) {
    while (!text.empty() && (text.back() == '\n' || text.back() == ' ')) text.pop_back();
    return text;
}

}  // namespace

ErrorClass classify(std::string_view sqlstate, std::string_view message, bool connection_lost) {
    // Problems that waiting cannot fix: missing objects, privileges, credentials.
    static constexpr std::array kIntervention{
        std::string_view("password authentication failed"),
        std::string_view("no pg_hba.conf entry"),
        std::string_view("does not exist"),
        std::string_view("permission denied"),
        std::string_view("must be superuser or replication role"),
        std::string_view("can no longer get changes from replication slot"),
        std::string_view("has been invalidated"),
    };

    if (sqlstate == "55006" || contains(message, "is active for PID")) return ErrorClass::SlotBusy;
    if (sqlstate.starts_with("08") || sqlstate == "57P01" || sqlstate == "57P02" ||
        sqlstate == "57P03" || sqlstate == "53300") {
        return ErrorClass::Transient;
    }
    if (sqlstate == "42501" || sqlstate == "42704" || sqlstate == "28000" || sqlstate == "28P01" ||
        sqlstate == "3D000" || sqlstate == "55000") {
        return ErrorClass::Intervention;
    }
    for (std::string_view part : kIntervention) {
        if (contains(message, part)) return ErrorClass::Intervention;
    }
    if (connection_lost || sqlstate.empty()) return ErrorClass::Transient;
    return ErrorClass::Unexpected;
}

void raise(Resource resource, std::string_view context, PGconn* conn, const PGresult* result) {
    std::string message;
    std::string sqlstate;
    if (result != nullptr) {
        message = PQresultErrorMessage(result);
        if (const char* state = PQresultErrorField(result, PG_DIAG_SQLSTATE)) sqlstate = state;
    }
    if (message.empty() && conn != nullptr) message = PQerrorMessage(conn);
    message = trimmed(message);
    const bool connection_lost = conn == nullptr || PQstatus(conn) == CONNECTION_BAD;

    const std::string what = sqlstate.empty() ? std::format("{}: {}", context, message)
                                              : std::format("{}: {} (SQLSTATE {})", context, message, sqlstate);
    switch (classify(sqlstate, message, connection_lost)) {
        case ErrorClass::SlotBusy:     throw SlotBusyError(what);
        case ErrorClass::Transient:    throw TransientError(resource, what);
        case ErrorClass::Intervention: throw InterventionError(what);
        case ErrorClass::Unexpected:   break;
    }
    throw std::runtime_error(what);
}

}  // namespace recordflow::pg
