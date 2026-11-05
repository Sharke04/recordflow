#include "stream/replication_stream.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <string>
#include <utility>
#include <vector>

#include "config.hpp"
#include "pgoutput/decoder.hpp"
#include "stream/printer/change_event_printer.hpp"
#include "pgoutput/wire.hpp"
#include "runtime/log.hpp"

namespace recordflow {

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

bool start_replication(PGconn* conn, const std::string& slot,
                       const std::string& publication) {
    const std::string command =
        "START_REPLICATION SLOT " + slot + " LOGICAL 0/0"
        " (proto_version '1', publication_names '\"" + publication + "\"')";

    PGresult* res = PQexec(conn, command.c_str());
    const bool ok = res && PQresultStatus(res) == PGRES_COPY_BOTH;

    if (!ok)
        log::error("could not start replication: {}", PQerrorMessage(conn));

    PQclear(res);
    return ok;
}

void send_standby_status(PGconn* conn, std::uint64_t lsn) {
    constexpr std::uint64_t kNoClientTime = 0;
    constexpr std::uint8_t kNoReplyRequested = 0;

    pgoutput::ByteWriter msg;
    msg.put('r')
       .putLong(lsn)
       .putLong(lsn)
       .putLong(lsn)
       .putLong(kNoClientTime)
       .put(kNoReplyRequested);

    if (PQputCopyData(conn, msg.data(), static_cast<int>(msg.size())) < 0 ||
        PQflush(conn) < 0) {
        log::warn("could not send standby status: {}", PQerrorMessage(conn));
    }
}

struct CopyBuffer {
    char* data = nullptr;
    ~CopyBuffer() { PQfreemem(data); }
};

struct StreamState {
    std::uint64_t confirmed = 0;
    bool in_transaction = false;
};

void handle_xlog_data(PGconn* conn, pgoutput::ByteReader& frame,
                      pgoutput::PgoutputDecoder& decoder, StreamState& state) {
    frame.getLong();
    frame.getLong();
    frame.getLong();
    const pgoutput::ParsedMessage msg =
        decoder.handle_message(frame.rest(), frame.remaining());

    if (msg.relation) {
        print_relation(*msg.relation);
    }
    for (const core::ChangeEvent& event : msg.events) {
        print_change_event(event);
    }

    if (msg.type == 'B') {
        state.in_transaction = true;
    } else if (msg.type == 'C') {
        state.in_transaction = false;
        state.confirmed = std::max(state.confirmed, msg.commit_end_lsn);
        send_standby_status(conn, state.confirmed);
    }
}

void handle_keepalive(PGconn* conn, pgoutput::ByteReader& frame, StreamState& state) {
    const auto wal_end = static_cast<std::uint64_t>(frame.getLong());
    frame.getLong();
    const bool reply_requested = frame.get() != 0;
    if (!state.in_transaction) {
        state.confirmed = std::max(state.confirmed, wal_end);
    }
    if (reply_requested) {
        send_standby_status(conn, state.confirmed);
    }
}

int run_stream(PGconn* conn) {
    pgoutput::PgoutputDecoder decoder;
    StreamState state;

    for (;;) {
        CopyBuffer buf;
        const int len = PQgetCopyData(conn, &buf.data, 0);
        if (len == -1) {
            log::info("Stream ended.");
            return 0;
        }
        if (len == -2) {
            log::error("stream read failed: {}", PQerrorMessage(conn));
            return 1;
        }
        if (len <= 0) {
            continue;
        }

        try {
            pgoutput::ByteReader frame(buf.data, static_cast<std::size_t>(len));
            switch (frame.get()) {
                case 'w':
                    handle_xlog_data(conn, frame, decoder, state);
                    break;
                case 'k':
                    handle_keepalive(conn, frame, state);
                    break;
            }
        } catch (const std::exception& e) {
            log::warn("skipping malformed frame: {}", e.what());
        }
    }
}

}

int stream(const std::string& slot, const std::string& publication) {
    Config config;
    try {
        config = load_config();
    } catch (const std::exception& e) {
        log::error("config error: {}", e.what());
        return 1;
    }

    PGconn* conn = connect_replication(std::move(config.params));
    if (PQstatus(conn) != CONNECTION_OK) {
        log::error("connection failed: {}", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    log::info("Connected; streaming from slot '{}' via publication '{}'.", slot, publication);

    int exit_code = 1;
    if (start_replication(conn, slot, publication)) {
        log::info("Streaming (Ctrl+C to stop)...");
        exit_code = run_stream(conn);
    }

    PQfinish(conn);
    return exit_code;
}

}  // namespace recordflow
