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
#include "stream/parser/pgoutput_parser.hpp"
#include "stream/printer/change_event_printer.hpp"
#include "stream/wire.hpp"

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
        std::fprintf(stderr, "could not start replication: %s", PQerrorMessage(conn));

    PQclear(res);
    return ok;
}

void send_standby_status(PGconn* conn, std::uint64_t lsn) {
    constexpr std::uint64_t kNoClientTime = 0;
    constexpr std::uint8_t kNoReplyRequested = 0;

    pgwire::ByteWriter msg;
    msg.put('r')
       .putLong(lsn)
       .putLong(lsn)
       .putLong(lsn)
       .putLong(kNoClientTime)
       .put(kNoReplyRequested);

    if (PQputCopyData(conn, msg.data(), static_cast<int>(msg.size())) < 0 ||
        PQflush(conn) < 0) {
        std::fprintf(stderr, "warning: could not send standby status: %s",
                     PQerrorMessage(conn));
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

void handle_xlog_data(PGconn* conn, pgwire::ByteReader& frame,
                      PgoutputParser& parser, StreamState& state) {
    frame.getLong();
    frame.getLong();
    frame.getLong();
    const pgoutput::ParsedMessage msg =
        parser.handle_message(frame.rest(), frame.remaining());

    if (msg.relation) {
        print_relation(*msg.relation);
    }
    for (const pgoutput::ChangeEvent& event : msg.events) {
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

void handle_keepalive(PGconn* conn, pgwire::ByteReader& frame, StreamState& state) {
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
    PgoutputParser parser;
    StreamState state;

    for (;;) {
        CopyBuffer buf;
        const int len = PQgetCopyData(conn, &buf.data, 0);
        if (len == -1) {
            std::fprintf(stderr, "Stream ended.\n");
            return 0;
        }
        if (len == -2) {
            std::fprintf(stderr, "stream read failed: %s", PQerrorMessage(conn));
            return 1;
        }
        if (len <= 0) {
            continue;
        }

        try {
            pgwire::ByteReader frame(buf.data, static_cast<std::size_t>(len));
            switch (frame.get()) {
                case 'w':
                    handle_xlog_data(conn, frame, parser, state);
                    break;
                case 'k':
                    handle_keepalive(conn, frame, state);
                    break;
            }
        } catch (const std::exception& e) {
            std::fprintf(stderr, "warning: skipping malformed frame: %s\n", e.what());
        }
    }
}

}

int stream(const std::string& slot, const std::string& publication) {
    Config config;
    try {
        config = load_config();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "config error: %s\n", e.what());
        return 1;
    }

    PGconn* conn = connect_replication(std::move(config.params));
    if (PQstatus(conn) != CONNECTION_OK) {
        std::fprintf(stderr, "connection failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    std::fprintf(stderr,
                 "Connected; streaming from slot '%s' via publication '%s'.\n",
                 slot.c_str(), publication.c_str());

    int exit_code = 1;
    if (start_replication(conn, slot, publication)) {
        std::fprintf(stderr, "Streaming (Ctrl+C to stop)...\n");
        exit_code = run_stream(conn);
    }

    PQfinish(conn);
    return exit_code;
}
