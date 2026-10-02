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
#include "stream/wire.hpp"

namespace {

PGconn* connect_replication(SourceConfig config) {
    config.params.emplace_back("replication", "database");

    std::vector<const char*> keywords, values;
    for (const auto& [keyword, value] : config.params) {
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
    if (!ok) {
        std::fprintf(stderr, "could not start replication: %s",
                     PQerrorMessage(conn));
    }
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

int run_stream(PGconn* conn) {
    PgoutputParser parser;

    std::uint64_t confirmed = 0;
    bool in_transaction = false;

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
            pgwire::ByteCursor frame(buf.data, static_cast<std::size_t>(len));
            switch (frame.get()) {
                case 'w': {
                    frame.getLong();
                    frame.getLong();
                    frame.getLong();
                    const pgoutput::ParsedMessage msg =
                        parser.handle_message(frame.rest(), frame.remaining());
                    if (msg.type == 'B') {
                        in_transaction = true;
                    } else if (msg.type == 'C') {
                        in_transaction = false;
                        confirmed = std::max(confirmed, msg.commit_end_lsn);
                        send_standby_status(conn, confirmed);
                    }
                    break;
                }
                case 'k': {
                    const auto wal_end = static_cast<std::uint64_t>(frame.getLong());
                    frame.getLong();
                    const bool reply_requested = frame.get() != 0;
                    if (!in_transaction) {
                        confirmed = std::max(confirmed, wal_end);
                    }
                    if (reply_requested) {
                        send_standby_status(conn, confirmed);
                    }
                    break;
                }
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

    PGconn* conn = connect_replication(std::move(config.source));
    if (PQstatus(conn) != CONNECTION_OK) {
        std::fprintf(stderr, "connection failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    std::fprintf(stderr,
                 "Connected; streaming from slot '%s' via publication '%s'.\n",
                 slot.c_str(), publication.c_str());

    int rc = 1;
    if (start_replication(conn, slot, publication)) {
        std::fprintf(stderr, "Streaming (Ctrl+C to stop)...\n");
        rc = run_stream(conn);
    }

    PQfinish(conn);
    return rc;
}
