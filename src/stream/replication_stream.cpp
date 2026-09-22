#include "stream/replication_stream.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "config.hpp"
#include "stream/parser/pgoutput_parser.hpp"
#include "stream/publisher/kafka_publisher.hpp"
#include "stream/wire.hpp"

namespace {

std::string format_lsn(std::uint64_t lsn) {
    char buf[24];
    std::snprintf(buf, sizeof buf, "%X/%X", static_cast<unsigned>(lsn >> 32),
                  static_cast<unsigned>(lsn));
    return buf;
}

bool has_param(const SourceConfig& config, std::string_view keyword) {
    return std::any_of(config.params.begin(), config.params.end(),
                       [&](const auto& param) { return param.first == keyword; });
}

PGconn* connect_replication(SourceConfig config) {
    config.params.emplace_back("replication", "database");

    // Values reach the payload as JSON strings, which must be UTF-8. The server
    // transcodes decoded output to client_encoding, so pinning it here keeps a
    // non-UTF-8 source database from emitting byte sequences that would make
    // the published JSON invalid. An explicit setting in the config wins.
    if (!has_param(config, "client_encoding")) {
        config.params.emplace_back("client_encoding", "UTF8");
    }

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

int run_stream(PGconn* conn, KafkaPublisher& publisher) {
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
                    for (const pgoutput::ChangeEvent& event : msg.events) {
                        publisher.publish(event);
                    }
                    if (msg.type == 'B') {
                        in_transaction = true;
                    } else if (msg.type == 'C') {
                        in_transaction = false;
                        // The slot may only advance over WAL the broker has
                        // durably taken. Carrying on after a failed flush would
                        // be worse than stopping: the next transaction's flush
                        // could succeed and confirm a higher LSN, moving the
                        // slot straight past the events that were lost. Exiting
                        // leaves the slot where it is, so a restart replays
                        // from the last transaction known to be on the broker.
                        if (!publisher.flush()) {
                            std::fprintf(stderr,
                                         "delivery failed; stopping with the "
                                         "slot at %s so the transaction is "
                                         "replayed on the next run\n",
                                         format_lsn(confirmed).c_str());
                            return 1;
                        }
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
        } catch (const PublishError& e) {
            // A decode failure is survivable, a broken producer is not:
            // continuing would silently drop every subsequent event.
            std::fprintf(stderr, "publish failed: %s\n", e.what());
            return 1;
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

    // Built before the replication connection so that a bad producer config
    // fails without having opened a slot on the source database.
    std::optional<KafkaPublisher> publisher;
    try {
        publisher.emplace(config.sink);
    } catch (const PublishError& e) {
        std::fprintf(stderr, "kafka error: %s\n", e.what());
        return 1;
    }

    PGconn* conn = connect_replication(std::move(config.source));
    if (PQstatus(conn) != CONNECTION_OK) {
        std::fprintf(stderr, "connection failed: %s", PQerrorMessage(conn));
        PQfinish(conn);
        return 1;
    }
    std::fprintf(stderr,
                 "Connected; streaming from slot '%s' via publication '%s' "
                 "into topic '%s'.\n",
                 slot.c_str(), publication.c_str(), config.sink.topic.c_str());

    int rc = 1;
    if (start_replication(conn, slot, publication)) {
        std::fprintf(stderr, "Streaming (Ctrl+C to stop)...\n");
        rc = run_stream(conn, *publisher);
    }

    PQfinish(conn);
    return rc;
}
