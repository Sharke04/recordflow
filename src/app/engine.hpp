#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "config/config.hpp"
#include "core/table_schema.hpp"
#include "state/directory_lock.hpp"
#include "state/state.hpp"
#include "state/state_store.hpp"

namespace recordflow::pg {
class QueryConnection;
class ReplicationConnection;
}  // namespace recordflow::pg

namespace recordflow::sink {
class Sink;
}  // namespace recordflow::sink

namespace recordflow::app {

// Owns all components and runs the main loop (decision 9.5): startup checks,
// the stream into the sink, checkpoints and recovery.
class Engine {
public:
    explicit Engine(config::Config config);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // Returns the process exit code; errors propagate as exceptions from
    // runtime/errors.hpp.
    int run();

private:
    // Decisions 5.1 and 8.2: lock and state, connections (with backoff while
    // the databases are unreachable), IDENTIFY_SYSTEM, catalog, publication and
    // privilege, sink.prepare, the slot's position, the startup decision and
    // START_REPLICATION from last_flushed_lsn.
    void startup();

    // Runs `step` until it does not throw a TransientError, with backoff
    // (decision 8.3); `on_failure` runs after each failed attempt.
    void retry_transient(std::string_view what, const std::function<void()>& step,
                         const std::function<void()>& on_failure = {});

    int stream();

    config::Config config_;
    state::StateStore store_;
    std::optional<state::DirectoryLock> lock_;
    state::State state_;
    core::TableSchema table_;
    std::unique_ptr<pg::QueryConnection> query_;
    std::unique_ptr<pg::ReplicationConnection> repl_;
    std::unique_ptr<sink::Sink> sink_;
};

}  // namespace recordflow::app
