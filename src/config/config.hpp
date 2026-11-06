#pragma once

#include <string>

#include "runtime/log.hpp"

// Configuration model from decision 6 (with the [retry] and [shutdown]
// sections from decision 8 and the ClickHouse user from decision 10).
// Every value remembers where it came from, for the effective-config printout.
namespace recordflow::config {

enum class Source { Default, File, Env, Cli };

template <typename T>
struct Setting {
    T value{};
    Source source = Source::Default;
};

enum class SnapshotOnStart { Always, IfIncomplete };
enum class SinkKind { ClickHouse, Jsonl };
enum class Compression { Lz4, Zstd, None };

struct SourceSection {
    Setting<std::string> conninfo;     // required, libpq conninfo
    Setting<std::string> slot;         // required
    Setting<std::string> publication;  // required
    Setting<std::string> table;        // required, "schema.table"
};

struct ClickHouseSection {
    Setting<std::string> host{"localhost"};
    Setting<int> port{9000};
    Setting<std::string> database{"recordflow"};
    Setting<std::string> user{"recordflow"};
    Setting<std::string> password{};  // only from the environment
    Setting<Compression> compression{Compression::Lz4};
    Setting<std::string> table{};     // empty: "<schema>__<table>"
};

struct StateSection {
    Setting<std::string> data_dir{"data"};
};

struct BatchSection {
    Setting<int> max_rows{50000};
    Setting<int> max_mb{32};
    Setting<int> max_delay_ms{1000};
};

struct SnapshotSection {
    Setting<SnapshotOnStart> on_start{SnapshotOnStart::Always};
    Setting<int> chunk_size{1000};
    Setting<int> chunk_delay_ms{0};
    Setting<int> window_timeout_s{60};
};

struct OutputSection {
    Setting<SinkKind> sink{SinkKind::ClickHouse};
};

struct LogSection {
    Setting<log::Level> level{log::Level::Info};
};

struct RetrySection {
    Setting<int> initial_ms{1000};
    Setting<int> max_ms{60000};
    Setting<int> slot_busy_timeout_s{300};
};

struct ShutdownSection {
    Setting<int> timeout_s{8};
};

struct Config {
    SourceSection source;
    ClickHouseSection clickhouse;
    StateSection state;
    BatchSection batch;
    SnapshotSection snapshot;
    OutputSection output;
    LogSection log;
    RetrySection retry;
    ShutdownSection shutdown;
};

}  // namespace recordflow::config
