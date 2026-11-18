#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "support/process.hpp"
#include "support/support.hpp"

namespace recordflow::it {

// One source table per test, `public.<name> (id bigint PRIMARY KEY, name text,
// amount numeric(12,2))`, with its own publication and slot, its ClickHouse
// target `recordflow.public__<name>` and a private data directory. The
// destructor removes all of it.
class StreamTable {
public:
    explicit StreamTable(const std::string& prefix = "it_orders");
    ~StreamTable();
    StreamTable(const StreamTable&) = delete;
    StreamTable& operator=(const StreamTable&) = delete;

    const std::string& name() const { return name_; }
    std::string qualified() const { return "public." + name_; }
    std::string target() const { return "recordflow.public__" + name_; }
    const std::string& slot() const { return slot_; }
    const std::filesystem::path& data_dir() const { return data_dir_; }

    PgAdmin& pg() { return pg_; }

    // Configuration for recordflow through the environment.
    std::map<std::string, std::string> env() const;

    // Starts recordflow for this table; `extra` overrides env().
    std::unique_ptr<RecordflowProcess> start(const std::map<std::string, std::string>& extra = {});
    // Starts recordflow and waits until it streams.
    std::unique_ptr<RecordflowProcess> start_streaming(const std::map<std::string, std::string>& extra = {});

    // confirmed_flush_lsn of the slot and last_flushed_lsn of the state file.
    std::string slot_confirmed_lsn();
    std::string state_last_flushed_lsn() const;

    // Source rows (ORDER BY id) and the rows of <target>_current, both as
    // "id|name|amount" with the values as the converter writes them.
    std::vector<std::string> source_rows();
    std::vector<std::string> target_rows();

private:
    std::string name_;
    std::string publication_;
    std::string slot_;
    std::filesystem::path root_;
    std::filesystem::path data_dir_;
    int starts_ = 0;
    PgAdmin pg_;
};

// Compares the source with <target>_current until they are equal or the
// timeout expires; the failure message shows the first difference.
::testing::AssertionResult target_equals_source(StreamTable& table,
                                                std::chrono::milliseconds timeout = std::chrono::seconds(20));

}  // namespace recordflow::it
