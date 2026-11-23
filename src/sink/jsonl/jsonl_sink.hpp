#pragma once

#include <cstdio>
#include <optional>
#include <string>

#include "sink/sink.hpp"
#include "sink/tx_buffer.hpp"

// JSON Lines on stdout (decision 2.6), for development and tests: one event per
// line with the envelope of the internal model, written at flush().
namespace recordflow::sink::jsonl {

class JsonlSink : public Sink {
public:
    explicit JsonlSink(std::FILE* out = stdout) : out_(out) {}

    PrepareResult prepare(const core::TableSchema& source) override;

    void begin() override { buffer_.begin(); }
    void append(const core::ChangeEvent& event) override { buffer_.append(event); }
    void commit(core::Lsn commit_end) override { buffer_.commit(commit_end); }
    void abort() override { buffer_.abort(); }

    BufferStats stats() const override { return buffer_.stats(); }
    bool in_transaction() const override { return buffer_.in_transaction(); }

    std::optional<core::Lsn> flush(bool include_open = false) override;

    // Nothing to sweep in a stream of lines.
    void sweep(core::Version, core::Version, std::int64_t) override {}

    // One event as a JSON object on a single line (no trailing newline).
    static std::string to_json_line(const core::ChangeEvent& event);

private:
    std::FILE* out_;
    TxBuffer buffer_;
};

}  // namespace recordflow::sink::jsonl
