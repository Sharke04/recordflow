#pragma once

#include <filesystem>
#include <optional>

#include "state/state.hpp"

// Reads and writes data/recordflow.state.json (decisions 4.2 and 10.4).
namespace recordflow::state {

class StateStore {
public:
    static constexpr const char* kFileName = "recordflow.state.json";

    explicit StateStore(std::filesystem::path data_dir);

    // Creates the data directory with mode 0700 if it is missing; warns if an
    // existing one is accessible to group or others (its mode is kept).
    void prepare_directory() const;

    // nullopt if there is no state file (first start). A file that cannot be
    // read or parsed is an InterventionError; it is never removed.
    std::optional<State> load() const;

    // Atomic replacement: write <file>.tmp (0600), fsync it, rename it over
    // the file and fsync the directory. A failure throws std::runtime_error
    // (exit code 1) and leaves the previous file intact.
    void save(const State& state) const;

    const std::filesystem::path& directory() const { return dir_; }
    const std::filesystem::path& file() const { return file_; }

private:
    std::filesystem::path dir_;
    std::filesystem::path file_;
};

}  // namespace recordflow::state
