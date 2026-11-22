#pragma once

#include <filesystem>

// One instance per data directory (decision 4.2): an exclusive flock on
// data/recordflow.lock, held until the object is destroyed or the process ends.
namespace recordflow::state {

class DirectoryLock {
public:
    static constexpr const char* kFileName = "recordflow.lock";

    // Throws InterventionError if another process holds the lock.
    explicit DirectoryLock(const std::filesystem::path& data_dir);
    ~DirectoryLock();
    DirectoryLock(const DirectoryLock&) = delete;
    DirectoryLock& operator=(const DirectoryLock&) = delete;

private:
    int fd_ = -1;
};

}  // namespace recordflow::state
