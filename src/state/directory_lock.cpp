#include "state/directory_lock.hpp"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <format>

#include "runtime/errors.hpp"

namespace recordflow::state {

DirectoryLock::DirectoryLock(const std::filesystem::path& data_dir) {
    const std::filesystem::path path = data_dir / kFileName;
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if (fd_ < 0) {
        throw InterventionError(std::format("cannot open lock file {}: {}", path.string(), std::strerror(errno)));
    }
    if (::flock(fd_, LOCK_EX | LOCK_NB) == 0) return;
    const int error = errno;
    ::close(fd_);
    fd_ = -1;
    if (error == EWOULDBLOCK) {
        throw InterventionError(std::format(
            "data directory {} is in use by another Recordflow instance (lock {} is held); each instance needs "
            "its own data directory",
            data_dir.string(), path.string()));
    }
    throw InterventionError(std::format("cannot lock {}: {}", path.string(), std::strerror(error)));
}

DirectoryLock::~DirectoryLock() {
    if (fd_ >= 0) ::close(fd_);  // releases the lock
}

}  // namespace recordflow::state
