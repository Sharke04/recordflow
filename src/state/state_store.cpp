#include "state/state_store.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <format>
#include <stdexcept>
#include <string>
#include <utility>

#include "runtime/errors.hpp"
#include "runtime/log.hpp"

namespace recordflow::state {

namespace {

std::string errno_text() { return std::strerror(errno); }

// Closes the descriptor on every path.
class Fd {
public:
    explicit Fd(int fd) : fd_(fd) {}
    ~Fd() {
        if (fd_ >= 0) ::close(fd_);
    }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
    int get() const { return fd_; }
    int release() { return std::exchange(fd_, -1); }

private:
    int fd_;
};

void write_all(int fd, const std::string& data, const std::filesystem::path& path) {
    std::size_t written = 0;
    while (written < data.size()) {
        const ssize_t n = ::write(fd, data.data() + written, data.size() - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw std::runtime_error(std::format("cannot write {}: {}", path.string(), errno_text()));
        }
        written += static_cast<std::size_t>(n);
    }
}

}  // namespace

StateStore::StateStore(std::filesystem::path data_dir)
    : dir_(std::move(data_dir)), file_(dir_ / kFileName) {}

void StateStore::prepare_directory() const {
    struct stat info {};
    if (::stat(dir_.c_str(), &info) != 0) {
        if (errno != ENOENT) {
            throw InterventionError(std::format("cannot access data directory {}: {}", dir_.string(), errno_text()));
        }
        std::error_code ec;
        std::filesystem::create_directories(dir_.parent_path().empty() ? "." : dir_.parent_path(), ec);
        if (::mkdir(dir_.c_str(), 0700) != 0 && errno != EEXIST) {
            throw InterventionError(std::format("cannot create data directory {}: {}", dir_.string(), errno_text()));
        }
        // mkdir applies the umask; set the mode explicitly.
        ::chmod(dir_.c_str(), 0700);
        log::info("Created data directory {}.", dir_.string());
        return;
    }
    if (!S_ISDIR(info.st_mode)) {
        throw InterventionError(std::format("data directory {} is not a directory", dir_.string()));
    }
    if ((info.st_mode & 0077) != 0) {
        log::warn("Data directory {} has mode {:04o}; 0700 is recommended.", dir_.string(), info.st_mode & 07777);
    }
}

std::optional<State> StateStore::load() const {
    const int raw = ::open(file_.c_str(), O_RDONLY | O_CLOEXEC);
    if (raw < 0) {
        if (errno == ENOENT) return std::nullopt;
        throw InterventionError(std::format("cannot read state file {}: {}", file_.string(), errno_text()));
    }
    Fd fd(raw);
    std::string text;
    char buffer[8192];
    for (;;) {
        const ssize_t n = ::read(fd.get(), buffer, sizeof buffer);
        if (n < 0) {
            if (errno == EINTR) continue;
            throw InterventionError(std::format("cannot read state file {}: {}", file_.string(), errno_text()));
        }
        if (n == 0) break;
        text.append(buffer, static_cast<std::size_t>(n));
    }
    try {
        return from_json(text);
    } catch (const StateFormatError& e) {
        throw InterventionError(std::format(
            "state file {} is invalid: {}. Recordflow does not remove it; restore it, or remove it together "
            "with the ClickHouse target table to start over with a full snapshot",
            file_.string(), e.what()));
    }
}

void StateStore::save(const State& state) const {
    const std::filesystem::path tmp = file_.string() + ".tmp";
    const std::string text = to_json(state);
    {
        Fd fd(::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600));
        if (fd.get() < 0) {
            throw std::runtime_error(std::format("cannot create {}: {}", tmp.string(), errno_text()));
        }
        ::fchmod(fd.get(), 0600);  // an older .tmp may have had another mode
        write_all(fd.get(), text, tmp);
        if (::fsync(fd.get()) != 0) {
            throw std::runtime_error(std::format("cannot fsync {}: {}", tmp.string(), errno_text()));
        }
        if (::close(fd.release()) != 0) {
            throw std::runtime_error(std::format("cannot close {}: {}", tmp.string(), errno_text()));
        }
    }
    if (::rename(tmp.c_str(), file_.c_str()) != 0) {
        throw std::runtime_error(std::format("cannot rename {} to {}: {}", tmp.string(), file_.string(),
                                             errno_text()));
    }
    Fd dir(::open(dir_.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC));
    if (dir.get() < 0 || ::fsync(dir.get()) != 0) {
        throw std::runtime_error(std::format("cannot fsync directory {}: {}", dir_.string(), errno_text()));
    }
}

}  // namespace recordflow::state
