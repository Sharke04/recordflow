#include "support/process.hpp"

#include <fcntl.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>

extern char** environ;

namespace recordflow::it {

namespace {

int decode_status(int status) {
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return -1;
}

}  // namespace

RecordflowProcess::RecordflowProcess(const std::map<std::string, std::string>& env,
                                     std::filesystem::path log_file)
    : log_file_(std::move(log_file)) {
    std::vector<std::string> entries;
    for (char** item = environ; *item != nullptr; ++item) {
        const std::string_view entry = *item;
        if (entry.starts_with("RECORDFLOW_") || entry.starts_with("PG")) continue;
        entries.emplace_back(entry);
    }
    for (const auto& [key, value] : env) entries.push_back(key + "=" + value);
    std::vector<char*> envp;
    for (auto& entry : entries) envp.push_back(entry.data());
    envp.push_back(nullptr);

    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null", O_RDONLY, 0);
    posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_addopen(&actions, STDERR_FILENO, log_file_.c_str(), O_WRONLY | O_CREAT | O_APPEND,
                                     0600);
    std::string binary = RECORDFLOW_BINARY;
    char* argv[] = {binary.data(), nullptr};
    const int error = posix_spawn(&pid_, binary.c_str(), &actions, nullptr, argv, envp.data());
    posix_spawn_file_actions_destroy(&actions);
    if (error != 0) throw std::runtime_error("cannot start " + binary + ": " + std::strerror(error));
}

RecordflowProcess::~RecordflowProcess() {
    if (running()) {
        ::kill(pid_, SIGKILL);
        wait_exit(std::chrono::seconds(5));
    }
}

bool RecordflowProcess::running() {
    if (exit_code_) return false;
    int status = 0;
    const pid_t done = ::waitpid(pid_, &status, WNOHANG);
    if (done == pid_) {
        exit_code_ = decode_status(status);
        return false;
    }
    return true;
}

std::optional<int> RecordflowProcess::wait_exit(std::chrono::milliseconds timeout) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (running()) {
        if (std::chrono::steady_clock::now() >= until) return std::nullopt;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return exit_code_;
}

void RecordflowProcess::signal(int signal) {
    if (running()) ::kill(pid_, signal);
}

std::string RecordflowProcess::log() const {
    std::ifstream in(log_file_);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

bool RecordflowProcess::wait_for_log(const std::string& text, std::chrono::milliseconds timeout) {
    const auto until = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < until) {
        if (log().find(text) != std::string::npos) return true;
        if (!running()) return log().find(text) != std::string::npos;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

}  // namespace recordflow::it
