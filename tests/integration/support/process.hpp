#pragma once

#include <sys/types.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <optional>
#include <string>

namespace recordflow::it {

// The recordflow binary started as a child process: stdout is discarded,
// stderr (the log) goes to `log_file`.
class RecordflowProcess {
public:
    // `env` is added to the test's environment, from which every RECORDFLOW_*
    // and PG* variable is removed first.
    RecordflowProcess(const std::map<std::string, std::string>& env, std::filesystem::path log_file);
    ~RecordflowProcess();  // SIGKILL if it is still running
    RecordflowProcess(const RecordflowProcess&) = delete;
    RecordflowProcess& operator=(const RecordflowProcess&) = delete;

    // Waits until the log contains `text`; false on timeout or if the process exits first.
    bool wait_for_log(const std::string& text, std::chrono::milliseconds timeout);

    void signal(int signal);
    void kill_hard() { signal(9); }

    // The exit code (128 + signal number if a signal ended it), or nullopt if
    // it is still running after `timeout`.
    std::optional<int> wait_exit(std::chrono::milliseconds timeout);

    bool running();
    std::string log() const;
    pid_t pid() const { return pid_; }

private:
    pid_t pid_ = -1;
    std::optional<int> exit_code_;
    std::filesystem::path log_file_;
};

}  // namespace recordflow::it
