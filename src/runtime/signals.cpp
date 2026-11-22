#include "runtime/signals.hpp"

#include <signal.h>
#include <unistd.h>

#include <algorithm>
#include <csignal>
#include <thread>

namespace recordflow::runtime {

namespace {

volatile std::sig_atomic_t g_shutdown = 0;

void write_stderr(const char* text, std::size_t size) {
    // write() is async-signal-safe; the logger is not.
    [[maybe_unused]] const ssize_t ignored = ::write(STDERR_FILENO, text, size);
}

extern "C" void on_stop_signal(int) {
    if (g_shutdown != 0) {
        static constexpr char kMessage[] = "second stop signal: exiting at once\n";
        write_stderr(kMessage, sizeof kMessage - 1);
        ::_exit(0);
    }
    g_shutdown = 1;
}

extern "C" void on_deadline(int) {
    static constexpr char kMessage[] = "shutdown deadline (shutdown.timeout_s) passed: exiting at once\n";
    write_stderr(kMessage, sizeof kMessage - 1);
    ::_exit(0);
}

void install(int signal, void (*handler)(int)) {
    struct sigaction action {};
    action.sa_handler = handler;
    sigemptyset(&action.sa_mask);
    action.sa_flags = 0;  // no SA_RESTART: a blocking poll() returns EINTR
    ::sigaction(signal, &action, nullptr);
}

}  // namespace

void install_signal_handlers() {
    install(SIGTERM, on_stop_signal);
    install(SIGINT, on_stop_signal);
}

bool shutdown_requested() {
    return g_shutdown != 0;
}

void arm_shutdown_deadline(std::chrono::seconds timeout) {
    install(SIGALRM, on_deadline);
    ::alarm(static_cast<unsigned>(std::max<std::chrono::seconds::rep>(timeout.count(), 1)));
}

bool sleep_unless_shutdown(std::chrono::milliseconds duration) {
    constexpr std::chrono::milliseconds kStep{100};
    const auto until = std::chrono::steady_clock::now() + duration;
    while (!shutdown_requested()) {
        const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(until - std::chrono::steady_clock::now());
        if (left <= std::chrono::milliseconds::zero()) return true;
        std::this_thread::sleep_for(std::min(left, kStep));
    }
    return false;
}

}  // namespace recordflow::runtime
