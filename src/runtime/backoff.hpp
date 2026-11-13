#pragma once

#include <chrono>
#include <random>
#include <string>
#include <string_view>

#include "config/config.hpp"

// Exponential backoff with jitter for reconnects and retried writes
// (decision 8.3). Retries are unlimited; the caller waits for the returned delay.
namespace recordflow::runtime {

class Backoff {
public:
    using Clock = std::chrono::steady_clock;

    // `what` names the operation in log lines, e.g. "ClickHouse insert".
    Backoff(std::string what, std::chrono::milliseconds initial, std::chrono::milliseconds max);
    Backoff(std::string what, const config::RetrySection& retry);

    // Records a failed attempt and returns the delay before the next one:
    // doubling from `initial` up to `max`, each with ±20% jitter. Logs a WARN
    // for each of the first 5 attempts, then one per minute with the count.
    std::chrono::milliseconds failure(std::string_view reason, Clock::time_point now = Clock::now());

    // After a success: logs the outage duration if attempts had failed and
    // starts again from `initial`.
    void success(Clock::time_point now = Clock::now());

    int attempts() const { return attempts_; }

    static constexpr int kVerboseAttempts = 5;
    static constexpr std::chrono::minutes kQuietLogInterval{1};
    static constexpr double kJitter = 0.2;

private:
    std::string what_;
    std::chrono::milliseconds initial_;
    std::chrono::milliseconds max_;
    std::chrono::milliseconds next_;
    int attempts_ = 0;
    Clock::time_point first_failure_{};
    Clock::time_point last_log_{};
    std::mt19937_64 random_;
};

}  // namespace recordflow::runtime
