#include "runtime/backoff.hpp"

#include <algorithm>
#include <utility>

#include "runtime/log.hpp"

namespace recordflow::runtime {

namespace {

double seconds(std::chrono::nanoseconds d) {
    return std::chrono::duration<double>(d).count();
}

}  // namespace

Backoff::Backoff(std::string what, std::chrono::milliseconds initial, std::chrono::milliseconds max)
    : what_(std::move(what)),
      initial_(initial),
      max_(std::max(initial, max)),
      next_(initial),
      random_(std::random_device{}()) {}

Backoff::Backoff(std::string what, const config::RetrySection& retry)
    : Backoff(std::move(what), std::chrono::milliseconds(retry.initial_ms.value),
              std::chrono::milliseconds(retry.max_ms.value)) {}

std::chrono::milliseconds Backoff::failure(std::string_view reason, Clock::time_point now) {
    if (attempts_ == 0) first_failure_ = now;
    ++attempts_;

    std::uniform_real_distribution<double> jitter(1.0 - kJitter, 1.0 + kJitter);
    const auto delay = std::chrono::milliseconds(
        static_cast<std::chrono::milliseconds::rep>(static_cast<double>(next_.count()) * jitter(random_)));
    next_ = std::min(next_ * 2, max_);

    if (attempts_ <= kVerboseAttempts) {
        log::warn("{} failed (attempt {}): {}; retrying in {:.1f} s", what_, attempts_, reason, seconds(delay));
        last_log_ = now;
    } else if (now - last_log_ >= kQuietLogInterval) {
        log::warn("{} still failing after {} attempts ({:.0f} s): {}; retrying in {:.1f} s", what_, attempts_,
                  seconds(now - first_failure_), reason, seconds(delay));
        last_log_ = now;
    }
    return delay;
}

void Backoff::success(Clock::time_point now) {
    if (attempts_ > 0) {
        log::info("{} recovered after {:.1f} s ({} failed attempts)", what_, seconds(now - first_failure_),
                  attempts_);
    }
    attempts_ = 0;
    next_ = initial_;
}

}  // namespace recordflow::runtime
