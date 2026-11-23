#pragma once

#include <chrono>

// SIGTERM and SIGINT (decision 8.4). The handler only sets a flag; the main
// loop checks it on every round (at least every 200 ms).
namespace recordflow::runtime {

// The first signal requests a graceful shutdown; a second one ends the
// process at once with exit code 0 (everything is idempotent: what is not
// written waits in the slot, what is not confirmed is replayed).
void install_signal_handlers();

bool shutdown_requested();

// Ends the process with exit code 0 after `timeout`, wherever it is then
// (the shutdown deadline, shutdown.timeout_s).
void arm_shutdown_deadline(std::chrono::seconds timeout);

// Sleeps for `duration` in short steps. Returns false as soon as a shutdown
// is requested, true after the full duration.
bool sleep_unless_shutdown(std::chrono::milliseconds duration);

}  // namespace recordflow::runtime
