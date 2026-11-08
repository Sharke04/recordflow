#pragma once

#include "config/config.hpp"

namespace recordflow::app {

// Owns all components and runs the main loop (decision 9). For now it streams
// one slot and prints the decoded events.
class Engine {
public:
    explicit Engine(config::Config config);

    // Returns the process exit code; errors propagate as exceptions from
    // runtime/errors.hpp.
    int run();

private:
    config::Config config_;
};

}  // namespace recordflow::app
