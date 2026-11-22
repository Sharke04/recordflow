#pragma once

#include <string>

namespace recordflow::app {

// Owns all components and runs the main loop (decision 9). For now it streams
// one slot and prints the decoded events.
class Engine {
public:
    Engine(std::string slot, std::string publication);

    // Returns the process exit code; errors propagate as exceptions from
    // runtime/errors.hpp.
    int run();

private:
    std::string slot_;
    std::string publication_;
};

}  // namespace recordflow::app
