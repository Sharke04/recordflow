#include <cstdio>
#include <exception>

#include "app/engine.hpp"
#include "runtime/errors.hpp"
#include "runtime/log.hpp"

int main(int argc, char** argv) {
    using recordflow::ExitCode;

    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <slot> <publication>\n", argv[0]);
        std::fprintf(stderr,
                     "  the slot and publication must already exist on the "
                     "source database.\n");
        return static_cast<int>(ExitCode::Config);
    }

    try {
        recordflow::app::Engine engine(argv[1], argv[2]);
        return engine.run();
    } catch (const std::exception& e) {
        const ExitCode code = recordflow::exit_code_for(e);
        recordflow::log::error("{} (exit code {})", e.what(), static_cast<int>(code));
        return static_cast<int>(code);
    }
}
