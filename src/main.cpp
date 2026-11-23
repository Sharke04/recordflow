#include <cstdio>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "app/engine.hpp"
#include "config/config_loader.hpp"
#include "config/describe.hpp"
#include "runtime/errors.hpp"
#include "runtime/log.hpp"
#include "runtime/signals.hpp"

namespace {

constexpr const char* kUsage =
    "usage: recordflow [--config PATH] [--check-config]\n"
    "\n"
    "  --config PATH    TOML configuration file (default: ./recordflow.toml if it exists;\n"
    "                   otherwise the configuration comes from RECORDFLOW_* variables)\n"
    "  --check-config   load and validate the configuration, print it and exit\n"
    "  -h, --help       show this help\n";

struct CliOptions {
    std::optional<std::string> config_path;
    bool check_only = false;
    bool help = false;
};

CliOptions parse_cli(int argc, char** argv) {
    CliOptions options;
    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        if (arg == "--config") {
            if (i + 1 >= argc) throw recordflow::ConfigError("--config needs a path");
            options.config_path = argv[++i];
        } else if (arg.starts_with("--config=")) {
            options.config_path = std::string(arg.substr(9));
        } else if (arg == "--check-config") {
            options.check_only = true;
        } else if (arg == "-h" || arg == "--help") {
            options.help = true;
        } else {
            throw recordflow::ConfigError("unknown argument '" + std::string(arg) +
                                          "' (see --help)");
        }
    }
    return options;
}

}  // namespace

int main(int argc, char** argv) {
    using recordflow::ExitCode;
    namespace config = recordflow::config;
    namespace log = recordflow::log;

    try {
        const CliOptions options = parse_cli(argc, argv);
        if (options.help) {
            std::fputs(kUsage, stdout);
            return static_cast<int>(ExitCode::Ok);
        }

        config::LoadOptions load_options;
        load_options.config_path = options.config_path;
        config::Config cfg = config::load(load_options);
        log::set_level(cfg.log.level.value);

        if (options.check_only) {
            std::fprintf(stderr, "%s\n", config::describe(cfg).c_str());
            return static_cast<int>(ExitCode::Ok);
        }
        log::info("{}", config::describe(cfg));

        recordflow::runtime::install_signal_handlers();
        recordflow::app::Engine engine(std::move(cfg));
        return engine.run();
    } catch (const std::exception& e) {
        const ExitCode code = recordflow::exit_code_for(e);
        log::error("{} (exit code {})", e.what(), static_cast<int>(code));
        return static_cast<int>(code);
    }
}
