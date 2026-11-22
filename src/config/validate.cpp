#include "config/validate.hpp"

#include <format>
#include <string>
#include <vector>

#include "config/fields.hpp"
#include "runtime/errors.hpp"

namespace recordflow::config {

namespace {

class Checker {
public:
    explicit Checker(const Config& config) : config_(config) {}

    void required(std::string_view section, std::string_view key) {
        const Field& f = field(section, key);
        if (f.show(config_).empty()) {
            problems_.push_back(std::format("{} is required (set it in [{}] or {})", f.name(),
                                            section, f.env_name()));
        }
    }

    void non_empty(std::string_view section, std::string_view key) {
        const Field& f = field(section, key);
        if (f.show(config_).empty()) {
            problems_.push_back(std::format("{} must not be empty ({})", f.name(), origin(f)));
        }
    }

    void at_least(std::string_view section, std::string_view key, int value, int minimum) {
        if (value < minimum) {
            const Field& f = field(section, key);
            problems_.push_back(std::format("{} must be at least {}, got {} ({})", f.name(), minimum,
                                            value, origin(f)));
        }
    }

    void between(std::string_view section, std::string_view key, int value, int low, int high) {
        if (value < low || value > high) {
            const Field& f = field(section, key);
            problems_.push_back(std::format("{} must be between {} and {}, got {} ({})", f.name(),
                                            low, high, value, origin(f)));
        }
    }

    void qualified_table(std::string_view section, std::string_view key) {
        const Field& f = field(section, key);
        const std::string value = f.show(config_);
        if (value.empty()) return;  // reported by required()
        const auto dot = value.find('.');
        const bool ok = dot != std::string::npos && dot > 0 && dot + 1 < value.size() &&
                        value.find('.', dot + 1) == std::string::npos;
        if (!ok) {
            problems_.push_back(std::format("{} must be 'schema.table', got '{}' ({})", f.name(),
                                            value, origin(f)));
        }
    }

    void add(std::string problem) { problems_.push_back(std::move(problem)); }

    void report() const {
        if (problems_.empty()) return;
        std::string message = "invalid configuration:";
        for (const auto& problem : problems_) message += "\n  - " + problem;
        throw ConfigError(message);
    }

private:
    static const Field& field(std::string_view section, std::string_view key) {
        return *find_field(section, key);
    }

    std::string origin(const Field& f) const {
        const Source source = f.source(config_);
        if (source == Source::Env) return "from " + f.env_name();
        return std::string("from ") + source_name(source);
    }

    const Config& config_;
    std::vector<std::string> problems_;
};

}  // namespace

void validate(const Config& c) {
    Checker check(c);

    check.required("source", "conninfo");
    check.required("source", "slot");
    check.required("source", "publication");
    check.required("source", "table");
    check.qualified_table("source", "table");

    if (c.output.sink.value == SinkKind::ClickHouse) {
        check.non_empty("clickhouse", "host");
        check.between("clickhouse", "port", c.clickhouse.port.value, 1, 65535);
        check.non_empty("clickhouse", "database");
        check.non_empty("clickhouse", "user");
    }

    check.non_empty("state", "data_dir");

    check.at_least("batch", "max_rows", c.batch.max_rows.value, 1);
    check.at_least("batch", "max_mb", c.batch.max_mb.value, 1);
    check.at_least("batch", "max_delay_ms", c.batch.max_delay_ms.value, 0);

    check.at_least("snapshot", "chunk_size", c.snapshot.chunk_size.value, 1);
    check.at_least("snapshot", "chunk_delay_ms", c.snapshot.chunk_delay_ms.value, 0);
    check.at_least("snapshot", "window_timeout_s", c.snapshot.window_timeout_s.value, 1);

    check.at_least("retry", "initial_ms", c.retry.initial_ms.value, 1);
    check.at_least("retry", "max_ms", c.retry.max_ms.value, c.retry.initial_ms.value);
    check.at_least("retry", "slot_busy_timeout_s", c.retry.slot_busy_timeout_s.value, 1);

    check.at_least("shutdown", "timeout_s", c.shutdown.timeout_s.value, 1);

    check.report();
}

}  // namespace recordflow::config
