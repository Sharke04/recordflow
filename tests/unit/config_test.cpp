#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <map>
#include <string>

#include "config/config_loader.hpp"
#include "runtime/errors.hpp"

namespace {

using namespace recordflow::config;
using recordflow::ConfigError;

EnvLookup fake_env(std::map<std::string, std::string> vars) {
    return [vars](const std::string& name) -> std::optional<std::string> {
        const auto it = vars.find(name);
        if (it == vars.end()) return std::nullopt;
        return it->second;
    };
}

std::filesystem::path write_temp(const std::string& name, const std::string& content) {
    const auto path = std::filesystem::temp_directory_path() / ("recordflow_test_" + name);
    std::ofstream(path, std::ios::binary) << content;
    return path;
}

const std::string kRequired =
    "[source]\n"
    "conninfo = \"host=localhost\"\n"
    "slot = \"slot_from_file\"\n"
    "publication = \"pub\"\n"
    "table = \"public.orders\"\n";

std::string error_of(const std::function<void()>& action) {
    try {
        action();
    } catch (const ConfigError& e) {
        return e.what();
    }
    return "no error";
}

}  // namespace

// [U3] defaults < file < environment, the *_FILE variant, and environment-only setups.
TEST(Config, LayersDefaultsFileAndEnvironment) {
    const auto file = write_temp("layers.toml", kRequired + "[batch]\nmax_rows = 10\nmax_mb = 5\n");
    const auto secret = write_temp("secret", "s3cret\r\n");

    LoadOptions options;
    options.config_path = file.string();
    options.env = fake_env({{"RECORDFLOW_BATCH_MAX_ROWS", "77"},
                            {"RECORDFLOW_CLICKHOUSE_PASSWORD_FILE", secret.string()}});
    const Config config = load(options);

    EXPECT_EQ(config.source.slot.value, "slot_from_file");
    EXPECT_EQ(config.source.slot.source, Source::File);
    EXPECT_EQ(config.batch.max_rows.value, 77);
    EXPECT_EQ(config.batch.max_rows.source, Source::Env);
    EXPECT_EQ(config.batch.max_mb.value, 5);
    EXPECT_EQ(config.batch.max_delay_ms.value, 1000);
    EXPECT_EQ(config.batch.max_delay_ms.source, Source::Default);
    EXPECT_EQ(config.clickhouse.password.value, "s3cret");
    EXPECT_EQ(config.clickhouse.password.source, Source::Env);

    // Both variants of one key are ambiguous.
    options.env = fake_env({{"RECORDFLOW_CLICKHOUSE_PASSWORD", "a"},
                            {"RECORDFLOW_CLICKHOUSE_PASSWORD_FILE", secret.string()}});
    EXPECT_NE(error_of([&] { load(options); }).find("not both"), std::string::npos);

    // No --config and no default file: the environment alone.
    LoadOptions env_only;
    env_only.default_path = (std::filesystem::temp_directory_path() / "recordflow_absent.toml").string();
    env_only.env = fake_env({{"RECORDFLOW_SOURCE_CONNINFO", "host=x"},
                             {"RECORDFLOW_SOURCE_SLOT", "s"},
                             {"RECORDFLOW_SOURCE_PUBLICATION", "p"},
                             {"RECORDFLOW_SOURCE_TABLE", "a.b"},
                             {"RECORDFLOW_OUTPUT_SINK", "jsonl"}});
    const Config from_env = load(env_only);
    EXPECT_EQ(from_env.output.sink.value, SinkKind::Jsonl);
    EXPECT_EQ(from_env.source.table.source, Source::Env);

    std::filesystem::remove(file);
    std::filesystem::remove(secret);
}

// [U4] strict checks: unknown keys with their line, secrets in the file, invalid values.
TEST(Config, RejectsUnknownKeysSecretsAndInvalidValues) {
    Config config;
    const std::string typo = error_of([&] {
        apply_toml(config, kRequired + "\n[snapshot]\nchunk_sise = 5\n", "recordflow.toml");
    });
    EXPECT_NE(typo.find("recordflow.toml:8:"), std::string::npos) << typo;
    EXPECT_NE(typo.find("snapshot.chunk_sise"), std::string::npos) << typo;

    const std::string password = error_of([&] {
        apply_toml(config, "[clickhouse]\npassword = \"x\"\n", "recordflow.toml");
    });
    EXPECT_NE(password.find("RECORDFLOW_CLICKHOUSE_PASSWORD"), std::string::npos) << password;

    const std::string type = error_of([&] {
        apply_toml(config, "[clickhouse]\nport = \"9000\"\n", "recordflow.toml");
    });
    EXPECT_NE(type.find("expected an integer"), std::string::npos) << type;

    const auto file = write_temp("invalid.toml", "[source]\nconninfo = \"host=x\"\nslot = \"s\"\n"
                                                 "publication = \"p\"\ntable = \"orders\"\n");
    LoadOptions options;
    options.config_path = file.string();
    options.env = fake_env({{"RECORDFLOW_SNAPSHOT_CHUNK_SIZE", "0"}, {"RECORDFLOW_OUTPUT_SINK", "jsonl"}});
    const std::string invalid = error_of([&] { load(options); });
    EXPECT_NE(invalid.find("source.table must be 'schema.table'"), std::string::npos) << invalid;
    EXPECT_NE(invalid.find("snapshot.chunk_size must be at least 1, got 0 (from RECORDFLOW_SNAPSHOT_CHUNK_SIZE)"),
              std::string::npos)
        << invalid;

    options.env = fake_env({{"RECORDFLOW_OUTPUT_SINK", "kafka"}});
    EXPECT_NE(error_of([&] { load(options); }).find("expected clickhouse | jsonl"), std::string::npos);

    std::filesystem::remove(file);
}
