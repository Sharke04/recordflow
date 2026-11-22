#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "pg/conninfo.hpp"
#include "runtime/errors.hpp"

namespace {

using recordflow::pg::ConnectionKind;
using recordflow::pg::KeyValues;
using recordflow::pg::build_connection_params;

std::optional<std::string> value_of(const KeyValues& params, const std::string& keyword) {
    for (const auto& [key, value] : params) {
        if (key == keyword) return value;
    }
    return std::nullopt;
}

}  // namespace

// [U5] the required GUCs are merged into `options`, defaults are added, and
// settings that would break the stream are rejected.
TEST(Conninfo, MergesOptionsAndRejectsConflicts) {
    const KeyValues replication = build_connection_params(
        "host=db user=rf options='-c work_mem=64MB -c timezone=utc'", ConnectionKind::Replication);
    EXPECT_EQ(value_of(replication, "host"), "db");
    EXPECT_EQ(value_of(replication, "replication"), "database");
    EXPECT_EQ(value_of(replication, "application_name"), "recordflow");
    EXPECT_EQ(value_of(replication, "client_encoding"), "UTF8");
    const std::string options = value_of(replication, "options").value_or("");
    EXPECT_NE(options.find("work_mem=64MB"), std::string::npos) << options;
    for (const char* guc : {"-c TimeZone=UTC", "-c DateStyle=ISO", "-c extra_float_digits=1",
                            "-c bytea_output=hex"}) {
        EXPECT_NE(options.find(guc), std::string::npos) << guc << " missing in " << options;
    }

    const KeyValues query = build_connection_params(
        "postgresql://rf@db:5432/mydb?application_name=custom", ConnectionKind::Query);
    EXPECT_EQ(value_of(query, "replication"), std::nullopt);
    EXPECT_EQ(value_of(query, "application_name"), "custom");
    EXPECT_EQ(value_of(query, "dbname"), "mydb");

    EXPECT_THROW(build_connection_params("host=db replication=database", ConnectionKind::Query),
                 recordflow::ConfigError);
    EXPECT_THROW(build_connection_params("host=db options='-c DateStyle=German'", ConnectionKind::Query),
                 recordflow::ConfigError);
    EXPECT_THROW(build_connection_params("host=db options='--extra-float-digits=3'", ConnectionKind::Query),
                 recordflow::ConfigError);
    EXPECT_THROW(build_connection_params("host=db client_encoding=LATIN1", ConnectionKind::Query),
                 recordflow::ConfigError);
    EXPECT_THROW(build_connection_params("host=db nonsense=1", ConnectionKind::Query),
                 recordflow::ConfigError);
}
