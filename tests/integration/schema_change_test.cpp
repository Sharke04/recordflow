// [I4] The schema change stop (decisions 7.3 and 7.4): ALTER TABLE ADD COLUMN
// followed by an INSERT ends recordflow with exit code 3; every row before the
// change is in ClickHouse, the slot stays before the change, and a restart
// stops again with exit code 3.

#include <string>

#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;

TEST(SchemaChange, StopsWithExitCodeThree) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table;
    PgAdmin& pg = table.pg();
    const std::string t = table.qualified();

    auto process = table.start_streaming();
    pg.exec("INSERT INTO " + t + " SELECT g, 'n' || g, g FROM generate_series(1, 50) g");
    ASSERT_TRUE(target_equals_source(table));
    const auto before_change = table.source_rows();

    pg.exec("ALTER TABLE " + t + " ADD COLUMN discount numeric");
    const std::string change_lsn = pg.query("SELECT pg_current_wal_lsn()")[0][0];
    pg.exec("INSERT INTO " + t + " VALUES (51, 'with discount', 1, 0.5)");

    EXPECT_EQ(process->wait_exit(20s), 3) << process->log();
    const std::string log = process->log();
    EXPECT_NE(log.find("detected at: Relation message"), std::string::npos) << log;
    EXPECT_NE(log.find("+ new column \"discount\""), std::string::npos) << log;
    EXPECT_NE(log.find("pg_replication_slot_advance('" + table.slot() + "'"), std::string::npos) << log;

    // Everything before the change is stored and confirmed, nothing after it.
    EXPECT_EQ(table.target_rows(), before_change);
    const std::string confirmed = table.slot_confirmed_lsn();
    EXPECT_EQ(table.state_last_flushed_lsn(), confirmed);
    EXPECT_EQ(pg.query("SELECT '" + confirmed + "'::pg_lsn <= '" + change_lsn + "'::pg_lsn")[0][0], "t")
        << "slot confirmed at " << confirmed << ", the INSERT after the change starts after " << change_lsn;

    // Without intervention the next start stops again, now at startup.
    auto again = table.start();
    EXPECT_EQ(again->wait_exit(30s), 3) << again->log();
    EXPECT_NE(again->log().find("detected at: startup"), std::string::npos) << again->log();
    EXPECT_EQ(table.slot_confirmed_lsn(), confirmed);
}

}  // namespace
