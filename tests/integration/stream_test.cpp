// [I2] The CDC stream end to end (decisions 2.2, 2.5 and 4): inserts, updates,
// deletes, an update that changes the primary key, TRUNCATE and new inserts
// after it leave <target>_current equal to the source.

#include <signal.h>

#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;

TEST(Stream, ChangesReachClickHouse) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table;
    auto process = table.start_streaming();
    PgAdmin& pg = table.pg();
    const std::string t = table.qualified();

    pg.exec("INSERT INTO " + t + " SELECT g, 'name ' || g, g * 1.25 FROM generate_series(1, 200) g");
    pg.exec("UPDATE " + t + " SET name = NULL, amount = amount + 0.5 WHERE id % 10 = 0");
    pg.exec("DELETE FROM " + t + " WHERE id BETWEEN 150 AND 170");
    // Several changes of one key inside one transaction: the last one wins.
    pg.exec("BEGIN; UPDATE " + t + " SET name = 'first' WHERE id = 1; UPDATE " + t +
            " SET name = 'second' WHERE id = 1; COMMIT");
    // A key change: a delete of the old key plus the new row (decision 2.5).
    pg.exec("UPDATE " + t + " SET id = 1000 WHERE id = 2");
    EXPECT_TRUE(target_equals_source(table));

    // TRUNCATE at its stream position: rows of the same batch before it are
    // gone, rows after it stay.
    pg.exec("BEGIN; INSERT INTO " + t + " VALUES (5000, 'before truncate', 1); COMMIT");
    pg.exec("TRUNCATE " + t);
    pg.exec("INSERT INTO " + t + " VALUES (1, 'after truncate', 2.5), (7, 'seven', 7)");
    EXPECT_TRUE(target_equals_source(table));

    process->signal(SIGTERM);
    EXPECT_EQ(process->wait_exit(10s), 0) << process->log();
}

}  // namespace
