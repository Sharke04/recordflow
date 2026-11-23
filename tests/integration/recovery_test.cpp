// [I3] Recovery after a hard kill (decision 2.2): with writes running on the
// source, SIGKILL stops recordflow at an arbitrary point; after a restart the
// replay is idempotent and <target>_current settles equal to the source.

#include <signal.h>

#include <atomic>
#include <string>
#include <thread>

#include "support/stream_table.hpp"

namespace {

using namespace recordflow::it;
using namespace std::chrono_literals;

TEST(Recovery, HardKillAndRestartConverge) {
    RECORDFLOW_REQUIRE_INTEGRATION();
    StreamTable table;
    const std::string t = table.qualified();

    std::atomic<bool> stop{false};
    std::thread writer([&] {
        PgAdmin pg;  // its own connection
        for (int i = 1; !stop; ++i) {
            pg.exec("BEGIN; INSERT INTO " + t + " VALUES (" + std::to_string(i) + ", 'w" + std::to_string(i) +
                    "', " + std::to_string(i) + "); UPDATE " + t + " SET amount = amount + 1 WHERE id = " +
                    std::to_string(1 + (i * 37 + 11) % i) + "; " + (i % 5 == 0 ? "DELETE FROM " + t +
                    " WHERE id = " + std::to_string(i - 3) + "; " : "") + "COMMIT");
            std::this_thread::sleep_for(5ms);
        }
    });

    auto first = table.start_streaming();
    std::this_thread::sleep_for(1500ms);
    first->kill_hard();
    EXPECT_EQ(first->wait_exit(10s), 128 + SIGKILL);

    // The new process may wait for the old walsender to release the slot.
    auto second = table.start_streaming();
    std::this_thread::sleep_for(1500ms);
    stop = true;
    writer.join();

    EXPECT_TRUE(target_equals_source(table, 30s));
    second->signal(SIGTERM);
    EXPECT_EQ(second->wait_exit(10s), 0) << second->log();
}

}  // namespace
