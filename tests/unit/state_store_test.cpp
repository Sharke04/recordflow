// [U11] State file (decisions 4.2 and 10.4): every field survives a write and
// a read, a half-written .tmp never damages the stored file, and a damaged or
// unknown file stops the start (exit code 4) instead of being replaced.

#include <gtest/gtest.h>

#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "runtime/errors.hpp"
#include "state/state_store.hpp"

namespace {

using namespace recordflow;
namespace fs = std::filesystem;

class StateStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        dir_ = fs::temp_directory_path() /
               ("recordflow_state_" + std::to_string(::getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name()) / "data";
        fs::remove_all(dir_.parent_path());
    }
    void TearDown() override { fs::remove_all(dir_.parent_path()); }

    static state::State sample() {
        state::State s;
        s.source = {"7301845123456789012", "recordflow_slot", "public.orders"};
        s.target = {"recordflow.public__orders", "5b1f0c2e-1111-2222-3333-444455556666"};
        s.last_flushed_lsn = 0x16B3748;
        s.snapshot.status = state::SnapshotStatus::Running;
        s.snapshot.start_lsn = 0x16A0000;
        s.snapshot.max_pk = std::vector<std::string>{"250000", "eu"};
        s.snapshot.last_pk = {"1042", "us"};
        s.snapshot.version_lsn = (std::uint64_t{0x1F} << 32) | 0x16B3000;
        return s;
    }

    static void write_file(const fs::path& path, const std::string& text) {
        std::ofstream(path, std::ios::binary | std::ios::trunc) << text;
    }

    static mode_t mode_of(const fs::path& path) {
        struct stat info {};
        ::stat(path.c_str(), &info);
        return info.st_mode & 0777;
    }

    fs::path dir_;
};

TEST_F(StateStoreTest, RoundTripsEveryFieldWithPrivateModes) {
    const state::StateStore store(dir_);
    store.prepare_directory();
    EXPECT_EQ(mode_of(dir_), 0700u);
    EXPECT_FALSE(store.load().has_value());  // first start

    const state::State written = sample();
    store.save(written);
    EXPECT_EQ(mode_of(store.file()), 0600u);
    EXPECT_FALSE(fs::exists(store.file().string() + ".tmp"));

    const auto read = store.load();
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(read->format_version, 1);
    EXPECT_EQ(read->source.system_id, written.source.system_id);
    EXPECT_EQ(read->source.slot, written.source.slot);
    EXPECT_EQ(read->source.table, written.source.table);
    EXPECT_EQ(read->target.table, written.target.table);
    EXPECT_EQ(read->target.table_uuid, written.target.table_uuid);
    EXPECT_EQ(read->last_flushed_lsn, written.last_flushed_lsn);
    EXPECT_EQ(read->snapshot, written.snapshot);

    // A fresh snapshot: nulls for what the first chunk sets; an empty table
    // keeps max_pk null once start_lsn is set.
    state::State pending = written;
    pending.snapshot = {};
    store.save(pending);
    EXPECT_EQ(store.load()->snapshot, pending.snapshot);
    pending.snapshot.status = state::SnapshotStatus::Done;
    pending.snapshot.start_lsn = 0x100;
    store.save(pending);
    const auto done = store.load();
    EXPECT_EQ(done->snapshot, pending.snapshot);
    EXPECT_FALSE(done->snapshot.max_pk.has_value());
}

TEST_F(StateStoreTest, HalfWrittenTmpLeavesTheStoredFileIntact) {
    const state::StateStore store(dir_);
    store.prepare_directory();
    store.save(sample());

    // A crash between writing .tmp and the rename leaves a partial .tmp behind.
    write_file(store.file().string() + ".tmp", R"({"format_version": 1, "source": {"system_id": "73)");
    const auto read = store.load();
    ASSERT_TRUE(read.has_value());
    EXPECT_EQ(read->last_flushed_lsn, sample().last_flushed_lsn);

    // The next save replaces the leftover.
    state::State next = sample();
    next.last_flushed_lsn = 0x2000000;
    store.save(next);
    EXPECT_EQ(store.load()->last_flushed_lsn, 0x2000000u);
    EXPECT_FALSE(fs::exists(store.file().string() + ".tmp"));
}

TEST_F(StateStoreTest, DamagedOrUnknownFilesAreRefusedAndKept) {
    const state::StateStore store(dir_);
    store.prepare_directory();

    write_file(store.file(), "{ not json");
    EXPECT_THROW(store.load(), InterventionError);
    EXPECT_TRUE(fs::exists(store.file()));

    std::string text = state::to_json(sample());
    text.replace(text.find("\"format_version\": 1"), 19, "\"format_version\": 2");
    write_file(store.file(), text);
    try {
        store.load();
        FAIL() << "format_version 2 was accepted";
    } catch (const InterventionError& e) {
        EXPECT_NE(std::string(e.what()).find("format_version"), std::string::npos) << e.what();
    }

    text = state::to_json(sample());
    text.replace(text.find("\"0/16B3748\""), 11, "\"16B3748\"");
    write_file(store.file(), text);
    EXPECT_THROW(store.load(), InterventionError);
}

}  // namespace
