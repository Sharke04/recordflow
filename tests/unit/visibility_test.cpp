// [U14] Snapshot visibility (decision 5.4): a committed xid is visible to a
// pg_current_snapshot() if it is below xmin, or below xmax and not in xip; the
// 32-bit xid from pgoutput gets its epoch from xmax, also across an epoch
// boundary.

#include <gtest/gtest.h>

#include "snapshot/visibility.hpp"

namespace {

using namespace recordflow;
using snapshot::FullXid;

constexpr FullXid kEpoch = FullXid{1} << 32;

snapshot::PgSnapshot parsed(const char* text) {
    const auto snapshot = snapshot::parse_snapshot(text);
    EXPECT_TRUE(snapshot) << text;
    return snapshot.value_or(snapshot::PgSnapshot{});
}

TEST(Visibility, ParsesPgCurrentSnapshot) {
    const snapshot::PgSnapshot plain = parsed("100:105:");
    EXPECT_EQ(plain.xmin, 100u);
    EXPECT_EQ(plain.xmax, 105u);
    EXPECT_TRUE(plain.xip.empty());

    const snapshot::PgSnapshot busy = parsed("100:110:104,101");
    EXPECT_EQ(busy.xip, (std::vector<FullXid>{101, 104}));

    const snapshot::PgSnapshot wide = parsed("8589934600:8589934610:8589934605");
    EXPECT_EQ(wide.xmax, 2 * kEpoch + 18);
    EXPECT_EQ(wide.xmin32(), 8u);

    for (const char* bad : {"", "100", "100:105", "105:100:", "a:105:", "100:105:1,", "100:105:,1"}) {
        EXPECT_FALSE(snapshot::parse_snapshot(bad)) << bad;
    }
}

TEST(Visibility, BelowXminIsVisible) {
    const snapshot::PgSnapshot snapshot = parsed("100:110:104");
    EXPECT_TRUE(snapshot::is_visible(snapshot, 50));
    EXPECT_TRUE(snapshot::is_visible(snapshot, 99));
}

TEST(Visibility, InProgressIsNotVisible) {
    const snapshot::PgSnapshot snapshot = parsed("100:110:100,104");
    EXPECT_FALSE(snapshot::is_visible(snapshot, 100));
    EXPECT_FALSE(snapshot::is_visible(snapshot, 104));
    EXPECT_TRUE(snapshot::is_visible(snapshot, 101));
    EXPECT_TRUE(snapshot::is_visible(snapshot, 109));
}

TEST(Visibility, AtOrAboveXmaxIsNotVisible) {
    const snapshot::PgSnapshot snapshot = parsed("100:110:");
    EXPECT_FALSE(snapshot::is_visible(snapshot, 110));
    EXPECT_FALSE(snapshot::is_visible(snapshot, 111));
}

TEST(Visibility, EpochComesFromXmax) {
    EXPECT_EQ(snapshot::widen_xid(5, 3 * kEpoch + 10), 3 * kEpoch + 5);
    EXPECT_EQ(snapshot::widen_xid(20, 3 * kEpoch + 10), 3 * kEpoch + 20);
    // xmax just past an epoch boundary: an xid near 2^32 belongs to the previous epoch.
    EXPECT_EQ(snapshot::widen_xid(0xFFFFFFF0u, 3 * kEpoch + 5), 2 * kEpoch + 0xFFFFFFF0u);
    // xmax just before a boundary: a small xid already belongs to the next one.
    EXPECT_EQ(snapshot::widen_xid(3, 3 * kEpoch + 0xFFFFFFF0u), 4 * kEpoch + 3);
    // Nothing exists before epoch 0.
    EXPECT_EQ(snapshot::widen_xid(0xFFFFFFF0u, 10), 0xFFFFFFF0u);
}

TEST(Visibility, AcrossAnEpochBoundary) {
    // xmin in epoch 2, xmax and one running transaction in epoch 3.
    const FullXid xmin = 2 * kEpoch + 0xFFFFFFF0u;
    const FullXid xmax = 3 * kEpoch + 20;
    const snapshot::PgSnapshot snapshot =
        parsed((std::to_string(xmin) + ":" + std::to_string(xmax) + ":" + std::to_string(3 * kEpoch + 7)).c_str());
    EXPECT_TRUE(snapshot::is_visible(snapshot, 0xFFFFFFE0u));  // below xmin, epoch 2
    EXPECT_TRUE(snapshot::is_visible(snapshot, 0xFFFFFFF5u));  // between xmin and the boundary
    EXPECT_TRUE(snapshot::is_visible(snapshot, 3));            // after the boundary
    EXPECT_FALSE(snapshot::is_visible(snapshot, 7));           // in xip
    EXPECT_FALSE(snapshot::is_visible(snapshot, 20));          // xmax
}

}  // namespace
