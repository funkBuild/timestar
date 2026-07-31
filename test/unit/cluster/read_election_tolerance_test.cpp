// Debt D-26 / ADR 0006: how long a READ waits for a leader, and why it is deliberately
// far less than a WRITE waits.
//
// These are compile-time constants with static_asserts of their own (cluster_data_plane.hpp);
// this file asserts the SHAPE those assertions are meant to produce, in wall clock, so the
// numbers cannot be re-tuned into agreement without someone reading ADR 0006.
#include "../../../lib/cluster/integration/cluster_data_plane.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>
#include <sstream>
#include <string>

using namespace timestar;

// ---------------------------------------------------------------------------
// Debt D-26 / ADR 0006: reads and writes tolerate a Raft election ASYMMETRICALLY, and the
// asymmetry is a decision rather than an accident of two independent constants.
//
// The compiler already enforces the three relationships (static_asserts in
// cluster_data_plane.hpp). This asserts the SHAPE those relationships are supposed to
// produce, in wall clock, so the numbers cannot be re-tuned into agreement without someone
// reading this and ADR 0006 -- and so a reader can see the ratio the ADR argues about.
TEST(ReadElectionTolerance, AReadRidesOutATransferAndDeliberatelyNotAnElection) {
    using namespace std::chrono;
    // The budget is a product of its two parts, not an independent third number.
    EXPECT_EQ(cluster::kReadLeaderlessBudget,
              cluster::kReadLeaderRetryDelay * static_cast<int64_t>(cluster::kReadLeaderRetries));
    EXPECT_EQ(cluster::kReadLeaderlessBudget, milliseconds(1200));

    // It covers the routine event (a leadership transfer, whose abandon window is D-20's
    // kRaftTransferTicks) ...
    EXPECT_GE(cluster::kReadLeaderlessBudget, cluster::raftTicksToWallClock(cluster::kRaftTransferTicks));
    // ... and NOT the exceptional one (an election, 2.5-5 s).
    EXPECT_LT(cluster::kReadLeaderlessBudget, cluster::raftTicksToWallClock(cluster::kRaftElectionTicksMin));

    // The asymmetry itself, and its DIRECTION: the write path waits out an election
    // (debt D-14) and the read path must stay the fast-failing half.
    EXPECT_LT(cluster::kReadLeaderlessBudget, data::ReplicatedBatchWriteRouter::kElectionDeadline);
    EXPECT_GE(data::ReplicatedBatchWriteRouter::kElectionDeadline / cluster::kReadLeaderlessBudget, 3)
        << "the two are meant to differ by an order of magnitude-ish; if they have converged, the decision in "
           "ADR 0006 has been undone by tuning rather than by argument";

    // NEGATIVE CONTROL for the whole block: these must be the LIVE constants, not numbers
    // this test also owns. A budget that no longer covers a transfer -- the pre-D-26 value
    // was 4 x 25 ms = 100 ms -- must fail the transfer assertion above.
    EXPECT_LT(milliseconds(100), cluster::raftTicksToWallClock(cluster::kRaftTransferTicks))
        << "the pre-D-26 budget must be demonstrably too short for the event it claimed to cover, or the "
           "assertion above proves nothing";
    // ... and the redirect budget stays SEPARATE from the wait budget (a redirect is
    // progress, not a wait), which is what stops the ordinary RF < N cold-cache round from
    // spending the election tolerance.
    EXPECT_GT(cluster::kReadRedirectRounds, 0);
    EXPECT_LT(static_cast<unsigned>(cluster::kReadRedirectRounds), cluster::kReadLeaderRetries);
}

// ... and the budget must have exactly ONE definition. It used to be two function-local
// `static constexpr`s inside queryReplicated, where no test and no assertion could see it;
// that is precisely how it drifted to 4% of an election while its own comment claimed it
// was sized for a transfer. A second definition reappearing is the regression.
TEST(ReadElectionTolerance, TheBudgetHasASingleDefinition) {
    auto readSource = [](const std::string& rel) {
        for (const std::string& prefix : {"", "../", "../../", "../../../"}) {
            std::ifstream in(prefix + rel);
            if (in.good()) {
                std::ostringstream ss;
                ss << in.rdbuf();
                return ss.str();
            }
        }
        return std::string();
    };
    const std::string hdr = readSource("lib/cluster/integration/cluster_data_plane.hpp");
    const std::string src = readSource("lib/cluster/integration/cluster_data_plane.cpp");
    ASSERT_FALSE(hdr.empty());
    ASSERT_FALSE(src.empty());

    // Defined once, in the header that can see the Raft clocks it is asserted against.
    EXPECT_NE(hdr.find("kReadLeaderRetries = "), std::string::npos);
    EXPECT_NE(hdr.find("kReadLeaderRetryDelay{"), std::string::npos);
    // ... and CONSUMED, not redefined, by the read loop.
    EXPECT_NE(src.find("kReadLeaderRetries"), std::string::npos);
    EXPECT_EQ(src.find("kReadLeaderRetries = "), std::string::npos)
        << "the read leaderless budget was redefined next to its use; it belongs in cluster_data_plane.hpp "
           "where the static_asserts against the Raft clocks and the write window can see it [debt D-26]";
    EXPECT_EQ(src.find("kReadLeaderRetryDelay = "), std::string::npos);
    // The old spelling must be gone entirely -- a surviving `kLeaderRetries` is a second
    // budget under the previous name.
    EXPECT_EQ(src.find("static constexpr int kLeaderRetries"), std::string::npos);
}
