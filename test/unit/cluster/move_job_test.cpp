// Phase 7 (movement core): the move step machine + Mover. Proves a REPLACE and a
// GROW move keep the group at/above RF at every committed step, move exactly one
// member at a time, promote the destination to voter BEFORE removing the old
// voter, resume correctly from any persisted step (crash-resume), and abort rather
// than commit an unsafe (sub-RF) membership.
#include "../../../lib/cluster/movement/mover.hpp"
#include "../../../lib/cluster/integration/controller_job_driver.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <set>
#include <vector>

using namespace timestar::movement;

namespace {

// A mock group: applies committed configs and records the history so the test can
// assert the RF invariant held at EVERY committed step.
class MockExecutor : public MoveExecutor {
public:
    std::vector<NodeId> voters_;
    std::vector<NodeId> learners_;
    std::vector<std::vector<NodeId>> voterHistory;  // every committed voter set
    bool leader = true;
    MoveJob lastPersisted;

    std::vector<NodeId> voters() const override { return voters_; }
    std::vector<NodeId> learners() const override { return learners_; }
    seastar::future<bool> commitConfig(ConfigTarget target) override {
        if (!leader)
            return seastar::make_ready_future<bool>(false);
        voters_ = target.voters;
        learners_ = target.learners;
        voterHistory.push_back(voters_);
        return seastar::make_ready_future<bool>(true);
    }
    seastar::future<uint64_t> catchUp(NodeId) override { return seastar::make_ready_future<uint64_t>(42); }
    seastar::future<> persist(const MoveJob& job) override {
        lastPersisted = job;
        return seastar::make_ready_future<>();
    }
};

bool has(const std::vector<NodeId>& v, NodeId n) {
    return std::find(v.begin(), v.end(), n) != v.end();
}
bool sameSet(std::vector<NodeId> a, std::vector<NodeId> b) {
    std::sort(a.begin(), a.end());
    std::sort(b.begin(), b.end());
    return a == b;
}

seastar::future<> testReplaceMaintainsRf() {
    MockExecutor ex;
    ex.voters_ = {1, 2, 3};
    MoveJob job(MovePlan{/*vshard=*/7, /*dest=*/4, /*victim=*/1});
    Mover mover(/*minRf=*/3);
    co_await mover.run(job, ex);

    EXPECT_TRUE(job.done());
    EXPECT_EQ(ex.voters_.size(), 3u);
    EXPECT_TRUE(has(ex.voters_, 4));   // dest is now a voter
    EXPECT_FALSE(has(ex.voters_, 1));  // victim removed
    // RF never dropped below 3 at any committed step.
    for (const auto& v : ex.voterHistory)
        EXPECT_GE(v.size(), 3u) << "RF dropped below 3 mid-move";
    // dest was promoted to voter (a committed voter set contains 4) BEFORE the
    // victim was removed (a committed voter set lacks 1). The promote step reaches
    // 4 voters {1,2,3,4}; the remove step returns to {2,3,4}.
    bool sawFourVoters = false;
    for (const auto& v : ex.voterHistory)
        if (v.size() == 4u)
            sawFourVoters = true;
    EXPECT_TRUE(sawFourVoters);  // RF+1 during the overlap, never RF-1
}

seastar::future<> testGrowMaintainsRf() {
    MockExecutor ex;
    ex.voters_ = {1, 2, 3};
    MoveJob job(MovePlan{/*vshard=*/7, /*dest=*/4, /*victim=*/0});  // pure grow
    Mover mover(/*minRf=*/3);
    co_await mover.run(job, ex);
    EXPECT_TRUE(job.done());
    EXPECT_EQ(ex.voters_.size(), 4u);  // grew to 4 voters, no removal
    EXPECT_TRUE(has(ex.voters_, 4));
    for (const auto& v : ex.voterHistory)
        EXPECT_GE(v.size(), 3u);
}

seastar::future<> testCrashResumeFromEveryStep() {
    // On a crash-resume the group's COMMITTED config already reflects the completed
    // steps; the persisted job step says where we are. Resume from each step with
    // the group config that step implies, and verify convergence to {2,3,4} with RF
    // held. Re-issuing a step whose config is already active is an idempotent no-op.
    struct Seed {
        MoveStep step;
        std::vector<NodeId> voters;
        std::vector<NodeId> learners;
    };
    std::vector<Seed> seeds = {
        {MoveStep::Planned, {1, 2, 3}, {}},    {MoveStep::LearnerAdded, {1, 2, 3}, {4}},
        {MoveStep::CaughtUp, {1, 2, 3}, {4}},  {MoveStep::Promoted, {1, 2, 3, 4}, {}},
        {MoveStep::OldRemoved, {2, 3, 4}, {}}, {MoveStep::Done, {2, 3, 4}, {}},
    };
    for (const auto& seed : seeds) {
        MockExecutor ex;
        ex.voters_ = seed.voters;
        ex.learners_ = seed.learners;
        MoveJob job(MovePlan{7, 4, 1}, seed.step);
        auto decoded = MoveJob::decode(job.encode());  // as persisted in a group-0 Job
        EXPECT_TRUE(decoded.has_value());
        if (decoded)
            job = *decoded;
        Mover mover(3);
        co_await mover.run(job, ex);
        EXPECT_TRUE(job.done());
        EXPECT_EQ(ex.voters_.size(), 3u);  // converges to {2,3,4}
        EXPECT_TRUE(has(ex.voters_, 4));
        EXPECT_FALSE(has(ex.voters_, 1));
        for (const auto& v : ex.voterHistory)
            EXPECT_GE(v.size(), 3u);
    }
}

seastar::future<> testMoverGuardFiresOnUnderReplicated() {
    // The group is already below RF (only 2 voters). The Mover must REFUSE to move
    // rather than commit a membership that stays sub-RF -- "foreground/RF outranks
    // rebalance progress". Nothing is committed.
    MockExecutor ex;
    ex.voters_ = {1, 2};
    MoveJob job(MovePlan{7, 4, 1});
    Mover mover(/*minRf=*/3);
    bool threw = false;
    try {
        co_await mover.run(job, ex);
    } catch (const UnsafeMove&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    EXPECT_TRUE(ex.voterHistory.empty());  // nothing committed on an under-replicated group
}

seastar::future<> testPromoteRefusesMissingLearner() {
    // External drift removed the learner between the add and the promote: the
    // Mover must REFUSE to promote a destination that is neither a learner nor a
    // voter (it would add an empty voter that stalls quorum), not silently proceed.
    MockExecutor ex;
    ex.voters_ = {1, 2, 3};
    ex.learners_ = {};  // dest 4 vanished
    MoveJob job(MovePlan{7, 4, 1}, MoveStep::CaughtUp);
    Mover mover(3);
    bool threw = false;
    try {
        co_await mover.run(job, ex);
    } catch (const UnsafeMove&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
}

seastar::future<> testNotLeaderStopsCleanly() {
    MockExecutor ex;
    ex.voters_ = {1, 2, 3};
    ex.leader = false;  // this node lost leadership
    MoveJob job(MovePlan{7, 4, 1});
    Mover mover(3);
    co_await mover.run(job, ex);  // must not throw or hang
    EXPECT_FALSE(job.done());     // stopped; the current leader will redrive it
    EXPECT_TRUE(ex.voterHistory.empty());
}

// A move paused (SLO breach / manual pause / cancel) after a membership change
// STOPS in a safe forward-committed state -- RF never dropped, learner present,
// old voter still there -- and resuming completes forward without rolling back.
seastar::future<> testPauseResumeSafeForward() {
    MockExecutor ex;
    ex.voters_ = {1, 2, 3};
    MoveJob job(MovePlan{7, 4, 1});
    Mover mover(3);
    // Gate stops the move the moment the learner has been added.
    co_await mover.run(job, ex, [&]() { return job.step() != MoveStep::LearnerAdded; });
    EXPECT_EQ(job.step(), MoveStep::LearnerAdded);
    EXPECT_FALSE(job.done());
    EXPECT_EQ(ex.voters_.size(), 3u);   // RF never dropped
    EXPECT_TRUE(has(ex.learners_, 4));  // learner present (safe forward state)
    EXPECT_FALSE(has(ex.voters_, 4));   // not yet promoted; nothing removed
    for (const auto& v : ex.voterHistory)
        EXPECT_GE(v.size(), 3u);

    // Resume: completes forward to the target, never rolling the learner back.
    co_await mover.run(job, ex, []() { return true; });
    EXPECT_TRUE(job.done());
    EXPECT_TRUE(sameSet(ex.voters_, {2, 3, 4}));
    for (const auto& v : ex.voterHistory)
        EXPECT_GE(v.size(), 3u);
}

}  // namespace

TEST(MoveJobTest, PauseResumeSafeForward) {
    testPauseResumeSafeForward().get();
}
TEST(MoveJobTest, ReplaceMaintainsRf) {
    testReplaceMaintainsRf().get();
}
TEST(MoveJobTest, GrowMaintainsRf) {
    testGrowMaintainsRf().get();
}
TEST(MoveJobTest, CrashResumeFromEveryStep) {
    testCrashResumeFromEveryStep().get();
}
TEST(MoveJobTest, MoverGuardFiresOnUnderReplicated) {
    testMoverGuardFiresOnUnderReplicated().get();
}
TEST(MoveJobTest, MaintainsRfInvariant) {
    EXPECT_TRUE(MoveJob::maintainsRf({1, 2, 3}, {1, 2, 3, 4}, 3));  // promote: +1, RF ok
    EXPECT_TRUE(MoveJob::maintainsRf({1, 2, 3, 4}, {2, 3, 4}, 3));  // remove: -1, still >=3
    EXPECT_TRUE(MoveJob::maintainsRf({1, 2, 3}, {1, 2, 3}, 3));     // learner add: unchanged
    EXPECT_FALSE(MoveJob::maintainsRf({1, 2, 3}, {2, 3}, 3));       // drops to 2 < 3: reject
    EXPECT_FALSE(MoveJob::maintainsRf({1, 2, 3}, {1, 4, 5}, 3));    // two members change: reject
}
TEST(MoveJobTest, PromoteRefusesMissingLearner) {
    testPromoteRefusesMissingLearner().get();
}
TEST(MoveJobTest, NotLeaderStopsCleanly) {
    testNotLeaderStopsCleanly().get();
}
TEST(MoveJobTest, EncodeDecodeRoundTrip) {
    constexpr NodeId largeDestination = (NodeId{1} << 48) + 4;
    constexpr NodeId largeVictim = (NodeId{1} << 40) + 1;
    MoveJob j(MovePlan{7, largeDestination, largeVictim}, MoveStep::Promoted);
    const std::string encoded = j.encode();
    EXPECT_EQ(encoded.substr(0, 5), "TSMJ1");
    auto d = MoveJob::decode(encoded);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->plan(), (MovePlan{7, largeDestination, largeVictim}));
    EXPECT_EQ(d->step(), MoveStep::Promoted);
}

TEST(MoveJobTest, RejectsUnknownOrMalformedV1Records) {
    const std::string valid = MoveJob(MovePlan{7, 4, 1}, MoveStep::Promoted).encode();

    std::string unknownVersion = valid;
    unknownVersion[4] = '2';
    EXPECT_FALSE(MoveJob::decode(unknownVersion).has_value());
    EXPECT_FALSE(MoveJob::decode(valid.substr(0, valid.size() - 1)).has_value());
    EXPECT_FALSE(MoveJob::decode(valid + "x").has_value());

    std::string badStep = valid;
    badStep[5] = static_cast<char>(0xff);
    EXPECT_FALSE(MoveJob::decode(badStep).has_value());

    std::string invalidVShard = valid;
    invalidVShard[6] = static_cast<char>(0xff);
    invalidVShard[7] = static_cast<char>(0xff);
    EXPECT_FALSE(MoveJob::decode(invalidVShard).has_value());

    std::string noDestination = valid;
    std::fill(noDestination.begin() + 8, noDestination.begin() + 16, '\0');
    EXPECT_FALSE(MoveJob::decode(noDestination).has_value());

    std::string sameDestinationAndVictim = valid;
    std::copy(sameDestinationAndVictim.begin() + 8, sameDestinationAndVictim.begin() + 16,
              sameDestinationAndVictim.begin() + 16);
    EXPECT_FALSE(MoveJob::decode(sameDestinationAndVictim).has_value());

    EXPECT_THROW(MoveJob(MovePlan{7, 0, 1}).encode(), std::invalid_argument);
    EXPECT_THROW(MoveJob(MovePlan{7, 4, 4}).encode(), std::invalid_argument);
    EXPECT_THROW(MoveJob(MovePlan{timestar::VIRTUAL_SHARD_COUNT, 4, 1}).encode(), std::invalid_argument);
}

TEST(MoveJobTest, ControllerRejectsInconsistentDurableJobMetadata) {
    MoveJob move(MovePlan{7, 4, 1}, MoveStep::Promoted);
    timestar::control::Job valid{"move-7", static_cast<uint32_t>(MoveStep::Promoted), false, move.encode()};
    ASSERT_TRUE(timestar::cluster::ControllerJobDriver::decodeMoveJob(valid).has_value());

    auto wrongStep = valid;
    wrongStep.step = static_cast<uint32_t>(MoveStep::CaughtUp);
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::decodeMoveJob(wrongStep).has_value());

    auto falseCompletion = valid;
    falseCompletion.done = true;
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::decodeMoveJob(falseCompletion).has_value());

    auto emptyId = valid;
    emptyId.id.clear();
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::decodeMoveJob(emptyId).has_value());
}
