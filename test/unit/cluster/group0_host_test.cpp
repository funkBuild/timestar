// Production group-0 host substrate: collision-free wire identity, a dedicated
// journal, durable snapshot recovery, and fail-closed corrupt recovery. No
// sockets or background server processes are used.
#include "../../../lib/cluster/integration/group0_host.hpp"
#include "../../../lib/cluster/integration/replicated_vshard_host.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/storage/journal_segment.hpp"

#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>
#include <unistd.h>

using namespace timestar;
using namespace timestar::cluster;
using namespace timestar::control;
using namespace timestar::raft;
namespace fs = std::filesystem;

namespace {

class NullTransport final : public RaftTransport {
public:
    seastar::future<> send(Envelope) override { return seastar::make_ready_future<>(); }
};

fs::path tempRoot(const std::string& tag) {
    static std::atomic_uint64_t seq{0};
    fs::path p = fs::temp_directory_path() /
                 ("timestar_group0_host_" + tag + "_" + std::to_string(::getpid()) + "_" +
                  std::to_string(seq.fetch_add(1)));
    fs::remove_all(p);
    return p;
}

JournalIdentity identity() {
    JournalIdentity id;
    id.clusterUuid.fill(0x21);
    id.bootId.fill(0x42);
    return id;
}

RaftOptions singleVoterOptions() {
    RaftOptions opts;
    opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
    opts.heartbeatTimeout = 1;
    return opts;
}

seastar::future<> durableSnapshotRecovery() {
    const fs::path root = tempRoot("recover");
    const JournalIdentity id = identity();
    NullTransport transport;
    Group0State expected;

    {
        Group0Host host(transport, 1, root, id);
        co_await host.start({1}, singleVoterOptions());
        EXPECT_TRUE(host.freshJournal());
        EXPECT_EQ(host.group()->groupId(), kControlRaftGroupId);
        EXPECT_FALSE(VShardId{kControlRaftGroupId}.valid());

        co_await host.group()->campaign();
        EXPECT_TRUE(host.group()->isLeader());
        EXPECT_TRUE(co_await host.propose(InitCluster{"cluster-durable"}));
        EXPECT_TRUE(co_await host.propose(SetMetaVoters{{1}}));
        EXPECT_TRUE(co_await host.propose(SetActiveVersion{5}));
        expected = host.state();
        co_await host.compact();
        EXPECT_GT(host.group()->node().log().snapshotIndex(), 0u);
        co_await host.stop();
    }

    EXPECT_TRUE(fs::exists(root / "group0"));
    EXPECT_FALSE(fs::exists(root / "vshard_0"))
        << "control state must not share data VShard 0's journal directory";

    {
        Group0Host recovered(transport, 1, root, id);
        co_await recovered.start({1}, singleVoterOptions());
        EXPECT_FALSE(recovered.freshJournal());
        EXPECT_EQ(recovered.state(), expected);
        EXPECT_EQ(recovered.group()->groupId(), kControlRaftGroupId);
        co_await recovered.stop();
    }

    fs::remove_all(root);
}

seastar::future<> corruptSnapshotRefusesStartup() {
    const fs::path root = tempRoot("corrupt");
    const fs::path dir = root / "group0";
    const JournalIdentity id = identity();
    fs::create_directories(dir);

    JournalSegmentHeader header;
    header.clusterUuid = id.clusterUuid;
    header.bootId = id.bootId;
    header.coreNumber = 0;
    {
        JournalWriter writer(dir, header, 1u << 20);
        co_await writer.open();
        JournalRaftPersistence persistence(writer, VShardId{0});
        Snapshot bad;
        bad.index = 7;
        bad.term = 2;
        bad.config.voters = {1};
        bad.data = "not a group0 snapshot";
        co_await persistence.persistSnapshot(std::move(bad), /*receivedFromPeer=*/true);
        co_await persistence.sync();
        co_await writer.close();
    }

    NullTransport transport;
    Group0Host host(transport, 1, root, id);
    bool rejected = false;
    try {
        co_await host.start({1}, singleVoterOptions());
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    EXPECT_FALSE(host.started());
    fs::remove_all(root);
}

seastar::future<> freshObserverNeverCampaigns() {
    const fs::path root = tempRoot("observer");
    NullTransport transport;
    Group0Host observer(transport, /*self=*/2, root, identity());
    co_await observer.start(/*known seed voters=*/{1}, singleVoterOptions());
    EXPECT_TRUE(observer.started());
    EXPECT_TRUE(observer.freshJournal());
    auto* group = observer.group();
    if (!group)
        throw std::runtime_error("observer group was not registered");
    EXPECT_EQ(group->role(), Role::Follower);

    // Even with the shortest election timeout, a node outside the voter set is
    // not promotable and must never form its own control-plane term/cluster.
    for (int i = 0; i < 10; ++i)
        co_await group->tick();
    EXPECT_EQ(group->role(), Role::Follower);
    EXPECT_FALSE(group->isLeader());
    EXPECT_EQ(group->currentTerm(), kNoTerm);
    EXPECT_TRUE(observer.state().clusterUuid.empty());

    co_await observer.stop();
    fs::remove_all(root);
}

seastar::future<> periodicPolicyBoundsControlReplay() {
    const fs::path root = tempRoot("maintenance");
    NullTransport transport;
    Group0Host host(transport, 1, root, identity());
    co_await host.start({1}, singleVoterOptions());
    co_await host.group()->campaign();
    EXPECT_TRUE(co_await host.propose(InitCluster{"cluster-maintained"}));
    EXPECT_TRUE(co_await host.propose(SetMetaVoters{{1}}));
    host.startTicking();

    host.setCompactionEntryThreshold(1000);
    EXPECT_FALSE(co_await host.maybeCompactOnce());
    EXPECT_EQ(host.group()->node().log().snapshotIndex(), kNoIndex);

    host.setCompactionEntryThreshold(1);
    EXPECT_TRUE(co_await host.maybeCompactOnce());
    EXPECT_EQ(host.group()->node().log().snapshotIndex(), host.group()->appliedIndex());
    EXPECT_EQ(host.compactionsTaken(), 1u);
    EXPECT_FALSE(co_await host.maybeCompactOnce());
    EXPECT_EQ(host.compactionsTaken(), 1u);
    EXPECT_EQ(host.maintenancePasses(), 3u);

    co_await host.stop();
    fs::remove_all(root);
}

}  // namespace

TEST(Group0HostTest, UsesReservedWireIdAndRecoversDedicatedJournal) {
    durableSnapshotRecovery().get();
}

TEST(Group0HostTest, CorruptSnapshotFailsStartup) {
    corruptSnapshotRefusesStartup().get();
}

TEST(Group0HostTest, FreshNonVoterIsAnInertObserver) {
    freshObserverNeverCampaigns().get();
}

TEST(Group0HostTest, PeriodicPolicyBoundsControlReplay) {
    periodicPolicyBoundsControlReplay().get();
}
