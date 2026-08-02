#include "../../../lib/cluster/data/replicated_command_router.hpp"

#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

using namespace timestar::data;
using timestar::buildSeriesKey;
using timestar::control::ControlMap;
using timestar::raft::NodeId;

namespace {

class CommandSink : public ProposeSink {
public:
    bool commit = true;
    NodeId rejectHint = timestar::raft::kNoNode;
    WriteFailure rejectKind = WriteFailure::NotLeader;
    unsigned calls = 0;
    std::vector<std::string> encoded;

    seastar::future<bool> proposeBatch(WriteBatch) override {
        return seastar::make_exception_future<bool>(std::runtime_error("unused"));
    }

    seastar::future<ProposeOutcome> proposeCommandHinted(uint16_t vshard, ReplicatedCommand command,
                                                         OptDeadline deadline) override {
        ++calls;
        encoded.push_back(encodeReplicatedCommand(command));
        EXPECT_TRUE(deadline.has_value());
        ProposeOutcome out;
        if (commit) {
            out.committed = true;
            out.committedVShards = {vshard};
        } else {
            out.rejects.push_back(SliceReject{vshard, rejectHint, rejectKind});
        }
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
    }
};

class CommandTransport : public NodeTransport {
public:
    unsigned calls = 0;
    std::vector<NodeId> targets;
    std::vector<std::string> encoded;
    std::vector<OptDeadline> deadlines;

    seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
    }
    seastar::future<ProposeOutcome> proposeCommandHinted(NodeId to, uint16_t vshard, ReplicatedCommand command,
                                                         OptDeadline deadline) override {
        ++calls;
        targets.push_back(to);
        encoded.push_back(encodeReplicatedCommand(command));
        deadlines.push_back(deadline);
        ProposeOutcome out;
        out.committed = true;
        out.committedVShards = {vshard};
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
    }
};

class OneShotAmbiguousTransport : public CommandTransport {
public:
    seastar::future<ProposeOutcome> proposeCommandHinted(NodeId to, uint16_t vshard, ReplicatedCommand command,
                                                         OptDeadline deadline) override {
        if (calls++ == 0) {
            targets.push_back(to);
            encoded.push_back(encodeReplicatedCommand(command));
            deadlines.push_back(deadline);
            return seastar::make_exception_future<ProposeOutcome>(std::runtime_error("connection lost after send"));
        }
        targets.push_back(to);
        encoded.push_back(encodeReplicatedCommand(command));
        deadlines.push_back(deadline);
        ProposeOutcome out;
        out.committed = true;
        out.committedVShards = {vshard};
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
    }
};

class MapLeaders : public LeaderResolver {
public:
    std::map<uint16_t, NodeId> leaders;
    NodeId leaderOf(uint16_t vshard) const override {
        const auto it = leaders.find(vshard);
        return it == leaders.end() ? timestar::raft::kNoNode : it->second;
    }
};

ControlMap mapWith(uint16_t vshard) {
    ControlMap map;
    map.epoch = 1;
    map.placement[vshard] = {1, 2, 3};
    return map;
}

DeleteRangeBatch deleteFor(const std::string& key) {
    return DeleteRangeBatch{{DeleteRangeTarget{key, 10, 20}},
                            SeriesId128::fromHex("0fedcba9876543210fedcba987654321"), 1'800'000'000'000};
}

}  // namespace

TEST(ReplicatedCommandRouterTest, CommitsAnExactCommandOnTheLocalLeader) {
    const std::string key = buildSeriesKey("delete", {{"host", "local"}}, "value");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    VShardDirectory dir(1, mapWith(vshard));
    CommandSink local;
    CommandTransport remote;
    MapLeaders leaders;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    const ReplicatedCommand command{deleteFor(key)};
    router.propose(vshard, command).get();

    ASSERT_EQ(local.calls, 1u);
    EXPECT_EQ(remote.calls, 0u);
    ASSERT_EQ(local.encoded.size(), 1u);
    EXPECT_EQ(local.encoded[0], encodeReplicatedCommand(command));
}

TEST(ReplicatedCommandRouterTest, PreservesTheCommandAcrossALeaderHintRetry) {
    const std::string key = buildSeriesKey("delete", {{"host", "redirect"}}, "value");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    VShardDirectory dir(1, mapWith(vshard));
    CommandSink local;
    local.commit = false;
    local.rejectHint = 2;
    CommandTransport remote;
    MapLeaders leaders;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    const ReplicatedCommand command{deleteFor(key)};
    const std::string expected = encodeReplicatedCommand(command);
    router.propose(vshard, command).get();

    ASSERT_EQ(local.encoded.size(), 1u);
    ASSERT_EQ(remote.encoded.size(), 1u);
    EXPECT_EQ(local.encoded[0], expected);
    EXPECT_EQ(remote.encoded[0], expected) << "retry changed or consumed the delete command";
    EXPECT_EQ(remote.targets, std::vector<NodeId>{2});
    ASSERT_EQ(remote.deadlines.size(), 1u);
    EXPECT_TRUE(remote.deadlines[0].has_value());
}

TEST(ReplicatedCommandRouterTest, SnapshotPersistentDeleteIdentityMakesAmbiguousRetrySafe) {
    const std::string key = buildSeriesKey("delete", {{"host", "idempotent-remote"}}, "value");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    VShardDirectory dir(1, mapWith(vshard));
    CommandSink local;
    OneShotAmbiguousTransport remote;
    MapLeaders leaders;
    leaders.leaders[vshard] = 2;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    auto command = deleteFor(key);
    const std::string expected = encodeReplicatedCommand(ReplicatedCommand{command});
    EXPECT_NO_THROW(router.propose(vshard, ReplicatedCommand{command}).get());
    ASSERT_EQ(remote.calls, 2u);
    ASSERT_EQ(remote.encoded.size(), 2u);
    EXPECT_EQ(remote.encoded[0], expected);
    EXPECT_EQ(remote.encoded[1], expected);
}

TEST(ReplicatedCommandRouterTest, VShardDeleteBatchRetriesTheExactBytesAfterAmbiguousSend) {
    const std::string key = buildSeriesKey("delete", {{"host", "batch-remote"}}, "value");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    VShardDirectory dir(1, mapWith(vshard));
    CommandSink local;
    OneShotAmbiguousTransport remote;
    MapLeaders leaders;
    leaders.leaders[vshard] = 2;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    const ReplicatedCommand command{deleteFor(key)};
    const std::string expected = encodeReplicatedCommand(command);
    EXPECT_NO_THROW(router.propose(vshard, command).get());
    ASSERT_EQ(remote.encoded.size(), 2u);
    EXPECT_EQ(remote.encoded[0], expected);
    EXPECT_EQ(remote.encoded[1], expected);
}

TEST(ReplicatedCommandRouterTest, RetiredDeleteIdentityIsTerminalWithoutReproposal) {
    const std::string key = buildSeriesKey("delete", {{"host", "expired"}}, "value");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    VShardDirectory dir(1, mapWith(vshard));
    CommandSink local;
    local.commit = false;
    local.rejectKind = WriteFailure::Expired;
    CommandTransport remote;
    MapLeaders leaders;
    ReplicatedCommandRouter router(dir, local, remote, leaders);
    auto batch = deleteFor(key);

    EXPECT_THROW(router.propose(vshard, ReplicatedCommand{batch}).get(), DeleteReceiptExpiredError);
    EXPECT_EQ(local.calls, 1u);
    EXPECT_EQ(remote.calls, 0u);
}

TEST(ReplicatedCommandRouterTest, RejectsCrossVShardCommandBeforeProposal) {
    const std::string key = buildSeriesKey("delete", {{"host", "foreign"}}, "value");
    const uint16_t actual = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    const uint16_t requested = static_cast<uint16_t>((actual + 1) % timestar::VIRTUAL_SHARD_COUNT);
    VShardDirectory dir(1, mapWith(requested));
    CommandSink local;
    CommandTransport remote;
    MapLeaders leaders;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    EXPECT_THROW(router.propose(requested, ReplicatedCommand{deleteFor(key)}).get(), std::invalid_argument);
    EXPECT_EQ(local.calls, 0u);
    EXPECT_EQ(remote.calls, 0u);
}

TEST(ReplicatedCommandRouterTest, RejectsCrossVShardDeleteBatchBeforeProposal) {
    const std::string first = buildSeriesKey("delete", {{"host", "batch-local"}}, "value");
    const uint16_t requested = timestar::virtualShard(SeriesId128::fromSeriesKey(first));
    std::string foreign;
    for (unsigned i = 0;; ++i) {
        foreign = buildSeriesKey("delete", {{"host", "batch-foreign-" + std::to_string(i)}}, "value");
        if (timestar::virtualShard(SeriesId128::fromSeriesKey(foreign)) != requested)
            break;
    }
    DeleteRangeBatch batch{{DeleteRangeTarget{first, 10, 20}, DeleteRangeTarget{foreign, 10, 20}},
                           SeriesId128::fromHex("123456789abcdef0123456789abcdef0"), 1'800'000'000'000};
    std::sort(batch.targets.begin(), batch.targets.end());
    VShardDirectory dir(1, mapWith(requested));
    CommandSink local;
    CommandTransport remote;
    MapLeaders leaders;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    EXPECT_THROW(router.propose(requested, ReplicatedCommand{batch}).get(), std::invalid_argument);
    EXPECT_EQ(local.calls, 0u);
    EXPECT_EQ(remote.calls, 0u);
}

TEST(ReplicatedCommandRouterTest, RejectsUnassignedVShardBeforeProposal) {
    const std::string key = buildSeriesKey("delete", {{"host", "unassigned"}}, "value");
    const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
    ControlMap empty;
    empty.epoch = 1;
    VShardDirectory dir(1, std::move(empty));
    CommandSink local;
    CommandTransport remote;
    MapLeaders leaders;
    ReplicatedCommandRouter router(dir, local, remote, leaders);

    EXPECT_THROW(router.propose(vshard, ReplicatedCommand{deleteFor(key)}).get(), UnassignedVShardError);
    EXPECT_EQ(local.calls, 0u);
    EXPECT_EQ(remote.calls, 0u);
}
