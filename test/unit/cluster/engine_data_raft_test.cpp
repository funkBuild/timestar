// Integration M3: EngineDataStateMachine driven through a REAL RaftGroup end to end.
// A single-voter group (self-majority, no peer traffic) commits a ReplicatedCommand
// via proposeAndAwaitApplied; the committed entry is applied through
// EngineDataStateMachine into the real Engine and is then queryable. This proves the
// Raft->state-machine->Engine path (the RF=3 gate with 3 real Engines needs a
// multi-engine harness and is the next M3 step; the apply/commit wiring is proven
// here on one node).
#include "../../../lib/cluster/integration/engine_data_state_machine.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/storage/journal_segment.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

using namespace timestar;
using namespace timestar::raft;
namespace fs = std::filesystem;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class EngineDataRaftTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

fs::path tmpDir() {
    static std::atomic_uint64_t seq{0};
    auto dir = fs::temp_directory_path() /
               ("timestar_m3raft_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(dir);
    return dir;
}

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0x11);
    h.coreNumber = 1;
    h.bootId.fill(0x44);
    return h;
}

// A single voter sends no peer messages, so the transport is never used.
class NoopTransport : public RaftTransport {
public:
    seastar::future<> send(Envelope) override { return seastar::make_ready_future<>(); }
};

std::string writeCommand(const std::string& key, double value) {
    data::WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {BASE};
    s.values = std::vector<double>{value};
    data::WriteBatch b;
    b.series = {std::move(s)};
    return data::encodeReplicatedCommand(data::ReplicatedCommand{std::move(b)});
}

std::string deleteCommand(const std::string& key) {
    return data::encodeReplicatedCommand(data::ReplicatedCommand{data::DeleteRangeKey{key, BASE - 1, BASE + 1}});
}
}  // namespace

TEST_F(EngineDataRaftTest, ProposeThroughRaftAppliesToEngine) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        const std::string key = buildSeriesKey("temp", {{"host", "h1"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        cluster::EngineDataStateMachine sm(store, timestar::VShardId{vshard});

        // Single-voter Raft group over the real Engine's state machine.
        fs::path dir = tmpDir();
        JournalWriter writer(dir, header(), 1u << 20);
        auto recovered = writer.open().get();
        RecoveredRaftState st = recoverRaftState(recovered, VShardId{vshard});
        JournalRaftPersistence persistence(writer, VShardId{vshard}, st.nextSeq);
        NoopTransport transport;

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;
        RaftNode node(1, {1}, std::move(st.log), st.hardState, opts, {});
        RaftGroup group(vshard, std::move(node), persistence, transport, sm);

        // Tick to leadership (a lone voter self-elects within a couple ticks).
        for (int i = 0; i < 10 && !group.isLeader(); ++i)
            group.tick().get();
        ASSERT_TRUE(group.isLeader());

        // Propose a write; drive ticks until the commit+apply ack resolves.
        auto f = group.proposeAndAwaitApplied(writeCommand(key, 42.5));
        for (int i = 0; i < 20 && !f.available(); ++i)
            group.tick().get();
        bool applied = f.get();
        EXPECT_TRUE(applied);
        EXPECT_GT(sm.appliedIndex(), 0u);

        // The replicated write is now visible in the real Engine.
        http::HttpQueryHandler h(&*eng);
        QueryRequest q;
        q.aggregation = AggregationMethod::LATEST;
        q.measurement = "temp";
        q.fields = {"value"};
        q.startTime = BASE - 1'000'000'000ULL;
        q.endTime = BASE + 1'000'000'000ULL;
        auto r = h.executeQuery(q).get();
        ASSERT_TRUE(r.success) << r.errorMessage;
        ASSERT_EQ(r.series.size(), 1u);
        EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(r.series[0].fields.at("value").second)[0], 42.5);

        // The exact-key delete travels through the same real Raft commit/apply
        // boundary and removes the point only after quorum acknowledgement.
        auto deleted = group.proposeAndAwaitApplied(deleteCommand(key));
        for (int i = 0; i < 20 && !deleted.available(); ++i)
            group.tick().get();
        EXPECT_TRUE(deleted.get());
        auto afterDelete = h.executeQuery(q).get();
        EXPECT_TRUE(afterDelete.success) << afterDelete.errorMessage;
        EXPECT_TRUE(afterDelete.series.empty());

        writer.close().get();
        fs::remove_all(dir);
    }).get();
}
