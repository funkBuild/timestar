// Integration F.4 gate: the enriched data-plane transport (WriteBatch / queryNode)
// driven over a REAL loopback seastar::rpc socket into a REAL sharded<Engine> via
// EngineLocalStore. Proves the lossless command path survives the wire end to end:
//   - a WriteBatch (incl. a STRING series the flat DataPoint could not carry) is
//     forwarded over the socket and durably applied into the Engine, and
//   - queryNode returns the owner's NodeQueryPartial, byte-for-byte equal to the
//     in-process EngineLocalStore answer -- for a non-numeric (string) read and a
//     cross-series `spread ... by {tag}` aggregation (a method whose fold-of-one is
//     not the identity, so it must survive the round-trip through real aggregation).
#include "../../../lib/cluster/data/dataplane_rpc.hpp"
#include "../../../lib/cluster/data/leader_filtered_node_store.hpp"
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <optional>
#include <seastar/core/thread.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/rpc/rpc.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class DataPlaneRpcEnrichedTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

seastar::socket_address loopback(uint16_t port) {
    return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", port));
}

data::WriteSeries series(
    const std::string& m, std::map<std::string, std::string> tags, const std::string& field, TSMValueType type,
    std::vector<uint64_t> ts,
    std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>, std::vector<std::string>> vals) {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey(m, tags, field);
    s.type = type;
    s.timestamps = std::move(ts);
    s.values = std::move(vals);
    return s;
}

// A one-value extract of a field's double column (for the spread assertion).
std::vector<double> doublesOf(const timestar::http::SeriesResult& r, const std::string& field) {
    auto it = r.fields.find(field);
    if (it == r.fields.end())
        return {};
    auto* v = std::get_if<std::vector<double>>(&it->second.second);
    return v ? *v : std::vector<double>{};
}
std::vector<std::string> stringsOf(const timestar::http::SeriesResult& r, const std::string& field) {
    auto it = r.fields.find(field);
    if (it == r.fields.end())
        return {};
    auto* v = std::get_if<std::vector<std::string>>(&it->second.second);
    return v ? *v : std::vector<std::string>{};
}

// A NodeStore whose apply always fails -- to prove a failed owner-apply surfaces
// to the forwarding caller as a failed RPC (fail-closed), never a silent ack.
class ThrowingNodeStore : public data::NodeStore {
public:
    seastar::future<> applyWrites(data::WriteBatch) override {
        return seastar::make_exception_future<>(std::runtime_error("apply boom"));
    }
    seastar::future<bool> applyDelete(std::string, uint64_t, uint64_t) override {
        return seastar::make_exception_future<bool>(std::runtime_error("delete boom"));
    }
    seastar::future<data::NodeQueryPartial> queryLocal(data::NodeQueryRequest) override {
        return seastar::make_exception_future<data::NodeQueryPartial>(std::runtime_error("query boom"));
    }
};

class RecordingPatternStore : public ThrowingNodeStore {
public:
    int calls = 0;
    data::PatternSeriesRequest lastRequest;
    data::PatternSeriesResult response;

    seastar::future<data::PatternSeriesResult> findPatternSeries(data::PatternSeriesRequest request) override {
        ++calls;
        lastRequest = std::move(request);
        return seastar::make_ready_future<data::PatternSeriesResult>(response);
    }
};

class RecordingFrozenDeletePlanSink : public data::FrozenDeletePlanSink {
public:
    int calls = 0;
    control::FrozenDeletePlanRpcRequest lastRequest;
    control::FreezeDeletePlanResult response;

    seastar::future<control::FreezeDeletePlanResult> handleFrozenDeletePlan(
        control::FrozenDeletePlanRpcRequest request) override {
        ++calls;
        lastRequest = std::move(request);
        return seastar::make_ready_future<control::FreezeDeletePlanResult>(response);
    }
};

// Minimal legacy DataPoint sink, so a node can be started on the LEGACY path and
// we can prove an enriched verb sent to it fails cleanly (unknown verb), not hangs.
class LegacyMemStore : public data::LocalStore {
public:
    seastar::future<> applyWrites(std::vector<data::DataPoint>) override { return seastar::make_ready_future<>(); }
    seastar::future<data::QueryPartial> queryLocal(data::QuerySpec) override {
        return seastar::make_ready_future<data::QueryPartial>();
    }
};

// A ProposeSink double: records the batch, returns a configurable committed result
// (true = leader committed on quorum, false = not the leader).
class RecordingProposeSink : public data::ProposeSink {
public:
    bool committed = true;
    data::WriteFailure commandFailure = data::WriteFailure::NotLeader;
    int calls = 0;
    int commandCalls = 0;
    size_t lastSeriesCount = 0;
    uint16_t lastCommandVShard = 0;
    std::string lastCommandBytes;
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override {
        ++calls;
        lastSeriesCount = batch.series.size();
        return seastar::make_ready_future<bool>(committed);
    }
    seastar::future<data::ProposeOutcome> proposeCommandHinted(uint16_t vshard, data::ReplicatedCommand command,
                                                               data::OptDeadline) override {
        ++commandCalls;
        lastCommandVShard = vshard;
        lastCommandBytes = data::encodeReplicatedCommand(command);
        data::ProposeOutcome out;
        out.committed = committed;
        if (committed)
            out.committedVShards = {vshard};
        else
            out.rejects.push_back(data::SliceReject{vshard, timestar::raft::kNoNode, commandFailure});
        return seastar::make_ready_future<data::ProposeOutcome>(std::move(out));
    }
};

data::WriteBatch oneFloatBatch() {
    data::WriteBatch b;
    b.series.push_back(series("m", {{"host", "h1"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{1.0}));
    return b;
}

// A raw seastar::rpc peer that speaks the data plane's wire framing but is NOT a
// DataPlaneRpc: it answers the version-negotiation verb with whatever version the test
// names, and CAPTURES the raw propose frame instead of decoding it. That is the only
// way to assert what was actually put on the wire -- a real DataPlaneRpc decodes both
// formats no matter which range it advertises, so it cannot tell a gated encoder from
// one that ignores the negotiation.
//
// The serializer must be byte-compatible with DataPlaneRpc's (u32 length + bytes) and
// the verb ids must match dataplane_rpc.cpp: 6 = proposeWrite, 9 = negotiateVersion.
struct TapSerializer {};
template <typename Output>
void write(TapSerializer, Output& out, const seastar::sstring& v) {
    const uint32_t n = static_cast<uint32_t>(v.size());
    out.write(reinterpret_cast<const char*>(&n), sizeof(n));
    out.write(v.data(), v.size());
}
template <typename Input>
seastar::sstring read(TapSerializer, Input& in, seastar::rpc::type<seastar::sstring>) {
    uint32_t n = 0;
    in.read(reinterpret_cast<char*>(&n), sizeof(n));
    seastar::sstring s = seastar::uninitialized_string(n);
    in.read(s.data(), n);
    return s;
}

// A raw CLIENT on the same framing: lets a test put ARBITRARY bytes on a data-plane
// verb, which is the only way to exercise inbound admission against a frame no
// DataPlaneRpc would ever encode.
class RawPeerClient {
public:
    explicit RawPeerClient(seastar::socket_address addr) : client_(proto_, addr) {
        send_ = proto_.make_client<seastar::sstring(seastar::sstring)>(3);  // kForwardWriteBatch
    }
    seastar::sstring sendRaw(const std::string& bytes) {
        return send_(client_, seastar::sstring(bytes.data(), bytes.size())).get();
    }
    void stop() { client_.stop().get(); }

private:
    seastar::rpc::protocol<TapSerializer> proto_{TapSerializer{}};
    seastar::rpc::protocol<TapSerializer>::client client_;
    std::function<seastar::future<seastar::sstring>(seastar::rpc::protocol<TapSerializer>::client&, seastar::sstring)>
        send_;
};

// Peak resident set (VmHWM) in KiB -- monotone, so a delta across one call is what that
// call forced us to touch.
size_t peakRssKb() {
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line))
        if (line.rfind("VmHWM:", 0) == 0)
            return static_cast<size_t>(std::strtoul(line.c_str() + 6, nullptr, 10));
    return 0;
}

// A checksum-valid v1 WriteBatch frame of `n` EMPTY Float series. Each costs exactly the
// 13 wire bytes the decoder charges for (type + zero keyLen + zero count + zero
// revCount) and 144 bytes resident -- an 11x structural ratio no decoder change can
// alter, which is why inbound admission is the lever. Built as raw bytes, so the test
// itself never materialises the 458 MB it is about to refuse.
std::string emptySeriesFrame(size_t n) {
    auto u32 = [](std::string& o, uint32_t v) {
        for (int i = 0; i < 4; ++i)
            o.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    };
    auto u64 = [](std::string& o, uint64_t v) {
        for (int i = 0; i < 8; ++i)
            o.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    };
    std::string s;
    s.reserve(n * 13 + 24);
    u64(s, 0);                         // schemaVersion
    u32(s, static_cast<uint32_t>(n));  // series count
    for (size_t i = 0; i < n; ++i) {
        s.push_back(static_cast<char>(TSMValueType::Float));
        u32(s, 0);  // empty seriesKey
        u32(s, 0);  // zero points
        u32(s, 0);  // zero revisions
    }
    uint64_t h = 1469598103934665603ull;
    for (char c : s) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ull;
    }
    u64(s, h);
    return s;
}

// A small, legal frame -- used to prove a connection still works after a refusal.
std::string encodeWriteBatchForTest() {
    return data::encodeWriteBatch(oneFloatBatch(), data::kWriteBatchFormatV1);
}

class WireTapPeer {
public:
    std::vector<std::string> captured;         // raw proposeWrite frames, in arrival order
    std::vector<std::string> capturedQueries;  // raw queryNode frames (verb 4), in arrival order
    int negotiateCalls = 0;                    // how many handshakes this peer was asked for
    // Redirects this tap answers a queryNode with, whether or not it was asked to resolve
    // anything -- so a test can play a peer whose reply shape has drifted (debt D-25).
    std::vector<data::VShardRedirect> replyRedirects;
    bool blockQueries = false;

    WireTapPeer(seastar::socket_address addr, uint32_t agreedVersion) {
        proto_.register_handler(9, [this, agreedVersion](seastar::sstring) {
            ++negotiateCalls;
            char b[4];
            for (int i = 0; i < 4; ++i)
                b[i] = static_cast<char>((agreedVersion >> (8 * i)) & 0xff);
            return seastar::make_ready_future<seastar::sstring>(seastar::sstring(b, 4));
        });
        proto_.register_handler(6, [this](seastar::sstring data) {
            captured.emplace_back(data.data(), data.size());
            return seastar::make_ready_future<seastar::sstring>(seastar::sstring("1"));  // committed
        });
        proto_.register_handler(4, [this](seastar::sstring data) {
            capturedQueries.emplace_back(data.data(), data.size());
            if (blockQueries) {
                if (blockedQuery_)
                    throw std::runtime_error("only one blocked test query is supported");
                blockedQuery_.emplace();
                return blockedQuery_->get_future();
            }
            data::NodeQueryPartial p;
            p.redirects = replyRedirects;
            const std::string bytes = data::encodeNodeQueryPartial(p);
            return seastar::make_ready_future<seastar::sstring>(seastar::sstring(bytes.data(), bytes.size()));
        });
        seastar::listen_options lo;
        lo.reuse_address = true;
        lo.set_fixed_cpu(seastar::this_shard_id());
        server_ = std::make_unique<seastar::rpc::protocol<TapSerializer>::server>(proto_, seastar::listen(addr, lo));
    }
    void stop() {
        releaseBlockedQuery();
        if (server_)
            server_->stop().get();
        server_.reset();
    }

    void releaseBlockedQuery() {
        if (!blockedQuery_)
            return;
        data::NodeQueryPartial p;
        const std::string bytes = data::encodeNodeQueryPartial(p);
        blockedQuery_->set_value(seastar::sstring(bytes.data(), bytes.size()));
        blockedQuery_.reset();
    }

private:
    std::optional<seastar::promise<seastar::sstring>> blockedQuery_;
    seastar::rpc::protocol<TapSerializer> proto_{TapSerializer{}};
    std::unique_ptr<seastar::rpc::protocol<TapSerializer>::server> server_;
};

// A ReadIndexSink double: returns configured indices, or rejects (as a non-leader
// would) when asked. Records the VShard it was asked about.
class RecordingReadIndexSink : public data::ReadIndexSink {
public:
    raft::LogIndex readIdx = 0;
    raft::LogIndex commitIdx = 0;
    bool rejectRead = false;
    bool rejectCommit = false;
    uint16_t lastVshard = 0xffff;
    seastar::future<raft::LogIndex> leaderReadIndex(uint16_t vs) override {
        lastVshard = vs;
        if (rejectRead)
            return seastar::make_exception_future<raft::LogIndex>(std::runtime_error("not leader"));
        return seastar::make_ready_future<raft::LogIndex>(readIdx);
    }
    seastar::future<raft::LogIndex> leaderCommitIndex(uint16_t vs) override {
        lastVshard = vs;
        if (rejectCommit)
            return seastar::make_exception_future<raft::LogIndex>(std::runtime_error("not leader"));
        return seastar::make_ready_future<raft::LogIndex>(commitIdx);
    }
};
}  // namespace

TEST_F(DataPlaneRpcEnrichedTest, WriteBatchAndQueryNodeOverRealSocket) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        // One transport as both owner-server and self-client: addPeer(self) loops a
        // forwardWriteBatch/queryNode back over its own loopback socket -- a genuine
        // wire round-trip through a single real Engine (no second-Engine shard-dir
        // collision).
        const uint16_t port = 39310;
        const data::NodeId self = 1;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        // A batch spanning a STRING series (DataPoint could not carry it) and two
        // FLOAT series sharing region=west at the SAME timestamp, so a cross-series
        // `spread by {region}` has a non-trivial answer (max-min = 20).
        data::WriteBatch batch;
        batch.series.push_back(series("log", {{"host", "h1"}}, "msg", TSMValueType::String, {BASE},
                                      std::vector<std::string>{"over the wire"}));
        batch.series.push_back(series("m", {{"host", "h1"}, {"region", "west"}}, "v", TSMValueType::Float, {BASE},
                                      std::vector<double>{10.0}));
        batch.series.push_back(series("m", {{"host", "h2"}, {"region", "west"}}, "v", TSMValueType::Float, {BASE},
                                      std::vector<double>{30.0}));

        // Forward over the socket; resolves only after the owner durably applied.
        rpc.forwardWriteBatch(self, batch).get();

        http::HttpQueryHandler handler(&*eng);
        // queryNode returns UNFINALIZED partials; the coordinator finalizes them. We
        // finalize here and assert the socket-shipped partials produce the same
        // answer as executeQuery would -- the real F.5b cluster query round-trip.
        auto finalizeSocket = [&](data::NodeQueryRequest req) {
            data::NodeQueryPartial viaSocket = rpc.queryNode(self, req).get();
            EXPECT_TRUE(viaSocket.incompleteReasons.empty());
            return handler
                .finalizeClusterPartials(req.request, std::move(viaSocket.partials), std::move(viaSocket.nonNumeric))
                .get();
        };

        // (1) Non-numeric read over the socket == the string written.
        {
            data::NodeQueryRequest req;
            req.request.aggregation = AggregationMethod::LATEST;
            req.request.measurement = "log";
            req.request.fields = {"msg"};
            req.request.startTime = BASE - 1'000'000'000ULL;
            req.request.endTime = BASE + 1'000'000'000ULL;

            QueryResponse got = finalizeSocket(req);
            QueryResponse direct = handler.executeQuery(req.request).get();
            ASSERT_TRUE(got.success) << got.errorMessage;
            ASSERT_EQ(got.series.size(), 1u);
            EXPECT_EQ(stringsOf(got.series[0], "msg"), (std::vector<std::string>{"over the wire"}));
            ASSERT_EQ(direct.series.size(), 1u);
            EXPECT_EQ(stringsOf(got.series[0], "msg"), stringsOf(direct.series[0], "msg"));
        }

        // (2) spread by {region}: max-min across the two series at BASE = 20, over the
        // wire -- the cross-series/cross-node fold-of-one-non-identity method that
        // finalized SeriesResult partials could NOT express.
        {
            data::NodeQueryRequest req;
            req.request.aggregation = AggregationMethod::SPREAD;
            req.request.measurement = "m";
            req.request.fields = {"v"};
            req.request.groupByTags = {"region"};
            req.request.startTime = BASE - 1'000'000'000ULL;
            req.request.endTime = BASE + 1'000'000'000ULL;

            QueryResponse got = finalizeSocket(req);
            ASSERT_TRUE(got.success) << got.errorMessage;
            ASSERT_EQ(got.series.size(), 1u);
            EXPECT_EQ(got.series[0].tags.at("region"), "west");
            ASSERT_EQ(doublesOf(got.series[0], "v").size(), 1u);
            EXPECT_DOUBLE_EQ(doublesOf(got.series[0], "v")[0], 20.0);
        }

        rpc.stop().get();
    }).get();
}

// Fail-closed: a failed owner-apply propagates to the forwarding caller as a
// failed RPC, never a silent success (the plan's "no silent partial" contract).
TEST_F(DataPlaneRpcEnrichedTest, OwnerApplyFailureSurfacesToCaller) {
    seastar::async([] {
        const uint16_t port = 39311;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), sink).get();
        rpc.addPeer(self, loopback(port));

        bool threw = false;
        try {
            rpc.forwardWriteBatch(self, oneFloatBatch()).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "a failed owner-apply must fail the forwarding RPC";
        rpc.stop().get();
    }).get();
}

// Coexistence safety: an enriched verb sent to a peer serving only the LEGACY
// path fails cleanly (unknown verb) rather than hanging forever on a waited call.
TEST_F(DataPlaneRpcEnrichedTest, EnrichedVerbToLegacyPeerFailsCleanly) {
    seastar::async([] {
        const uint16_t serverPort = 39312, clientPort = 39313;
        const data::NodeId server = 1;
        LegacyMemStore legacy;
        ThrowingNodeStore unused;  // the client's own sink (never invoked here)
        data::DataPlaneRpc srv, cli;
        srv.start(loopback(serverPort), legacy).get();  // legacy path only (verbs 1/2)
        cli.start(loopback(clientPort), unused).get();  // enriched path
        cli.addPeer(server, loopback(serverPort));

        bool threw = false;
        try {
            cli.forwardWriteBatch(server, oneFloatBatch()).get();  // verb 3 -> unknown on srv
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "enriched verb to a legacy-only peer must fail, not hang";
        cli.stop().get();
        srv.stop().get();
    }).get();
}

// M3: proposeWrite forwards a WriteBatch to a peer's Raft propose sink over the wire,
// returning the leader's committed/not-leader result.
TEST_F(DataPlaneRpcEnrichedTest, ProposeWriteForwardsToSinkAndReturnsResult) {
    seastar::async([] {
        const uint16_t port = 39315;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;  // node store (unused by proposeWrite)
        RecordingProposeSink propose;
        data::DataPlaneRpc rpc;
        rpc.setProposeSink(propose);
        rpc.start(loopback(port), sink).get();
        rpc.addPeer(self, loopback(port));

        // Leader commits -> true, and the sink saw the batch.
        propose.committed = true;
        bool ok = rpc.proposeWrite(self, oneFloatBatch()).get();
        EXPECT_TRUE(ok);
        EXPECT_EQ(propose.calls, 1);
        EXPECT_EQ(propose.lastSeriesCount, 1u);

        // Not the leader -> false (caller redirects).
        propose.committed = false;
        EXPECT_FALSE(rpc.proposeWrite(self, oneFloatBatch()).get());
        EXPECT_EQ(propose.calls, 2);

        rpc.stop().get();
    }).get();
}

// M4: leaderReadIndex/leaderCommitIndex reach a peer's ReadIndexSink over the wire,
// returning the index; a non-leader rejection propagates as a thrown exception (the
// caller's partition/redirect signal, never a stale value).
TEST_F(DataPlaneRpcEnrichedTest, LeaderReachVerbsForwardToSinkOverSocket) {
    seastar::async([] {
        const uint16_t port = 39317;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        RecordingReadIndexSink ri;
        data::DataPlaneRpc rpc;
        rpc.setReadIndexSink(ri);
        rpc.start(loopback(port), sink).get();
        rpc.addPeer(self, loopback(port));

        ri.readIdx = 7;
        ri.commitIdx = 9;
        EXPECT_EQ(rpc.leaderReadIndex(self, 5).get(), 7u);
        EXPECT_EQ(ri.lastVshard, 5u) << "the requested VShard reaches the sink";
        EXPECT_EQ(rpc.leaderCommitIndex(self, 42).get(), 9u);
        EXPECT_EQ(ri.lastVshard, 42u);

        // A non-leader rejection surfaces as a throw on the client.
        ri.rejectRead = true;
        bool threw = false;
        try {
            rpc.leaderReadIndex(self, 5).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "a rejected read-index must throw, not return a stale value";

        rpc.stop().get();
    }).get();
}

namespace tlscerts {
// Self-signed test CA + a node cert/key signed by it (SAN DNS:timestar-node). Test
// fixtures only -- never real credentials.
const char* kCa = R"PEM(-----BEGIN CERTIFICATE-----
MIIDFzCCAf+gAwIBAgIUBq8rloDXJSRUJOwe8AF/EDkSOr8wDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQdGltZXN0YXItdGVzdC1jYTAeFw0yNjA3MjQxNTU5MDha
Fw0zNjA3MjExNTU5MDhaMBsxGTAXBgNVBAMMEHRpbWVzdGFyLXRlc3QtY2EwggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC3/r9c/AC9i4mQg+Tjpyg0v7F3
HmtukTpJ0Rmc5Sl8YuFaCAHfH5GX2DAGFaUu5i4PCJ7caIuZiiHXv0i0Tapyskrh
JfupHSwimM0WIsDLFA7Pi9RV1b7AQEbW7fD6E3+sCaInMvgkrLus6OqkvoKzWxYK
nN+9vewfeJePswkh855HNNBZYT8KE1GvfLN7TrlwNoHVZjHvWEs/IGvZqY21jGPw
bCQXb9FOuJbwAlMZpE8Matqcz0jCBLxjxOW06IqcDbIsg0FGXgnNWke75a6Ylcml
BIVLk6Uhc8BVdNbovVPvS+S8B5Ef7SAgClqRHtnYxYV13EL9upO18tvdg1+XAgMB
AAGjUzBRMB0GA1UdDgQWBBRMntfiSA12YS9aT9AHtsXeWyvrWTAfBgNVHSMEGDAW
gBRMntfiSA12YS9aT9AHtsXeWyvrWTAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3
DQEBCwUAA4IBAQA8UDOmC27SHHEw/KEKRItEbRBuWdhQZ5FslWABIuUXIatI7hRk
VYAxr2wk9mIgeTDfE2gaeQLvHwSEv7RUXGiSx5QMpoiFAxdmBPUlk/b/UlBeTf9Q
ia9McvMMhVby98LM7cNpQ8kAs8yTArlYdM8G6BOWqagyKfZwwlgFjVl6zRdpZLm6
MogYwDdoSeQzg9IO6uRzXiHka+8hGKuhqLF0dBPt8jjBk+NqLec872pV1ngQ1OVV
tF/flqKYZucrkeWQOVgspaEGGB7fje5H4rRoQW5nVcvNqWJNWrNqG9VmMn/zzSL4
46RvYefIltLS/44+eFO97YLfs95OK+mBHN6C
-----END CERTIFICATE-----
)PEM";
const char* kNodeCrt = R"PEM(-----BEGIN CERTIFICATE-----
MIIDHTCCAgWgAwIBAgIUTwYHB1Ll4oY11uXWrowgT5AxBAkwDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQdGltZXN0YXItdGVzdC1jYTAeFw0yNjA3MjQxNTU5MDha
Fw0zNjA3MjExNTU5MDhaMBgxFjAUBgNVBAMMDXRpbWVzdGFyLW5vZGUwggEiMA0G
CSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDzkpQzIvPJ5J+Id8KnGTAdkj2GrDAH
cAgYWtO3/A3ivrYbESQ1Pa/4ntJe2bxeaXszDskcKm/xSdTnHZLN+OwJTjr4eUfb
/yApKr2lQF2XQRDgXjf03mn9T5EjwQqeQQXydM62j7c3WcrRZMQLqYZdnDaxjtrV
5As03uM8Dd1puy9Oupnibq/cflvdA/a0j1aaYb6yfZOcLgfLt8+3S4saaWfFaIoS
al/xcyL96lFAqHy4iqVOFo3WxpHhY8+gbMOVRhDd2MW0Nx26nKe/eGni8Rcji0F8
Tj4aaC2wTtMNbEIiM6kYRWPXdwosuzCIyA+SjtCBcADwt3U9plAoLgpFAgMBAAGj
XDBaMBgGA1UdEQQRMA+CDXRpbWVzdGFyLW5vZGUwHQYDVR0OBBYEFFteVJBO8d7w
gCS6MI7bCCQXwa0SMB8GA1UdIwQYMBaAFEye1+JIDXZhL1pP0Ae2xd5bK+tZMA0G
CSqGSIb3DQEBCwUAA4IBAQAOmDLsu2EJ3f2kcFARyh8xfkvMVWNF/dxzNThylW0k
epIeUXsbAAXDoMOiL2RoVDSRWYxZwdnlumPTOEjXhSu6FiG+xmfOYMAsBMZlG1fG
zEjEgSkiQbaAyL118PKiOxJlAiAp1OQxxhAG23pkjuNJhpRAmNRUFHks8F4LVoE6
aWCj1TUhmkzQn0dEmAM4nf/wM3z3VCxZkPtjwswpMQAUNoihXW/Gtib1ta4dijQd
q4FQIhy+FgY3RKbYe9aFi3NlvCdaxRnBfQE4dM9Bxf7sK9hsM2kWuJdTL51iZOFC
wocgUBxjCAQE5PYSDFaLOKuXaquJs6SPESNakEfhYru6
-----END CERTIFICATE-----
)PEM";
const char* kNodeKey = R"PEM(-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDzkpQzIvPJ5J+I
d8KnGTAdkj2GrDAHcAgYWtO3/A3ivrYbESQ1Pa/4ntJe2bxeaXszDskcKm/xSdTn
HZLN+OwJTjr4eUfb/yApKr2lQF2XQRDgXjf03mn9T5EjwQqeQQXydM62j7c3WcrR
ZMQLqYZdnDaxjtrV5As03uM8Dd1puy9Oupnibq/cflvdA/a0j1aaYb6yfZOcLgfL
t8+3S4saaWfFaIoSal/xcyL96lFAqHy4iqVOFo3WxpHhY8+gbMOVRhDd2MW0Nx26
nKe/eGni8Rcji0F8Tj4aaC2wTtMNbEIiM6kYRWPXdwosuzCIyA+SjtCBcADwt3U9
plAoLgpFAgMBAAECggEAIc6LN9LG3BOZqfcXYxp9oWkaFY5iJzIfSYQXxT5cjgdy
3qxhJmueuEcRA128xazlzucjNj/UpDyfaomiBekiF8OOL00kEm6lf9lBE8Xsh5Ee
HsotAZV6SBCqYDhLuT3krauVQmUNpMbXffs6s7Suo+EJ/ViK2qupe4fhKcVx4Rn2
63H3+pYj1tJJC3fgwhIjCjedbGTxOCMiyaHSUfJTvm9vdkOr5whQ0v/LAKGrjKP9
7OC2yZd3wNyKlfW+Ayscl4E/mLO1xRoJuF2V+6Q4XdI6xysiBMTLukiQL6yPLyDF
OJujYzBWutQJKbchNaCFfBMBXd1qetSVwqNxpZKssQKBgQD8Kh00Lue+bBZVnPur
pMEcSL7J6T1k1jrFsUPDDEuOIZEP13AnfGB4nV2kfTRttaHxkS0Z6lUEQrIvosN9
okJin2w93RS/rObek+jAL8RrJvRm43fqs8e/+jid1DXzOECdlePBh/yEWhM9hYCU
flF+1PvYqv+HHrQOgSbN5pzClQKBgQD3RwJgz070K4r2FOQAvH1jvj6vIXv4Zdgo
fgWWdTaxluTMwvTMlSyWYtIgLM9ahJKP/4RyvAOGLYmjUb9ezsLsTzfJDkB7VfkK
yTOYf/x+Dd/zgk6BX8mi1GFVo2jI4F2molad7I+LJWuCz9FR5Y+BIA/7qWp6h/V+
8o5RfXRs8QKBgQDsbXQaRExGj0NVnC2fnobtRQuVdpl4nSBX0T+Odk21AqXnK4Dd
lNFC5ZEyM65fmugu/YZDASIbL4mv/jS669LAc2dijZHxsWR5lkapQ2Avc0O94FLD
/TIxPqOs35aB5+E1n57/CshpM6dMjIqlL9arS3iiipmxD8mUu+UtMqcSDQKBgQDL
YJ32HcukS4PZbckxSdYfiUNpKzMZVDp641uZKgK4AZFhUB+jfDXV4qVMTU6l9k/N
G61F6JlFbIK9zuiFA62SSn1pYc1rI4TXeDB1hx6WVrcRQuVqxuvCfscndmUiglbE
TNTMwto06awJRP+2SgbDfylmJSssaFJj/P9MytBNIQKBgQDvS4YbqHAdl9uJUBGa
AtLKNjWdB4UkivX2JiRnK4/7bbxk3MrF08Mtn5+2Kg1rzcRSTyBQm6BLx7nrXsEK
hkAVmqv4oLvkQgg9T2s1UHLvRtXnouGU+8Mq0uc5ZX9NHGPCAR3StHTWfeNQYYSo
wCW8O9DtnlnIieWNH0yv5/HZ+A==
-----END PRIVATE KEY-----
)PEM";
}  // namespace tlscerts

// X1b: mutual TLS on the data-plane transport. A TLS-authenticated peer negotiates
// normally; a PLAINTEXT client to a TLS server cannot complete an RPC.
TEST_F(DataPlaneRpcEnrichedTest, MutualTlsAllowsAuthedPeerRejectsPlaintext) {
    seastar::async([] {
        const uint16_t port = 39322;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        data::DataPlaneRpc srv;
        srv.setTlsCredentials(tlscerts::kNodeCrt, tlscerts::kNodeKey, tlscerts::kCa, "timestar-node");
        srv.setLocalVersion(features::VersionRange{1, 2});
        srv.start(loopback(port), sink).get();
        srv.addPeer(self, loopback(port));  // self-loop: TLS client to our own TLS server

        // Mutual TLS handshake succeeds -> the verb round-trips.
        EXPECT_EQ(srv.negotiateVersion(self).get(), 2u);
        srv.stop().get();

        // A plaintext client to a TLS server: the RPC must fail (no plaintext bypass).
        data::DataPlaneRpc tlsSrv, plainCli;
        tlsSrv.setTlsCredentials(tlscerts::kNodeCrt, tlscerts::kNodeKey, tlscerts::kCa, "timestar-node");
        ThrowingNodeStore s2, s3;
        const uint16_t port2 = 39323;
        tlsSrv.start(loopback(port2), s2).get();
        plainCli.start(loopback(39324), s3).get();  // no TLS
        plainCli.addPeer(2, loopback(port2));
        bool threw = false;
        try {
            plainCli.negotiateVersion(2).get();
        } catch (...) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "a plaintext client must not complete an RPC to a TLS server";
        plainCli.stop().get();
        tlsSrv.stop().get();
    }).get();
}

// M6/X: wire-version negotiation over the socket. Compatible ranges agree on the
// highest common version; a non-overlapping peer is REFUSED (thrown), never
// silently mis-framed.
TEST_F(DataPlaneRpcEnrichedTest, VersionNegotiationAgreesOrRefuses) {
    seastar::async([] {
        const uint16_t port = 39319;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.setLocalVersion(features::VersionRange{1, 3});  // this node speaks 1..3
        rpc.start(loopback(port), sink).get();
        rpc.addPeer(self, loopback(port));

        // Self-negotiation (both ranges 1..3): agree on the highest common = 3.
        EXPECT_EQ(rpc.negotiateVersion(self).get(), 3u);

        rpc.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, VersionNegotiationRefusesIncompatiblePeer) {
    seastar::async([] {
        const uint16_t serverPort = 39320, clientPort = 39321;
        const data::NodeId server = 2;
        ThrowingNodeStore s1, s2;
        data::DataPlaneRpc srv, cli;
        srv.setLocalVersion(features::VersionRange{5, 6});  // server speaks 5..6
        cli.setLocalVersion(features::VersionRange{1, 2});  // client speaks 1..2 (no overlap)
        srv.start(loopback(serverPort), s1).get();
        cli.start(loopback(clientPort), s2).get();
        cli.addPeer(server, loopback(serverPort));

        bool threw = false;
        try {
            cli.negotiateVersion(server).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "incompatible wire versions must be refused, not silently downgraded";

        cli.stop().get();
        srv.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, NodeCapabilityBindsNegotiatedRangeAndExpectedIdentity) {
    seastar::async([] {
        const uint16_t serverPort = 39387;
        const data::NodeId server = 2;
        ThrowingNodeStore sink;
        data::DataPlaneRpc srv, cli, oldCli;
        srv.setLocalVersion(features::VersionRange{1, data::kWriteBatchFormatV7});
        srv.setLocalNodeCapability(
            std::string(32, 'a'),
            control::NodeRecord{server, std::string(32, 'b'), "127.0.0.1:8087", "rack-b",
                                control::NodeState::Active});
        srv.start(loopback(serverPort), sink).get();

        cli.setLocalVersion(features::VersionRange{1, data::kWriteBatchFormatV7});
        cli.startClientOnly().get();
        cli.addPeer(server, loopback(serverPort));
        const auto capability = cli.nodeCapability(server).get();
        EXPECT_EQ(capability.clusterUuid, std::string(32, 'a'));
        EXPECT_EQ(capability.record.raftId, server);
        EXPECT_EQ(capability.formats.max, data::kWriteBatchFormatV7);

        cli.addPeer(3, loopback(serverPort));
        EXPECT_THROW(cli.nodeCapability(3).get(), data::NodeCapabilityMismatchError)
            << "a configured Raft id must not accept another node's advertisement";

        oldCli.setLocalVersion(features::VersionRange{1, data::kWriteBatchFormatV6});
        oldCli.startClientOnly().get();
        oldCli.addPeer(server, loopback(serverPort));
        EXPECT_THROW(oldCli.nodeCapability(server).get(), data::ClusterFormatUnsupportedError)
            << "pre-v7 clients must refuse before sending an unknown verb";

        oldCli.stop().get();
        cli.stop().get();
        srv.stop().get();
    }).get();
}

// A node with NO read-index sink set fails leader-reach cleanly (fail-closed).
TEST_F(DataPlaneRpcEnrichedTest, LeaderReachWithoutSinkFailsClosed) {
    seastar::async([] {
        const uint16_t port = 39318;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), sink).get();  // no setReadIndexSink
        rpc.addPeer(self, loopback(port));
        bool threw = false;
        try {
            rpc.leaderReadIndex(self, 5).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "leader-reach without a sink must fail, not hang/crash";
        rpc.stop().get();
    }).get();
}

// A NodeStore-started node with NO propose sink set fails proposeWrite cleanly
// (fail-closed) rather than hanging or crashing.
TEST_F(DataPlaneRpcEnrichedTest, ProposeWriteWithoutSinkFailsClosed) {
    seastar::async([] {
        const uint16_t port = 39316;
        const data::NodeId self = 1;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), sink).get();  // no setProposeSink
        rpc.addPeer(self, loopback(port));
        bool threw = false;
        try {
            rpc.proposeWrite(self, oneFloatBatch()).get();
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "proposeWrite without a sink must fail, not hang/crash";
        rpc.stop().get();
    }).get();
}

// start() is not idempotent -- a second call throws loudly before mutating state.
TEST_F(DataPlaneRpcEnrichedTest, StartTwiceThrows) {
    seastar::async([] {
        const uint16_t port = 39314;
        ThrowingNodeStore sink;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), sink).get();
        bool threw = false;
        try {
            rpc.start(loopback(port), sink).get();
        } catch (const std::logic_error&) {
            threw = true;
        }
        EXPECT_TRUE(threw);
        rpc.stop().get();
    }).get();
}

// write-scaleout 2c: a forwarded write is encoded at the version negotiated with
// THAT peer, so a mixed-version cluster degrades to v1 and an incompatible peer
// fails closed -- it must never receive a frame it cannot parse.
TEST_F(DataPlaneRpcEnrichedTest, ForwardedWritesSpeakTheNegotiatedWireVersion) {
    seastar::async([] {
        // Both nodes on this binary: negotiate the newest format, and the batch
        // survives the round trip through it.
        {
            const uint16_t port = 39325;
            const data::NodeId self = 1;
            RecordingProposeSink propose;
            ThrowingNodeStore sink;
            data::DataPlaneRpc rpc;
            rpc.setProposeSink(propose);
            rpc.start(loopback(port), sink).get();
            rpc.addPeer(self, loopback(port));
            // v3 and v4 are PROTOCOL steps (the hinted propose verb; the leader-resolve
            // read exchange), not payload formats: two peers on this binary negotiate
            // kWriteBatchFormatMax while the BYTES stay v2 (asserted by the wire-tap legs
            // below). Written against Max rather than a literal so the next protocol step
            // does not have to edit this line -- the point is "the newest, whatever it is".
            EXPECT_EQ(rpc.versionFor(self).get(), data::kWriteBatchFormatMax);
            EXPECT_GE(rpc.versionFor(self).get(), data::kWriteBatchFormatV3) << "the hinted verb must stay reachable";
            EXPECT_TRUE(rpc.proposeWrite(self, oneFloatBatch()).get());
            EXPECT_EQ(propose.lastSeriesCount, 1u);
            rpc.stop().get();
        }
        // A peer that only speaks v1 (an un-upgraded node): we negotiate DOWN to 1
        // and the write still lands. Nothing about the payload changes.
        //
        // The peer is a WIRE TAP, not another DataPlaneRpc pinned to {1,1}: a
        // DataPlaneRpc decodes v2 perfectly well whatever range it advertises, so a
        // regression that ignored the negotiated version and always emitted v2 would
        // pass against one. The tap answers the negotiate verb itself and captures the
        // RAW propose frame, so the assertion is on the bytes actually sent.
        {
            const uint16_t serverPort = 39326, clientPort = 39327;
            const data::NodeId server = 2;
            WireTapPeer tap(loopback(serverPort), /*agreedVersion=*/1);
            ThrowingNodeStore s2;
            data::DataPlaneRpc cli;
            cli.start(loopback(clientPort), s2).get();
            cli.addPeer(server, loopback(serverPort));

            EXPECT_EQ(cli.versionFor(server).get(), data::kWriteBatchFormatV1);
            EXPECT_TRUE(cli.proposeWrite(server, oneFloatBatch()).get());
            ASSERT_EQ(tap.captured.size(), 1u);
            EXPECT_NE(tap.captured[0].compare(0, 4, "TSW2"), 0)
                << "a peer that negotiated v1 must NOT be sent a v2-tagged frame";
            // ... and it is a frame that peer's decoder really accepts.
            EXPECT_TRUE(data::decodeWriteBatch(tap.captured[0]).has_value());
            cli.stop().get();
            tap.stop();
        }
        // The same tap agreeing on v2 DOES get a v2-tagged frame -- proving the leg
        // above is testing the gate and not merely a codec that never emits v2.
        {
            const uint16_t serverPort = 39330, clientPort = 39331;
            const data::NodeId server = 2;
            WireTapPeer tap(loopback(serverPort), /*agreedVersion=*/2);
            ThrowingNodeStore s2;
            data::DataPlaneRpc cli;
            cli.start(loopback(clientPort), s2).get();
            cli.addPeer(server, loopback(serverPort));

            EXPECT_EQ(cli.versionFor(server).get(), data::kWriteBatchFormatV2);
            EXPECT_TRUE(cli.proposeWrite(server, oneFloatBatch()).get());
            ASSERT_EQ(tap.captured.size(), 1u);
            EXPECT_EQ(tap.captured[0].compare(0, 4, "TSW2"), 0)
                << "a peer that negotiated v2 must be sent the v2 format";
            EXPECT_TRUE(data::decodeWriteBatch(tap.captured[0]).has_value());
            cli.stop().get();
            tap.stop();
        }
        // No overlapping version: the write FAILS rather than being mis-framed, and
        // the peer's sink never sees it.
        {
            const uint16_t serverPort = 39328, clientPort = 39329;
            const data::NodeId server = 2;
            RecordingProposeSink propose;
            ThrowingNodeStore s1, s2;
            data::DataPlaneRpc srv, cli;
            // Pin BOTH ends. Using the client's moving default made this
            // negative case silently become compatible when protocol v7 landed.
            srv.setLocalVersion(features::VersionRange{data::kWriteBatchFormatV7,
                                                       data::kWriteBatchFormatV7});
            cli.setLocalVersion(features::VersionRange{1, data::kWriteBatchFormatV6});
            srv.setProposeSink(propose);
            srv.start(loopback(serverPort), s1).get();
            cli.start(loopback(clientPort), s2).get();
            cli.addPeer(server, loopback(serverPort));

            bool threw = false;
            try {
                cli.proposeWrite(server, oneFloatBatch()).get();
            } catch (const std::exception&) {
                threw = true;
            }
            EXPECT_TRUE(threw) << "an incompatible peer must fail the write closed";
            EXPECT_EQ(propose.calls, 0) << "nothing may be applied on an incompatible peer";
            cli.stop().get();
            srv.stop().get();
        }
    }).get();
}

// Inbound admission is bounded (rpc::resource_limits). The shape that matters is LEGAL
// and so cannot be rejected by any decoder bound: ~1.29M empty Float series, each
// exactly the 13 wire bytes the codec charges for and 144 bytes resident, is a
// checksum-valid 16 MiB frame that used to decode into ~458 MB and be RETAINED (handed
// to applyWrites) -- unauthenticated remote memory exhaustion whenever mTLS is not
// configured. seastar refuses it before the handler runs, on a connection that stays up,
// so the caller sees a clean retryable failure.
TEST_F(DataPlaneRpcEnrichedTest, OversizedInboundFrameIsRefusedWithoutAmplifying) {
    seastar::async([] {
        const uint16_t port = 39332;
        ThrowingNodeStore sink;  // would throw "apply boom" IF the frame ever reached it
        data::DataPlaneRpc srv;
        srv.start(loopback(port), sink).get();

        // 1.29M series x 13B = ~16 MiB on the wire, ~458 MB resident if decoded.
        const std::string frame = emptySeriesFrame(1290554);
        ASSERT_GT(frame.size(), 16u << 20);

        RawPeerClient cli(loopback(port));
        const size_t before = peakRssKb();
        ASSERT_GT(before, 0u) << "no /proc/self/status VmHWM";
        std::string err;
        try {
            cli.sendRaw(frame);
            ADD_FAILURE() << "an over-budget frame must be refused";
        } catch (const std::exception& e) {
            err = e.what();
        }
        const size_t after = peakRssKb();

        // Refused by ADMISSION, not by the sink: "apply boom" would mean the frame was
        // decoded first (and the 458 MB already spent).
        EXPECT_NE(err.find("memory limit"), std::string::npos) << "expected an admission rejection, got: " << err;
        EXPECT_EQ(err.find("apply boom"), std::string::npos) << "the frame reached the handler: " << err;
        EXPECT_LT(after - before, 64u << 10) << "refusing a 16 MiB frame grew peak RSS by " << (after - before)
                                             << " KiB -- inbound admission is not bounding decode amplification";

        // The connection is NOT wedged: a normal write on the SAME connection still
        // works, so this is a retryable error rather than a dead peer.
        bool threwOnGood = false;
        try {
            cli.sendRaw(encodeWriteBatchForTest());
        } catch (const std::exception& e) {
            // ThrowingNodeStore always throws -- what matters is that it REACHED it.
            threwOnGood = std::string(e.what()).find("apply boom") != std::string::npos;
        }
        EXPECT_TRUE(threwOnGood) << "the connection must survive a refused frame";

        cli.stop();
        srv.stop().get();
    }).get();
}

// ---------------------------------------------------------------------------
// write-scaleout 3a: the hinted propose verb over a real socket.

namespace {
// A ProposeSink that answers the HINTED entry point: rejects a configured VShard and
// names the node that really leads it, commits everything else.
class HintingProposeSink : public data::ProposeSink {
public:
    uint16_t rejectVShard = 0xffff;
    data::NodeId hint = 0;
    int hintedCalls = 0;
    int plainCalls = 0;
    std::vector<uint16_t> lastVShards;

    seastar::future<bool> proposeBatch(data::WriteBatch batch) override {
        ++plainCalls;
        for (auto& g : data::splitByVShard(std::move(batch)))
            if (g.first == rejectVShard)
                return seastar::make_ready_future<bool>(false);
        return seastar::make_ready_future<bool>(true);
    }
    seastar::future<data::ProposeOutcome> proposeBatchHinted(data::WriteBatch batch) override {
        ++hintedCalls;
        data::ProposeOutcome out;
        lastVShards.clear();
        for (auto& g : data::splitByVShard(std::move(batch))) {
            lastVShards.push_back(g.first);
            if (g.first == rejectVShard)
                out.rejects.push_back(data::SliceReject{g.first, hint, data::WriteFailure::NotLeader});
            else
                out.committedVShards.push_back(g.first);  // AUTHORITATIVE half
        }
        out.committed = out.rejects.empty();
        return seastar::make_ready_future<data::ProposeOutcome>(std::move(out));
    }
};

uint16_t vshardOfBatch(const data::WriteBatch& b) {
    data::WriteSeries copy = b.series.front();
    return data::vshardOf(copy);
}
}  // namespace

// A not-leader rejection now names the ACTUAL leader, end to end over the socket. This
// is the v1 gap: a bare "0" left the coordinator re-routing to the same stale primary.
TEST_F(DataPlaneRpcEnrichedTest, HintedProposeCarriesTheRealLeaderBack) {
    seastar::async([] {
        const uint16_t port = 39360;
        const data::NodeId self = 1;
        HintingProposeSink sink;
        ThrowingNodeStore store;
        data::DataPlaneRpc rpc;
        auto batch = oneFloatBatch();
        const uint16_t vs = vshardOfBatch(batch);
        sink.rejectVShard = vs;
        sink.hint = 7;  // "node 7 leads it now"
        rpc.setProposeSink(sink);
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        data::VShardBatches groups = data::splitByVShard(std::move(batch));
        data::ProposeOutcome out = rpc.proposeWriteHinted(self, data::viewOf(groups), std::nullopt).get();
        EXPECT_FALSE(out.committed);
        // The AUTHORITATIVE half must survive the wire: nothing committed here, and the
        // caller must be able to see that without inferring it from the reject list.
        EXPECT_TRUE(out.committedVShards.empty());
        ASSERT_EQ(out.rejects.size(), 1u);
        EXPECT_EQ(out.rejects[0].vshard, vs);
        EXPECT_EQ(out.rejects[0].leaderHint, 7u) << "a not-leader reply must carry the real leader";
        EXPECT_EQ(sink.hintedCalls, 1);
        EXPECT_EQ(sink.plainCalls, 0) << "a v3 peer must be served by the hinted verb";

        // The same call once the sink leads it: a full commit, and the reply is the
        // byte-identical "1" the old verb used.
        sink.rejectVShard = 0xffff;
        data::ProposeOutcome ok = rpc.proposeWriteHinted(self, data::viewOf(groups), std::nullopt).get();
        EXPECT_TRUE(ok.committed);
        EXPECT_TRUE(ok.rejects.empty());
        // A '1' reply names no VShards on the wire; the client fills in what it asked
        // for, so the caller's committed-set arithmetic works uniformly.
        EXPECT_EQ(ok.committedVShards, std::vector<uint16_t>{vs});
        rpc.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, ReplicatedCommandCrossesV3SocketAndRejectsVShardSpoofing) {
    seastar::async([] {
        const uint16_t port = 39361;
        const data::NodeId self = 1;
        RecordingProposeSink sink;
        ThrowingNodeStore store;
        data::DataPlaneRpc rpc;
        rpc.setProposeSink(sink);
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        const std::string key = buildSeriesKey("delete", {{"host", "wire"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        const data::ReplicatedCommand command{
            data::DeleteRangeBatch{{data::DeleteRangeTarget{key, BASE, BASE + 10}},
                                   SeriesId128::fromHex("abcdef0123456789abcdef0123456789"),
                                   1'800'000'000'000}};
        const data::ProposeOutcome out = rpc.proposeCommandHinted(self, vshard, command, std::nullopt).get();
        EXPECT_TRUE(out.committed);
        EXPECT_EQ(out.committedVShards, std::vector<uint16_t>{vshard});
        EXPECT_EQ(sink.commandCalls, 1);
        EXPECT_EQ(sink.lastCommandVShard, vshard);
        EXPECT_EQ(sink.lastCommandBytes, data::encodeReplicatedCommand(command));

        sink.committed = false;
        sink.commandFailure = data::WriteFailure::Expired;
        const data::ProposeOutcome expired = rpc.proposeCommandHinted(self, vshard, command, std::nullopt).get();
        EXPECT_FALSE(expired.committed);
        ASSERT_EQ(expired.rejects.size(), 1u);
        EXPECT_EQ(expired.rejects[0].vshard, vshard);
        EXPECT_EQ(expired.rejects[0].kind, data::WriteFailure::Expired)
            << "receipt expiry must remain typed across the socket so the coordinator can return HTTP 409";

        const uint16_t wrong = static_cast<uint16_t>((vshard + 1) % timestar::VIRTUAL_SHARD_COUNT);
        std::string error;
        try {
            rpc.proposeCommandHinted(self, wrong, command, std::nullopt).get();
        } catch (const std::exception& e) {
            error = e.what();
        }
        EXPECT_NE(error.find("does not belong to its VShard"), std::string::npos) << error;
        EXPECT_EQ(sink.commandCalls, 2) << "a spoofed VShard prefix must be rejected before the sink";

        rpc.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, ReplicatedCommandIsNotSentToAPeerBelowV3) {
    seastar::async([] {
        const uint16_t serverPort = 39380, clientPort = 39381;
        const data::NodeId server = 2;
        WireTapPeer tap(loopback(serverPort), /*agreedVersion=*/2);
        ThrowingNodeStore store;
        data::DataPlaneRpc client;
        client.start(loopback(clientPort), store).get();
        client.addPeer(server, loopback(serverPort));

        const std::string key = buildSeriesKey("delete", {{"host", "old-peer"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        std::string error;
        try {
            client
                .proposeCommandHinted(server, vshard,
                                      data::ReplicatedCommand{data::DeleteRangeKey{key, BASE, BASE + 10}}, std::nullopt)
                .get();
        } catch (const std::exception& e) {
            error = e.what();
        }
        EXPECT_NE(error.find("does not support replicated commands"), std::string::npos) << error;
        EXPECT_TRUE(tap.captured.empty()) << "the command fell back to the write verb on an old peer";

        client.stop().get();
        tap.stop();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, BoundedDeleteIsNotSentToAPeerBelowV5) {
    seastar::async([] {
        const uint16_t serverPort = 39382, clientPort = 39383;
        const data::NodeId server = 2;
        WireTapPeer tap(loopback(serverPort), /*agreedVersion=*/4);
        ThrowingNodeStore store;
        data::DataPlaneRpc client;
        client.start(loopback(clientPort), store).get();
        client.addPeer(server, loopback(serverPort));

        const std::string key = buildSeriesKey("delete", {{"host", "v4-peer"}}, "value");
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        const data::ReplicatedCommand command{
            data::DeleteRangeBatch{{data::DeleteRangeTarget{key, BASE, BASE + 10}},
                                   SeriesId128::fromHex("abcdef0123456789abcdef0123456789"),
                                   1'800'000'000'000}};
        EXPECT_THROW(client.proposeCommandHinted(server, vshard, command, std::nullopt).get(),
                     data::ClusterFormatUnsupportedError);
        EXPECT_EQ(tap.negotiateCalls, 1);

        client.stop().get();
        tap.stop();
    }).get();
}

// A peer that predates v3 keeps getting the v1-shaped verb, with hintless rejects --
// a rolling upgrade must not turn a clean not-leader into a malformed-reply 5xx.
TEST_F(DataPlaneRpcEnrichedTest, HintedProposeFallsBackForAPeerBelowV3) {
    seastar::async([] {
        const uint16_t serverPort = 39362, clientPort = 39363;
        const data::NodeId server = 2;
        WireTapPeer tap(loopback(serverPort), /*agreedVersion=*/2);  // knows verb 6 only
        ThrowingNodeStore s2;
        data::DataPlaneRpc cli;
        cli.start(loopback(clientPort), s2).get();
        cli.addPeer(server, loopback(serverPort));

        EXPECT_EQ(cli.versionFor(server).get(), data::kWriteBatchFormatV2);
        data::VShardBatches groups = data::splitByVShard(oneFloatBatch());
        data::ProposeOutcome out = cli.proposeWriteHinted(server, data::viewOf(groups), std::nullopt).get();
        EXPECT_TRUE(out.committed) << "the tap answers verb 6 with a commit";
        EXPECT_EQ(out.committedVShards.size(), groups.size())
            << "a pre-v3 'committed' must still name what it committed, or the caller "
               "cannot tell a full commit from an empty one";
        ASSERT_EQ(tap.captured.size(), 1u) << "the pre-v3 peer must be reached on verb 6";
        EXPECT_EQ(tap.captured[0].compare(0, 4, "TSW2"), 0) << "the PAYLOAD version is unaffected by v3";
        cli.stop().get();
        tap.stop();
    }).get();
}

// An encoded slice larger than any peer's inbound admission fails LOCALLY and
// terminally (413-shaped), instead of being sent, refused opaquely, and retried.
TEST_F(DataPlaneRpcEnrichedTest, OversizedSliceIsRefusedLocallyAsTooLarge) {
    seastar::async([] {
        const uint16_t port = 39364;
        const data::NodeId self = 1;
        HintingProposeSink sink;
        ThrowingNodeStore store;
        data::DataPlaneRpc rpc;
        rpc.setProposeSink(sink);
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        // ~12 MiB of float points in one series: over the ~10.67 MiB ceiling.
        data::WriteBatch big;
        data::WriteSeries s;
        s.seriesKey = buildSeriesKey("m", {{"host", "h1"}}, "v");
        s.type = TSMValueType::Float;
        const size_t n = 1'400'000;  // 8B ts (v2 delta ~2B) + 8B value
        s.timestamps.reserve(n);
        std::vector<double> vals;
        vals.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            s.timestamps.push_back(BASE + i * 1'000'000'000ULL);
            vals.push_back(static_cast<double>(i));
        }
        s.values = std::move(vals);
        big.series.push_back(std::move(s));
        data::VShardBatches groups = data::splitByVShard(std::move(big));

        bool tooLarge = false;
        try {
            rpc.proposeWriteHinted(self, data::viewOf(groups), std::nullopt).get();
        } catch (const data::WriteFrameTooLargeError&) {
            tooLarge = true;
        } catch (const std::exception& e) {
            ADD_FAILURE() << "expected WriteFrameTooLargeError, got: " << e.what();
        }
        EXPECT_TRUE(tooLarge);
        EXPECT_EQ(sink.hintedCalls, 0) << "the frame must never have been sent";
        rpc.stop().get();
    }).get();
}

// Debt D-13, the wire half: a coordinator that does not HOST a VShard cannot resolve
// its leader, so it names it for the HOLDER to resolve. The holder answers for what it
// leads and REDIRECTS the rest, and both the resolve list and the redirects have to
// survive the socket -- they are optional tails on the two frames.
//
// The redirected VShard must NOT also appear in the partials: the coordinator is about
// to ask another node for it, and data returned twice is a double count of a replicated
// series, which is the one thing a leader read exists to prevent.
TEST_F(DataPlaneRpcEnrichedTest, ResolveVShardsAndRedirectsCrossTheSocket) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore inner(*eng);

        // Two series that land in DIFFERENT VShards; we "lead" one and not the other.
        data::WriteSeries ledSeries =
            series("rdr", {{"host", "led"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{11.0});
        data::WriteSeries otherSeries =
            series("rdr", {{"host", "other"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{22.0});
        const uint16_t ledVs = data::vshardOf(ledSeries);
        const uint16_t otherVs = data::vshardOf(otherSeries);
        ASSERT_NE(ledVs, otherVs) << "fixture needs two distinct VShards";

        const data::NodeId self = 1, realLeader = 4;
        data::LeaderFilteredNodeStore store(inner, self, [&](std::vector<uint16_t> vshards) {
            std::vector<data::VShardRedirect> out;
            for (uint16_t vs : vshards)
                out.push_back(vs == ledVs ? data::VShardRedirect{vs, self, true}
                                          : data::VShardRedirect{vs, realLeader, true});
            return seastar::make_ready_future<std::vector<data::VShardRedirect>>(std::move(out));
        });

        const uint16_t port = 39366;
        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        data::WriteBatch batch;
        batch.series.push_back(ledSeries);
        batch.series.push_back(otherSeries);
        rpc.forwardWriteBatch(self, batch).get();

        data::NodeQueryRequest req;
        // SUM, not LATEST: the two series share a timestamp, and the cluster rule is
        // "aggregate ACROSS SERIES at equal timestamps", so LATEST would tie-break to
        // one value and hide which VShards actually contributed.
        req.request.aggregation = AggregationMethod::SUM;
        req.request.measurement = "rdr";
        req.request.fields = {"v"};
        req.request.startTime = BASE - 1'000'000'000ULL;
        req.request.endTime = BASE + 1'000'000'000ULL;
        req.vshards = {ledVs, otherVs};
        req.resolveVShards = {ledVs, otherVs};

        data::NodeQueryPartial part = rpc.queryNode(self, req).get();
        ASSERT_TRUE(part.incompleteReasons.empty());
        ASSERT_EQ(part.redirects.size(), 1u) << "exactly the VShard we do not lead comes back";
        EXPECT_EQ(part.redirects[0].vshard, otherVs);
        EXPECT_EQ(part.redirects[0].leader, realLeader);
        EXPECT_TRUE(part.redirects[0].hosted);

        http::HttpQueryHandler handler(&*eng);
        auto answer =
            handler.finalizeClusterPartials(req.request, std::move(part.partials), std::move(part.nonNumeric)).get();
        ASSERT_TRUE(answer.success) << answer.errorMessage;
        // Only the LED VShard's series is in the answer -- 22.0 belongs to the node the
        // coordinator was redirected to.
        double sum = 0.0;
        for (const auto& s : answer.series)
            for (double d : doublesOf(s, "v"))
                sum += d;
        EXPECT_DOUBLE_EQ(sum, 11.0);

        // ... and with no resolve list the SAME request returns both, unchanged: the
        // RF == N path is untouched by any of this.
        req.resolveVShards.clear();
        data::NodeQueryPartial both = rpc.queryNode(self, req).get();
        EXPECT_TRUE(both.redirects.empty());
        auto answerBoth =
            handler.finalizeClusterPartials(req.request, std::move(both.partials), std::move(both.nonNumeric)).get();
        double sumBoth = 0.0;
        for (const auto& s : answerBoth.series)
            for (double d : doublesOf(s, "v"))
                sumBoth += d;
        EXPECT_DOUBLE_EQ(sumBoth, 33.0);

        rpc.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, PatternSeriesDiscoveryCrossesTheV4SocketWithoutLosingItsFence) {
    seastar::async([] {
        const uint16_t port = 39379;
        const data::NodeId self = 1;
        RecordingPatternStore store;
        store.response.seriesKeys = {buildSeriesKey("cpu", {{"env", "prod"}, {"host", "a"}}, "usage")};
        store.response.redirects = {{11, 4, true}};

        data::DataPlaneRpc rpc;
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        data::PatternSeriesRequest request;
        request.selector.measurement = "cpu";
        request.selector.tags = {{"env", "prod"}};
        request.selector.fields = {"usage"};
        request.vshards = {7, 11};
        request.resolveVShards = {11};
        request.mapEpoch = 42;
        request.maxSeries = 100;

        data::PatternSeriesResult result = rpc.findPatternSeries(self, request).get();
        ASSERT_EQ(store.calls, 1);
        EXPECT_EQ(store.lastRequest.selector.measurement, request.selector.measurement);
        EXPECT_EQ(store.lastRequest.selector.tags, request.selector.tags);
        EXPECT_EQ(store.lastRequest.selector.fields, request.selector.fields);
        EXPECT_EQ(store.lastRequest.vshards, request.vshards);
        EXPECT_EQ(store.lastRequest.resolveVShards, request.resolveVShards);
        EXPECT_EQ(store.lastRequest.mapEpoch, request.mapEpoch);
        EXPECT_EQ(store.lastRequest.maxSeries, request.maxSeries);
        EXPECT_EQ(result.seriesKeys, store.response.seriesKeys);
        ASSERT_EQ(result.redirects.size(), 1u);
        EXPECT_EQ(result.redirects.front().vshard, 11);
        EXPECT_EQ(result.redirects.front().leader, 4u);
        EXPECT_TRUE(result.redirects.front().hosted);

        rpc.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, PatternSeriesDiscoveryIsNotSentToAPeerBelowV4) {
    seastar::async([] {
        const uint16_t serverPort = 39380, clientPort = 39381;
        const data::NodeId peer = 2;
        WireTapPeer tap(loopback(serverPort), data::kWriteBatchFormatV3);
        ThrowingNodeStore store;
        data::DataPlaneRpc client;
        client.start(loopback(clientPort), store).get();
        client.addPeer(peer, loopback(serverPort));

        data::PatternSeriesRequest request;
        request.selector.measurement = "cpu";
        request.vshards = {7};
        request.maxSeries = 100;
        bool refused = false;
        try {
            client.findPatternSeries(peer, std::move(request)).get();
        } catch (const data::PatternSeriesUnsupportedError&) {
            refused = true;
        } catch (const std::exception& error) {
            ADD_FAILURE() << "expected PatternSeriesUnsupportedError, got: " << error.what();
        }
        EXPECT_TRUE(refused);
        EXPECT_EQ(tap.negotiateCalls, 1);

        client.stop().get();
        tap.stop();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, FrozenDeletePlanRequestsCrossTheV6Socket) {
    seastar::async([] {
        const uint16_t port = 39384;
        const data::NodeId self = 1;
        ThrowingNodeStore store;
        RecordingFrozenDeletePlanSink sink;
        data::DataPlaneRpc rpc;
        rpc.setFrozenDeletePlanSink(sink);
        rpc.start(loopback(port), store).get();
        rpc.addPeer(self, loopback(port));

        const control::FrozenDeletePlan identity{
            std::string(32, '1'), std::string(32, 'a'), 1'800'000'000'000, {}};
        const control::FrozenDeletePlan frozen{
            identity.requestId,
            identity.requestFingerprint,
            identity.issuedAtMs,
            {{buildSeriesKey("cpu", {{"host", "a"}}, "usage"), BASE, BASE + 10}}};
        sink.response = {control::FreezeDeletePlanStatus::Stored, frozen};
        auto lookup = rpc
                          .frozenDeletePlan(
                              self,
                              {control::FrozenDeletePlanRpcOperation::Lookup, identity},
                              seastar::rpc::rpc_clock_type::now() + std::chrono::seconds(1))
                          .get();
        ASSERT_EQ(sink.calls, 1);
        EXPECT_EQ(sink.lastRequest.operation, control::FrozenDeletePlanRpcOperation::Lookup);
        EXPECT_EQ(sink.lastRequest.plan, identity);
        EXPECT_EQ(lookup.status, control::FreezeDeletePlanStatus::Stored);
        EXPECT_EQ(lookup.plan, frozen);

        sink.response = {control::FreezeDeletePlanStatus::Conflict, frozen};
        auto conflict = rpc
                            .frozenDeletePlan(
                                self,
                                {control::FrozenDeletePlanRpcOperation::Freeze, frozen},
                                seastar::rpc::rpc_clock_type::now() + std::chrono::seconds(1))
                            .get();
        ASSERT_EQ(sink.calls, 2);
        EXPECT_EQ(sink.lastRequest.operation, control::FrozenDeletePlanRpcOperation::Freeze);
        EXPECT_EQ(sink.lastRequest.plan, frozen);
        EXPECT_EQ(conflict.status, control::FreezeDeletePlanStatus::Conflict);
        EXPECT_EQ(conflict.plan, frozen);

        rpc.stop().get();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, FrozenDeletePlanIsNotSentToAPeerBelowV6) {
    seastar::async([] {
        const uint16_t serverPort = 39384, clientPort = 39385;
        const data::NodeId peer = 2;
        WireTapPeer tap(loopback(serverPort), /*agreedVersion=*/5);
        ThrowingNodeStore store;
        data::DataPlaneRpc client;
        client.start(loopback(clientPort), store).get();
        client.addPeer(peer, loopback(serverPort));

        const control::FrozenDeletePlan identity{
            std::string(32, '1'), std::string(32, 'a'), 1'800'000'000'000, {}};
        EXPECT_THROW(client
                         .frozenDeletePlan(
                             peer,
                             {control::FrozenDeletePlanRpcOperation::Lookup, identity},
                             seastar::rpc::rpc_clock_type::now() + std::chrono::seconds(1))
                         .get(),
                     data::ClusterFormatUnsupportedError);
        EXPECT_EQ(tap.negotiateCalls, 1);

        client.stop().get();
        tap.stop();
    }).get();
}

// ---------------------------------------------------------------------------
// Debt D-25: the read path's wire-version negotiation.
//
// `resolveVShards` / `redirects` are OPTIONAL TAILS (D-13), and an absent reply tail says
// "I lead everything you named" and "I never read your resolve list" in exactly the same
// bytes. Without a version gate a new coordinator therefore cannot tell a leader read from
// a follower read against an old holder. The gate rides the SAME per-peer handshake the
// write path uses, so it costs one round trip per (shard, peer) connection and none at all
// on a read that names nothing to resolve.

// NEW -> OLD. A peer below the read protocol is never SENT a resolve list, and the refusal
// is a distinct type, so the coordinator can degrade knowingly instead of reporting a
// healthy node as unreachable.
TEST_F(DataPlaneRpcEnrichedTest, AResolveListIsRefusedForAPeerBelowTheReadProtocol) {
    seastar::async([] {
        const uint16_t oldPort = 39370, newPort = 39371, clientPort = 39372;
        const data::NodeId oldPeer = 2, newPeer = 3;
        WireTapPeer oldTap(loopback(oldPort), data::kWriteBatchFormatV3);  // pre-read-protocol
        WireTapPeer newTap(loopback(newPort), data::kNodeQueryResolveMinVersion);
        ThrowingNodeStore store;
        data::DataPlaneRpc cli;
        cli.start(loopback(clientPort), store).get();
        cli.addPeer(oldPeer, loopback(oldPort));
        cli.addPeer(newPeer, loopback(newPort));

        data::NodeQueryRequest req;
        req.request.measurement = "m";
        req.vshards = {3, 7};
        req.resolveVShards = {3, 7};

        bool refused = false;
        try {
            cli.queryNode(oldPeer, req).get();
        } catch (const data::ReadResolveUnsupportedError&) {
            refused = true;
        } catch (const std::exception& e) {
            ADD_FAILURE() << "expected ReadResolveUnsupportedError, got: " << e.what();
        }
        EXPECT_TRUE(refused);
        EXPECT_TRUE(oldTap.capturedQueries.empty())
            << "a peer that cannot honour a resolve list must never be sent one -- a pre-D-13 decoder "
               "refuses the whole frame, which the coordinator would read as an outage";

        // POSITIVE CONTROL, and it is the one that makes the refusal above mean something:
        // the SAME request to a peer that DOES negotiate the read protocol goes out with
        // its tail intact. Without this the test would pass against a client that had
        // simply stopped sending resolve lists to anyone.
        data::NodeQueryPartial ok = cli.queryNode(newPeer, req).get();
        EXPECT_TRUE(ok.redirects.empty());
        ASSERT_EQ(newTap.capturedQueries.size(), 1u);
        auto decoded = data::decodeNodeQueryRequest(newTap.capturedQueries[0]);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->resolveVShards, (std::vector<uint16_t>{3, 7}));

        cli.stop().get();
        oldTap.stop();
        newTap.stop();
    }).get();
}

// THE COST BOUND. A read that names nothing to resolve -- every RF == N read, every RF == 1
// read -- must not handshake at all, and an RF < N read must handshake ONCE per connection.
// A version check that added a round trip per read would be a worse bug than the one it
// closes.
TEST_F(DataPlaneRpcEnrichedTest, TheReadVersionHandshakeIsPaidOncePerPeerAndNeverWithoutATail) {
    seastar::async([] {
        const uint16_t serverPort = 39373, clientPort = 39374;
        const data::NodeId server = 2;
        WireTapPeer tap(loopback(serverPort), data::kWriteBatchFormatV3);
        ThrowingNodeStore store;
        data::DataPlaneRpc cli;
        cli.start(loopback(clientPort), store).get();
        cli.addPeer(server, loopback(serverPort));

        data::NodeQueryRequest plain;
        plain.request.measurement = "m";
        plain.vshards = {3, 7};  // ... and nothing to resolve
        cli.queryNode(server, plain).get();
        cli.queryNode(server, plain).get();
        EXPECT_EQ(tap.negotiateCalls, 0)
            << "the steady-state read path must not pay a handshake for a tail it does not send";
        EXPECT_EQ(tap.capturedQueries.size(), 2u) << "... and both reads must still have happened";

        // Now a read that DOES carry a tail: exactly one handshake, cached across calls
        // even though the answer was a refusal.
        data::NodeQueryRequest resolving = plain;
        resolving.resolveVShards = {3};
        EXPECT_THROW(cli.queryNode(server, resolving).get(), data::ReadResolveUnsupportedError);
        EXPECT_EQ(tap.negotiateCalls, 1);
        EXPECT_THROW(cli.queryNode(server, resolving).get(), data::ReadResolveUnsupportedError);
        EXPECT_EQ(tap.negotiateCalls, 1) << "the negotiated version is cached per connection, not re-asked per read";

        cli.stop().get();
        tap.stop();
    }).get();
}

// The REPLY half of the gate. A reply tail is permitted by the REQUEST, never by the
// server's own version (which a server cannot know the caller's side of), so a peer that
// volunteers redirects for a read that asked it to resolve nothing has drifted and its
// answer is refused rather than acted on.
TEST_F(DataPlaneRpcEnrichedTest, RedirectsFromAPeerAskedToResolveNothingAreRefused) {
    seastar::async([] {
        const uint16_t serverPort = 39375, clientPort = 39376;
        const data::NodeId server = 2;
        WireTapPeer tap(loopback(serverPort), data::kNodeQueryResolveMinVersion);
        tap.replyRedirects.push_back(data::VShardRedirect{7, 4, true});
        ThrowingNodeStore store;
        data::DataPlaneRpc cli;
        cli.start(loopback(clientPort), store).get();
        cli.addPeer(server, loopback(serverPort));

        data::NodeQueryRequest plain;
        plain.request.measurement = "m";
        plain.vshards = {3, 7};
        EXPECT_THROW(cli.queryNode(server, plain).get(), std::runtime_error);

        // NEGATIVE CONTROL: the same volunteered redirect is LEGITIMATE once the request
        // asks for it, so the refusal above is about the permission and not about the tail.
        data::NodeQueryRequest resolving = plain;
        resolving.resolveVShards = {7};
        data::NodeQueryPartial part = cli.queryNode(server, resolving).get();
        ASSERT_EQ(part.redirects.size(), 1u);
        EXPECT_EQ(part.redirects[0].vshard, 7);
        EXPECT_EQ(part.redirects[0].leader, 4u);

        cli.stop().get();
        tap.stop();
    }).get();
}

TEST_F(DataPlaneRpcEnrichedTest, QueryNodeDeadlineBoundsABlackHoledPeer) {
    seastar::async([] {
        const uint16_t serverPort = 39377, clientPort = 39378;
        const data::NodeId server = 2;
        WireTapPeer tap(loopback(serverPort), data::kNodeQueryResolveMinVersion);
        tap.blockQueries = true;
        ThrowingNodeStore store;
        data::DataPlaneRpc cli;
        cli.start(loopback(clientPort), store).get();
        cli.addPeer(server, loopback(serverPort));

        data::NodeQueryRequest req;
        req.request.measurement = "m";
        req.vshards = {7};
        const auto started = std::chrono::steady_clock::now();
        bool timedOut = false;
        try {
            cli.queryNode(server, req, seastar::rpc::rpc_clock_type::now() + std::chrono::milliseconds(150)).get();
        } catch (...) {
            timedOut = true;
        }
        const auto elapsed = std::chrono::steady_clock::now() - started;
        EXPECT_TRUE(timedOut) << "a peer that accepts the query and never answers must not hang the coordinator";
        EXPECT_LT(elapsed, std::chrono::seconds(1));
        ASSERT_EQ(tap.capturedQueries.size(), 1u) << "the deadline test must reach the peer, not fail to connect";

        tap.releaseBlockedQuery();
        cli.stop().get();
        tap.stop();
    }).get();
}
