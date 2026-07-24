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
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>
#include <seastar/net/socket_defs.hh>

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

data::WriteSeries series(const std::string& m, std::map<std::string, std::string> tags, const std::string& field,
                         TSMValueType type, std::vector<uint64_t> ts,
                         std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>,
                                      std::vector<std::string>>
                             vals) {
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
    int calls = 0;
    size_t lastSeriesCount = 0;
    seastar::future<bool> proposeBatch(data::WriteBatch batch) override {
        ++calls;
        lastSeriesCount = batch.series.size();
        return seastar::make_ready_future<bool>(committed);
    }
};

data::WriteBatch oneFloatBatch() {
    data::WriteBatch b;
    b.series.push_back(
        series("m", {{"host", "h1"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{1.0}));
    return b;
}

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
        batch.series.push_back(
            series("log", {{"host", "h1"}}, "msg", TSMValueType::String, {BASE}, std::vector<std::string>{"over the wire"}));
        batch.series.push_back(
            series("m", {{"host", "h1"}, {"region", "west"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{10.0}));
        batch.series.push_back(
            series("m", {{"host", "h2"}, {"region", "west"}}, "v", TSMValueType::Float, {BASE}, std::vector<double>{30.0}));

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
        srv.start(loopback(serverPort), legacy).get();   // legacy path only (verbs 1/2)
        cli.start(loopback(clientPort), unused).get();   // enriched path
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
