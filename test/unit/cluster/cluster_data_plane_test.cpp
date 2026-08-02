// Integration M2: the ClusterDataPlane composition service the server instantiates.
// A single-node instance starts its real DataPlaneRpc listener, routes a WriteBatch
// through NodeWriteRouter into the real Engine, and answers query() through the
// NodeQueryCoordinator -- proving the whole M2 brick composition works end to end as
// one service (the cross-NODE socket forwarding is proven by dataplane_rpc_enriched_test;
// here every VShard is local, exercising the service wiring + lifecycle).
#include "../../../lib/cluster/integration/cluster_data_plane.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class ClusterDataPlaneTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

TEST_F(ClusterDataPlaneTest, ReplicatedStartupRejectsNonCohesiveCoreCounts) {
    EXPECT_NO_THROW(cluster::ClusterDataPlane::validateCoreTopology(1, 3));
    EXPECT_NO_THROW(cluster::ClusterDataPlane::validateCoreTopology(2, 3));
    EXPECT_NO_THROW(cluster::ClusterDataPlane::validateCoreTopology(4, 3));
    EXPECT_THROW(cluster::ClusterDataPlane::validateCoreTopology(3, 3), std::invalid_argument);
    EXPECT_THROW(cluster::ClusterDataPlane::validateCoreTopology(6, 3), std::invalid_argument);
    EXPECT_NO_THROW(cluster::ClusterDataPlane::validateCoreTopology(3, 1));
}

TEST_F(ClusterDataPlaneTest, ClusterReadinessFailsClosedOnCurrentServingBlockers) {
    cluster::ClusterDataPlane::Status st;
    st.replicated = true;
    st.vshardsHostedHere = 10;
    st.snapshotTriggerEnabled = true;
    st.activeClusterFormat = data::kSnapshotV2ActivationVersion;
    st.snapshotFormatReady = true;
    EXPECT_TRUE(st.readyForTraffic());

    st.unresolvedPeerCount = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("unresolved"), std::string::npos);
    st.unresolvedPeerCount = 0;

    st.vshardsLeaderless = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("no elected leader"), std::string::npos);
    st.vshardsLeaderless = 0;

    st.applyLagEntries = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("not applied"), std::string::npos);
    st.applyLagEntries = 0;

    st.applyFailures = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("apply failure"), std::string::npos);
    st.applyFailures = 0;

    st.tickErrors = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("tick error"), std::string::npos);
    st.tickErrors = 0;

    st.snapshotTriggerEnabled = false;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("snapshot"), std::string::npos);
    st.snapshotTriggerEnabled = true;

    st.activeClusterFormat = 1;
    st.snapshotFormatReady = false;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("below snapshot requirement v2"), std::string::npos);
    st.activeClusterFormat = data::kSnapshotV2ActivationVersion;
    st.snapshotFormatReady = true;

    st.snapshotsRefusedTooLarge = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("size bound"), std::string::npos);
    st.snapshotsRefusedTooLarge = 0;

    st.snapshotsUndeliverable = 1;
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("undeliverable"), std::string::npos);
}

TEST_F(ClusterDataPlaneTest, ControlHealthIsVisibleWithoutBlockingExistingDataGroups) {
    cluster::ClusterDataPlane::Status st;
    st.replicated = true;
    st.vshardsHostedHere = 10;
    st.snapshotTriggerEnabled = true;
    st.snapshotFormatReady = true;
    EXPECT_TRUE(st.controlLocallyReady());
    EXPECT_TRUE(st.readyForTraffic());

    st.controlEnabled = true;
    EXPECT_FALSE(st.controlLocallyReady());
    EXPECT_TRUE(st.readyForTraffic()) << "control quorum loss must not stop existing data groups";

    st.controlHosted = true;
    st.controlInitialized = true;
    st.controlServingMapEpoch = 1;
    st.controlLeader = 1;
    st.controlTerm = 7;
    st.controlControllerLeader = 1;
    st.controlControllerTerm = 7;
    st.controlCurrentTermCommit = true;
    st.controlCapabilitiesComplete = true;
    EXPECT_TRUE(st.controlLocallyReady());

    st.controlIdentityConflict = true;
    EXPECT_FALSE(st.controlLocallyReady());
    EXPECT_FALSE(st.readyForTraffic());
    EXPECT_NE(st.readinessReason().find("conflicting persistent cluster identity"), std::string::npos);
    st.controlIdentityConflict = false;

    st.controlControllerTerm = 6;
    EXPECT_FALSE(st.controlLocallyReady()) << "a leader is not actuating until its Raft term is durably stamped";
    st.controlControllerTerm = 7;
    st.controlApplyLagEntries = 1;
    EXPECT_FALSE(st.controlLocallyReady());
    EXPECT_TRUE(st.readyForTraffic());
    st.controlApplyLagEntries = 0;
    st.controlCompactionsRefusedTooLarge = 1;
    EXPECT_FALSE(st.controlLocallyReady());
}

data::WriteSeries series(const std::string& m, std::map<std::string, std::string> tags, const std::string& f,
                         TSMValueType type, std::vector<uint64_t> ts,
                         std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>,
                                      std::vector<std::string>>
                             vals) {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey(m, tags, f);
    s.type = type;
    s.timestamps = std::move(ts);
    s.values = std::move(vals);
    return s;
}
}  // namespace

TEST_F(ClusterDataPlaneTest, SingleNodeStartWriteQuery) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();

        ClusterConfig cfg;
        cfg.enabled = true;
        cfg.node_id = 1;
        cfg.peers = {"127.0.0.1:18099"};  // data-plane listener binds 127.0.0.1:19099

        cluster::ClusterDataPlane cdp;
        cdp.start(cfg, *eng).get();
        EXPECT_EQ(cdp.selfId(), 1);

        // Write a float and a string series through the service.
        data::WriteBatch b;
        b.series.push_back(series("temp", {{"host", "h1"}}, "value", TSMValueType::Float, {BASE},
                                  std::vector<double>{42.5}));
        b.series.push_back(series("log", {{"host", "h1"}}, "msg", TSMValueType::String, {BASE},
                                  std::vector<std::string>{"through the service"}));
        cdp.write(std::move(b)).get();

        auto query = [&](const std::string& m, const std::string& f) {
            QueryRequest q;
            q.aggregation = AggregationMethod::LATEST;
            q.measurement = m;
            q.fields = {f};
            q.startTime = BASE - 1'000'000'000ULL;
            q.endTime = BASE + 1'000'000'000ULL;
            return cdp.query(q).get();
        };

        auto rd = query("temp", "value");
        ASSERT_TRUE(rd.success) << rd.errorMessage;
        ASSERT_EQ(rd.series.size(), 1u);
        EXPECT_EQ(std::get<std::vector<double>>(rd.series[0].fields.at("value").second)[0], 42.5);

        auto rs = query("log", "msg");
        ASSERT_TRUE(rs.success) << rs.errorMessage;
        ASSERT_EQ(rs.series.size(), 1u);
        EXPECT_EQ(std::get<std::vector<std::string>>(rs.series[0].fields.at("msg").second)[0], "through the service");

        // Metadata scatter (single node: this node's owned schema).
        {
            auto meas = cdp.metadata({data::MetadataKind::Measurements, "", "", ""}).get();
            std::sort(meas.items.begin(), meas.items.end());
            EXPECT_EQ(meas.items, (std::vector<std::string>{"log", "temp"}));
            auto card = cdp.metadata({data::MetadataKind::MeasurementCardinality, "temp", "", ""}).get();
            EXPECT_GE(card.cardinality, 1.0);
        }

        cdp.stop().get();
    }).get();
}

// M4: every read-consistency mode returns the SAME correct answer through the
// cluster query path. A leader read satisfies linearizable ⊇ session ⊇ bounded, so
// until replica routing offloads reads for SCALING, non-leader modes are served
// correctly (never rejected or mis-served) -- the field is handled, not ignored in a
// way that breaks the query.
TEST_F(ClusterDataPlaneTest, ReadConsistencyModesAllReturnCorrectAnswer) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();

        ClusterConfig cfg;
        cfg.enabled = true;
        cfg.node_id = 1;
        cfg.peers = {"127.0.0.1:18097"};  // data-plane listener binds 127.0.0.1:19097

        cluster::ClusterDataPlane cdp;
        cdp.start(cfg, *eng).get();

        data::WriteBatch b;
        b.series.push_back(series("cm", {{"host", "h1"}}, "value", TSMValueType::Float, {BASE},
                                  std::vector<double>{5.5}));
        cdp.write(std::move(b)).get();

        auto queryAt = [&](ReadConsistencyMode mode, uint64_t maxLag) {
            QueryRequest q;
            q.aggregation = AggregationMethod::LATEST;
            q.measurement = "cm";
            q.fields = {"value"};
            q.startTime = BASE - 1'000'000'000ULL;
            q.endTime = BASE + 1'000'000'000ULL;
            q.readConsistency = mode;
            q.maxReadLagIndex = maxLag;
            return cdp.query(q).get();
        };

        for (auto [mode, lag] : {std::pair{ReadConsistencyMode::Leader, uint64_t{0}},
                                 std::pair{ReadConsistencyMode::Session, uint64_t{0}},
                                 std::pair{ReadConsistencyMode::BoundedStaleness, uint64_t{1000}}}) {
            auto r = queryAt(mode, lag);
            ASSERT_TRUE(r.success) << r.errorMessage << " (mode " << static_cast<int>(mode) << ")";
            ASSERT_EQ(r.series.size(), 1u);
            EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(r.series[0].fields.at("value").second)[0], 5.5)
                << "mode " << static_cast<int>(mode);
        }

        cdp.stop().get();
    }).get();
}

TEST_F(ClusterDataPlaneTest, MisconfiguredFailsToStart) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        ClusterConfig cfg;
        cfg.enabled = true;
        cfg.node_id = 5;              // out of range for a 1-peer cluster
        cfg.peers = {"127.0.0.1:18098"};
        cluster::ClusterDataPlane cdp;
        EXPECT_THROW(cdp.start(cfg, *eng).get(), std::invalid_argument);
        cdp.stop().get();
    }).get();
}
