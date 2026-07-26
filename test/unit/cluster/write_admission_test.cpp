// write-scaleout 3b/3d: the write-path failure TAXONOMY and the per-shard in-flight
// byte bound. Pure logic -- no sockets, no Raft.
#include "../../../lib/cluster/integration/write_admission.hpp"

#include "../../../lib/cluster/data/dataplane_rpc.hpp"
#include "../../../lib/cluster/data/write_errors.hpp"
#include "../../../lib/cluster/data/write_record.hpp"
#include "../../../lib/cluster/integration/cluster_data_plane.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <stdexcept>

using namespace timestar;
using timestar::data::WriteFailure;

namespace {

data::WriteBatch floatBatch(size_t series, size_t points) {
    data::WriteBatch b;
    for (size_t i = 0; i < series; ++i) {
        data::WriteSeries s;
        s.seriesKey = buildSeriesKey("m", {{"host", "h" + std::to_string(i)}}, "v");
        s.type = TSMValueType::Float;
        std::vector<double> v;
        for (size_t k = 0; k < points; ++k) {
            s.timestamps.push_back(1'700'000'000'000'000'000ULL + k);
            v.push_back(static_cast<double>(k));
        }
        s.values = std::move(v);
        b.series.push_back(std::move(s));
    }
    return b;
}

}  // namespace

// Every class is either retryable or terminal, and the AMBIGUOUS ones are exactly the
// two where the proposal may already have committed. If this table is edited, the audit
// in write_errors.hpp has to be re-read: a class that becomes retryable also becomes a
// candidate for the LWW re-apply argument.
TEST(WriteFailureTaxonomyTest, RetryAndAmbiguityPolicyIsExplicit) {
    EXPECT_TRUE(data::isRetryableWriteFailure(WriteFailure::NotLeader));
    EXPECT_TRUE(data::isRetryableWriteFailure(WriteFailure::LeadershipLost));
    EXPECT_TRUE(data::isRetryableWriteFailure(WriteFailure::Transport));
    EXPECT_TRUE(data::isRetryableWriteFailure(WriteFailure::ShardStopping));
    EXPECT_TRUE(data::isRetryableWriteFailure(WriteFailure::Overloaded));
    EXPECT_FALSE(data::isRetryableWriteFailure(WriteFailure::Unassigned));
    EXPECT_FALSE(data::isRetryableWriteFailure(WriteFailure::Fatal));
    EXPECT_FALSE(data::isRetryableWriteFailure(WriteFailure::None));

    EXPECT_TRUE(data::isAmbiguousWriteFailure(WriteFailure::LeadershipLost));
    EXPECT_TRUE(data::isAmbiguousWriteFailure(WriteFailure::Transport));
    EXPECT_FALSE(data::isAmbiguousWriteFailure(WriteFailure::NotLeader))
        << "a refused proposal appended nothing -- calling it ambiguous would be wrong";
    EXPECT_FALSE(data::isAmbiguousWriteFailure(WriteFailure::ShardStopping));
    EXPECT_FALSE(data::isAmbiguousWriteFailure(WriteFailure::Overloaded));
}

// A LOCAL failure is classified strictly: only what we recognise is retried, so a retry
// budget is never spent hiding a bug in this process.
TEST(WriteFailureTaxonomyTest, LocalClassificationIsConservative) {
    auto cls = [](std::exception_ptr e) { return data::classifyLocalWriteFailure(e); };
    EXPECT_EQ(cls(std::make_exception_ptr(raft::LeadershipLostError("lost"))), WriteFailure::LeadershipLost);
    EXPECT_EQ(cls(std::make_exception_ptr(data::WriteOverloadedError("full"))), WriteFailure::Overloaded);
    EXPECT_EQ(cls(std::make_exception_ptr(data::UnassignedVShardError("none"))), WriteFailure::Unassigned);
    EXPECT_EQ(cls(std::make_exception_ptr(data::WriteFrameTooLargeError("big"))), WriteFailure::Fatal);
    EXPECT_EQ(cls(std::make_exception_ptr(data::ShardStoppingError("shard data plane is stopping"))),
              WriteFailure::ShardStopping);
    // Matched by TYPE now, not by message. A plain runtime_error wearing the same words is
    // NOT a shard-stopping condition and must stay Fatal -- the string match it replaced
    // was the classifier's one remaining wart, and it was also why the HTTP batch path
    // could not recognise the condition at all.
    EXPECT_EQ(
        cls(std::make_exception_ptr(std::runtime_error("cluster: shard data plane is stopping; retry this write"))),
        WriteFailure::Fatal);
    EXPECT_EQ(cls(std::make_exception_ptr(std::runtime_error("journal write failed: EIO"))), WriteFailure::Fatal)
        << "an unrecognised local failure must NOT be retried";
}

// A REMOTE failure is classified the other way round: a peer that errors is an
// availability problem (the same judgement the query path already makes), so it is
// retryable -- except for the two we raise ourselves before the frame leaves.
TEST(WriteFailureTaxonomyTest, RemoteClassificationTreatsPeerErrorsAsAvailability) {
    auto cls = [](std::exception_ptr e) { return data::classifyRemoteWriteFailure(e); };
    EXPECT_EQ(cls(std::make_exception_ptr(std::runtime_error("connection is closed"))), WriteFailure::Transport);
    EXPECT_EQ(cls(std::make_exception_ptr(std::runtime_error("dataplane: unknown peer"))), WriteFailure::Transport);
    EXPECT_EQ(cls(std::make_exception_ptr(data::WriteFrameTooLargeError("big"))), WriteFailure::Fatal);
    EXPECT_EQ(cls(std::make_exception_ptr(data::UnassignedVShardError("none"))), WriteFailure::Unassigned);
}

// The in-flight bound admits, charges, releases, and REJECTS rather than queueing.
TEST(WriteAdmissionTest, BoundRejectsRatherThanQueues) {
    auto& adm = cluster::WriteAdmission::local();
    const size_t lim = cluster::WriteAdmission::limitBytes();
    ASSERT_EQ(adm.inFlight(), 0u) << "a previous test leaked a charge";

    {
        cluster::WriteAdmissionGuard g(lim / 2);
        EXPECT_EQ(adm.inFlight(), lim / 2);
        // Still under the bound: admitted.
        cluster::WriteAdmissionGuard g2(lim / 4);
        EXPECT_EQ(adm.inFlight(), lim / 2 + lim / 4);
        // Over: rejected, and the budget is unchanged (all-or-nothing).
        bool rejected = false;
        try {
            cluster::WriteAdmissionGuard g3(lim);
        } catch (const data::WriteOverloadedError&) {
            rejected = true;
        }
        EXPECT_TRUE(rejected);
        EXPECT_EQ(adm.inFlight(), lim / 2 + lim / 4) << "a rejected batch must not be charged";
    }
    EXPECT_EQ(adm.inFlight(), 0u) << "the guard must release on every exit";

    // A single batch bigger than the whole budget is still admitted on an idle shard --
    // it would otherwise be permanently unwritable.
    {
        cluster::WriteAdmissionGuard big(lim * 4);
        EXPECT_EQ(adm.inFlight(), lim * 4);
    }
    EXPECT_EQ(adm.inFlight(), 0u);
}

// TWO BUDGETS, ONE PER DOOR (debt D-8). What a node ORIGINATES and what a peer pushes at
// it are bounded separately, so neither role can starve the other: a burst of replication
// must not 503 a client's own writes on a node that is coordinating almost nothing, and a
// busy coordinator must not turn away replication it is the LEADER for.
TEST(WriteAdmissionTest, TheOriginatedAndIngressBudgetsAreIndependent) {
    auto& orig = cluster::WriteAdmission::local(cluster::AdmissionClass::Originated);
    auto& ingress = cluster::WriteAdmission::local(cluster::AdmissionClass::PeerIngress);
    ASSERT_EQ(orig.inFlight(), 0u) << "a previous test leaked a charge";
    ASSERT_EQ(ingress.inFlight(), 0u);
    EXPECT_NE(&orig, &ingress) << "one counter for both doors is the starvation shape D-8 rejects";

    const size_t lim = cluster::WriteAdmission::limitBytes(cluster::AdmissionClass::PeerIngress);
    {
        // Fill the INGRESS budget completely.
        cluster::WriteAdmissionGuard full(lim, cluster::AdmissionClass::PeerIngress);
        EXPECT_EQ(ingress.inFlight(), lim);
        EXPECT_EQ(orig.inFlight(), 0u) << "an ingress charge must not touch the originated counter";

        bool ingressRejected = false;
        try {
            cluster::WriteAdmissionGuard more(lim, cluster::AdmissionClass::PeerIngress);
        } catch (const data::WriteOverloadedError& e) {
            ingressRejected = true;
            EXPECT_NE(std::string(e.what()).find("peer-ingress"), std::string::npos)
                << "the message must name the door that is full: " << e.what();
        }
        EXPECT_TRUE(ingressRejected);

        // ... and a client's own write still goes through on the same shard.
        EXPECT_NO_THROW({ cluster::WriteAdmissionGuard mine(lim); });
    }
    EXPECT_EQ(ingress.inFlight(), 0u) << "the guard must release to the class it charged";
    EXPECT_EQ(orig.inFlight(), 0u);

    // The mirror image: a full ORIGINATED budget does not stop replication we lead.
    {
        cluster::WriteAdmissionGuard full(cluster::WriteAdmission::limitBytes(), cluster::AdmissionClass::Originated);
        EXPECT_THROW(cluster::WriteAdmissionGuard(cluster::WriteAdmission::limitBytes()), data::WriteOverloadedError);
        EXPECT_NO_THROW({ cluster::WriteAdmissionGuard peer(1024, cluster::AdmissionClass::PeerIngress); });
    }
    EXPECT_EQ(orig.inFlight(), 0u);
    EXPECT_EQ(ingress.inFlight(), 0u);
}

// The size estimate the bound is charged in tracks the payload, not the object count.
TEST(WriteAdmissionTest, ResidentEstimateTracksThePayload) {
    const size_t small = data::approxResidentBytes(floatBatch(1, 10));
    const size_t big = data::approxResidentBytes(floatBatch(1, 10'000));
    EXPECT_GT(big, small * 100);
    // 10k float points = 10k timestamps (8B) + 10k values (8B) + the key.
    EXPECT_GE(big, 10'000u * 16u);

    data::VShardBatches groups = data::splitByVShard(floatBatch(20, 100));
    EXPECT_EQ(data::approxResidentBytes(data::viewOf(groups)), data::approxResidentBytes(floatBatch(20, 100)));
}

// A node must advertise the NEWEST wire version this binary supports, and this asserts
// the ADVERTISED VALUE, not the source text that produces it.
//
// The bug it guards: ClusterDataPlane::localVersion_ still said kWriteBatchFormatV2 after
// v3 landed, so every negotiation capped at 2, no peer spoke the hinted-propose verb, and
// the whole leader-hint path was dead in production with a green suite -- unit and socket
// tests build their own DataPlaneRpc, whose default was already correct.
//
// A source grep for the initializer (the first version of this test) was too weak: it
// cannot see DataPlaneRpc's own default regressing, a third advertiser appearing, or a
// setLocalVersion({1,2}) overriding the initializer afterwards. Reading the value back
// from each advertiser sees all three.
TEST(WriteAdmissionTest, EveryAdvertiserOffersTheNewestSupportedWireVersion) {
    // Advertiser 1: the transport's own default (what a peer handshakes against).
    data::DataPlaneRpc rpc;
    EXPECT_EQ(rpc.localVersion().min, 1u);
    EXPECT_EQ(rpc.localVersion().max, data::kWriteBatchFormatMax)
        << "DataPlaneRpc must advertise the newest format this binary can write";

    // Advertiser 2: the NODE, which pushes its range to every per-shard transport in
    // start(). This is the one that regressed.
    cluster::ClusterDataPlane node;
    EXPECT_EQ(node.localVersion().min, 1u);
    EXPECT_EQ(node.localVersion().max, data::kWriteBatchFormatMax)
        << "ClusterDataPlane must advertise the newest format this binary can write; "
           "pinning a literal silently disables every protocol step newer than it, cluster-wide";

    // ... and a narrowed range still round-trips, so the getter reads the live value
    // rather than a constant (a getter returning kWriteBatchFormatMax unconditionally
    // would pass everything above and prove nothing).
    rpc.setLocalVersion(features::VersionRange{1, 1});
    EXPECT_EQ(rpc.localVersion().max, 1u);

    // The protocol step the hinted-propose verb is gated on must be the newest one.
    EXPECT_EQ(data::kWriteBatchFormatMax, data::kWriteBatchFormatV3);
}

// ... and the list of advertisers must be EXHAUSTIVE.
//
// Reading the value back from the two known advertisers cannot see a THIRD one appearing,
// nor a setLocalVersion() call that overrides a correct initializer after start(). Both
// are how the original bug would come back. So: enumerate every site in the tree that
// declares or sets an advertised range, and require it to be one of the reviewed ones. A
// new site fails this test and has to be added deliberately -- which is the point.
TEST(WriteAdmissionTest, TheAdvertiserListIsExhaustive) {
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
    // (file, needle, why it is allowed to exist)
    const std::vector<std::array<std::string, 2>> reviewed = {
        // The two DEFAULTS, both of which must name kWriteBatchFormatMax.
        {"lib/cluster/data/dataplane_rpc.cpp", "features::VersionRange localVersion{1, kWriteBatchFormatMax};"},
        {"lib/cluster/integration/cluster_data_plane.hpp", "localVersion_{1, data::kWriteBatchFormatMax}"},
        // The one PROPAGATION path: the node pushes its range to every per-shard
        // transport. It forwards a value, it does not invent one.
        {"lib/cluster/integration/cluster_data_plane.cpp", "rpc_->setLocalVersion(localVersion_)"},
        {"lib/cluster/integration/shard_raft_plane.hpp", "rpc_->setLocalVersion(localVersion)"},
    };
    for (const auto& [file, needle] : reviewed) {
        const std::string src = readSource(file);
        ASSERT_FALSE(src.empty()) << "could not locate " << file;
        EXPECT_NE(src.find(needle), std::string::npos)
            << file << " no longer contains the reviewed advertiser/propagation: " << needle;
    }

    // Now the exhaustiveness half: no OTHER file may declare or set an advertised range.
    const std::vector<std::string> allowed = {
        "lib/cluster/data/dataplane_rpc.cpp", "lib/cluster/data/dataplane_rpc.hpp",
        "lib/cluster/integration/cluster_data_plane.cpp", "lib/cluster/integration/cluster_data_plane.hpp",
        "lib/cluster/integration/shard_raft_plane.hpp"};
    for (const std::string& prefix : {"", "../", "../../", "../../../"}) {
        if (!std::filesystem::exists(prefix + std::string("lib/cluster")))
            continue;
        std::vector<std::string> offenders;
        for (auto& e : std::filesystem::recursive_directory_iterator(prefix + std::string("lib"))) {
            if (!e.is_regular_file())
                continue;
            const std::string ext = e.path().extension().string();
            if (ext != ".cpp" && ext != ".hpp")
                continue;
            std::string rel = e.path().string().substr(std::string(prefix).size());
            std::ifstream in(e.path());
            std::ostringstream ss;
            ss << in.rdbuf();
            const std::string body = ss.str();
            const bool touches = body.find("setLocalVersion(") != std::string::npos ||
                                 body.find("VersionRange localVersion") != std::string::npos;
            if (touches && std::find(allowed.begin(), allowed.end(), rel) == allowed.end())
                offenders.push_back(rel);
        }
        EXPECT_TRUE(offenders.empty())
            << "a NEW wire-version advertiser appeared and was not reviewed: " << offenders.front()
            << " -- add it to this list only after checking it advertises kWriteBatchFormatMax";
        return;  // one prefix resolved; done
    }
    GTEST_SKIP() << "could not locate the lib tree from the test's working directory";
}
