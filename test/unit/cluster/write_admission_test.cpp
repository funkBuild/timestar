// write-scaleout 3b/3d: the write-path failure TAXONOMY and the per-shard in-flight
// byte bound. Pure logic -- no sockets, no Raft.
#include "../../../lib/cluster/data/write_errors.hpp"
#include "../../../lib/cluster/data/write_record.hpp"
#include "../../../lib/cluster/integration/write_admission.hpp"
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
    EXPECT_EQ(cls(std::make_exception_ptr(
                  std::runtime_error("cluster: shard data plane is stopping; retry this write"))),
              WriteFailure::ShardStopping);
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
