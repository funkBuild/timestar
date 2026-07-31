// THE WRITE-SLICE SIZE CHAIN (debt D-31).
//
// The Raft proposal bound used to be a round number picked above every producer; it is
// now DERIVED from the largest write slice the data plane will carry. These tests pin the
// two things that derivation rests on:
//
//   1. `maxEncodedBytes` really is an upper bound over EVERY format version the codec can
//      emit -- not the version the caller happens to hold. The two ends of a forwarded
//      write disagree about version by design (the wire is per-peer negotiated, the Raft
//      command is gated cluster-wide), so a refusal computed at one end must charge the
//      worst version or it admits something the encoder then exceeds.
//   2. `firstUnproposableSlice` finds the offending group of a view, so an oversized slice
//      is a local terminal 413 naming the VShard rather than the receiving leader's
//      ProposalTooLargeError arriving as an opaque remote error.

#include "../../../lib/cluster/data/dataplane_limits.hpp"
#include "../../../lib/cluster/data/replicated_command.hpp"
#include "../../../lib/cluster/data/write_record.hpp"
#include "../../../lib/cluster/raft/raft_group.hpp"  // kMaxProposalBytes
#include "../../../lib/cluster/raft/raft_types.hpp"

#include <gtest/gtest.h>

#include <limits>

using namespace timestar::data;

namespace {

WriteSeries floatSeries(const std::string& key, size_t n, uint64_t step) {
    WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    std::vector<double> vals;
    uint64_t t = 1;
    for (size_t i = 0; i < n; ++i) {
        s.timestamps.push_back(t);
        vals.push_back(static_cast<double>(i) * 1.5);
        s.revisions.push_back(i + 1);
        t += step;
    }
    s.values = std::move(vals);
    return s;
}

WriteSeries boolSeries(const std::string& key, size_t n, uint64_t step) {
    WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Boolean;
    std::vector<bool> vals;
    uint64_t t = 1;
    for (size_t i = 0; i < n; ++i) {
        s.timestamps.push_back(t);
        vals.push_back(i % 2 == 0);
        t += step;
    }
    s.values = std::move(vals);
    return s;
}

WriteSeries stringSeries(const std::string& key, size_t n) {
    WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::String;
    std::vector<std::string> vals;
    for (size_t i = 0; i < n; ++i) {
        s.timestamps.push_back(1000 + i);
        vals.push_back("value-" + std::to_string(i));
    }
    s.values = std::move(vals);
    return s;
}

WriteSeries intSeries(const std::string& key, size_t n) {
    WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Integer;
    std::vector<int64_t> vals;
    for (size_t i = 0; i < n; ++i) {
        s.timestamps.push_back(7 + i * 3);
        vals.push_back(-static_cast<int64_t>(i));
    }
    s.values = std::move(vals);
    return s;
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. The bound is an UPPER bound, in both directions, for every type
// ---------------------------------------------------------------------------

TEST(WriteSliceSizeChainTest, MaxEncodedBytesBoundsEveryVersionAndEveryType) {
    WriteBatch b;
    b.schemaVersion = 4;
    b.series.push_back(floatSeries("cpu,host=a value", 500, 1000));
    b.series.push_back(intSeries("cpu,host=a count", 300));
    b.series.push_back(boolSeries("cpu,host=a up", 400, 5));
    b.series.push_back(stringSeries("cpu,host=a state", 200));

    const size_t bound = maxEncodedBytes(b);
    EXPECT_LE(encodeWriteBatch(b, kWriteBatchFormatV1).size(), bound);
    EXPECT_LE(encodeWriteBatch(b, kWriteBatchFormatV2).size(), bound);
    // ...and it is a BOUND, not a wild over-estimate: within 2 bytes/point + the magic of
    // v1's exact size. (If this ever fails high, the bound has stopped tracking the
    // format and the refusal it feeds will start rejecting legitimate batches.)
    EXPECT_LE(bound, encodeWriteBatch(b, kWriteBatchFormatV1).size() + 4 + 2 * 1400);
}

// THE DIRECTION THAT BITES, and the reason the bound is not just "v1's size": v2 is
// usually SMALLER (deltas beat fixed u64s) but is LARGER when the deltas are huge, since
// a zigzag varint of a 64-bit value runs to 10 bytes where v1 pays a flat 8. A slice that
// arrived inside a v2 frame is re-encoded by the receiver at whatever the journal gate
// allows, so the refusal must charge the bigger of the two.
TEST(WriteSliceSizeChainTest, MaxEncodedBytesCoversAV2PayloadThatIsLargerThanV1) {
    WriteBatch b;
    WriteSeries s;
    s.seriesKey = "sparse,host=a value";
    s.type = TSMValueType::Float;
    std::vector<double> vals;
    // Alternating enormous forward/backward jumps: EVERY delta has magnitude ~2^62, whose
    // zigzag varint is the full 10 bytes, so this series is strictly bigger in v2 than in
    // v1. (A shape with one big jump and one small one is not enough -- the small deltas
    // pay it back and v2 comes out ahead again.)
    uint64_t t = 0;
    for (size_t i = 0; i < 256; ++i) {
        s.timestamps.push_back(t);
        vals.push_back(1.0);
        t += (i % 2 == 0) ? (uint64_t{1} << 62) : (0 - ((uint64_t{1} << 62) - 1));
    }
    s.values = std::move(vals);
    b.series.push_back(std::move(s));

    const size_t v1 = encodeWriteBatch(b, kWriteBatchFormatV1).size();
    const size_t v2 = encodeWriteBatch(b, kWriteBatchFormatV2).size();
    ASSERT_GT(v2, v1) << "the test's premise: this shape must be the one where v2 is bigger";
    EXPECT_LE(v2, maxEncodedBytes(b)) << "the bound must cover the LARGER version, not the usual one";
}

TEST(WriteSliceSizeChainTest, TheCommandBoundCoversTheFramingTheEntryReallyCarries) {
    WriteBatch b;
    b.series.push_back(floatSeries("m,host=a v", 100, 10));
    // encodeWriteCommand's own framing: kind tag + length prefix + FNV trailer.
    EXPECT_EQ(maxEncodedWriteCommandBytes(b), maxEncodedBytes(b) + kWriteCommandFramingBytes);
    EXPECT_LE(encodeWriteCommand(b).size(), maxEncodedWriteCommandBytes(b));

    WriteBatch empty;
    EXPECT_LE(encodeWriteCommand(empty).size(), maxEncodedWriteCommandBytes(empty));
}

// ---------------------------------------------------------------------------
// 2. The per-slice refusal
// ---------------------------------------------------------------------------

TEST(WriteSliceSizeChainTest, FirstUnproposableSliceNamesTheOffendingVShardAndNothingElse) {
    VShardBatches groups;
    groups.emplace_back(uint16_t{7}, WriteBatch{});
    groups.back().second.series.push_back(floatSeries("small,host=a v", 4, 1));
    groups.emplace_back(uint16_t{19}, WriteBatch{});
    groups.back().second.series.push_back(floatSeries("big,host=a v", 400, 1));
    groups.emplace_back(uint16_t{23}, WriteBatch{});
    groups.back().second.series.push_back(floatSeries("bigger,host=a v", 800, 1));

    const VShardBatchView view = viewOf(groups);

    // A bound above everything refuses nothing.
    EXPECT_FALSE(firstUnproposableSlice(view, std::numeric_limits<size_t>::max()).has_value());

    // A bound between the first and second group names the SECOND -- the first offender in
    // view order, not the largest, so the error is deterministic and the retry loop cannot
    // be handed a different answer depending on how the groups were ordered.
    const size_t small = maxEncodedWriteCommandBytes(groups[0].second);
    const size_t big = maxEncodedWriteCommandBytes(groups[1].second);
    ASSERT_LT(small, big);
    auto over = firstUnproposableSlice(view, small + 1);
    ASSERT_TRUE(over.has_value());
    EXPECT_EQ(over->vshard, 19);
    EXPECT_EQ(over->bytes, big);

    // A bound at exactly the largest slice admits it: the comparison is `>`, so a slice
    // that measures exactly the bound is proposable.
    const size_t biggest = maxEncodedWriteCommandBytes(groups[2].second);
    EXPECT_FALSE(firstUnproposableSlice(view, biggest).has_value());
    EXPECT_TRUE(firstUnproposableSlice(view, biggest - 1).has_value());
}

// ---------------------------------------------------------------------------
// 3. The chain itself
// ---------------------------------------------------------------------------

TEST(WriteSliceSizeChainTest, TheProposalBoundIsDerivedFromTheFrameBound) {
    // The same assertions cluster_data_plane.hpp makes at compile time, restated where a
    // reader looking for the numbers will find them. If these move, D-31's derivation has
    // been broken and the register row is no longer true.
    static_assert(timestar::raft::RaftGroup::kMaxProposalBytes == timestar::raft::kMaxRaftPayloadBytes);
    static_assert(chargeCeilingForV1Bytes(kMaxOutboundFrameBytes) + kWriteCommandFramingBytes <=
                      timestar::raft::RaftGroup::kMaxProposalBytes,
                  "any frame the data plane will send must be proposable AS CHARGED -- comparing raw bytes here "
                  "is review F1's bug, and passes while a maximal boolean frame is refused");
    static_assert(timestar::raft::RaftGroup::kMaxProposalBytes < 2 * kMaxOutboundFrameBytes,
                  "the entry bound is the wire bound plus margin, not an independent opinion");

    EXPECT_EQ(timestar::raft::kMaxRaftPayloadBytes, size_t{14} << 20);
    EXPECT_EQ(timestar::raft::kMaxRaftSendBytes, size_t{18} << 20);
    EXPECT_EQ(kMaxOutboundFrameBytes, (size_t{128} << 20) / 12);
}

// ---------------------------------------------------------------------------
// 4. The charge ratio, and the frame it has to cover (review F1)
// ---------------------------------------------------------------------------

// `chargeCeilingForV1Bytes` is what relates the wire bound to the proposal bound, so 11/9
// had better be a real ceiling and not a plausible one. v1's cheapest point is a boolean
// (8-byte timestamp + 1-byte value) and the charge adds 2 per point, so booleans bind at
// 11/9 and everything else is further under.
TEST(WriteSliceSizeChainTest, TheChargeNeverExceedsElevenNinthsOfAV1Encoding) {
    struct Case {
        const char* what;
        WriteBatch batch;
    };
    std::vector<Case> cases;
    {
        WriteBatch b;
        b.series.push_back(boolSeries("b,host=a v", 20000, 1));
        cases.push_back({"boolean (the binding case)", std::move(b)});
    }
    {
        WriteBatch b;
        b.series.push_back(floatSeries("f,host=a v", 20000, 1));
        cases.push_back({"float", std::move(b)});
    }
    {
        WriteBatch b;
        b.series.push_back(intSeries("i,host=a v", 20000));
        cases.push_back({"integer", std::move(b)});
    }
    {
        WriteBatch b;
        b.series.push_back(stringSeries("s,host=a v", 20000));
        cases.push_back({"string", std::move(b)});
    }

    for (const auto& c : cases) {
        const size_t v1 = encodeWriteBatch(c.batch, kWriteBatchFormatV1).size();
        const size_t charge = maxEncodedBytes(c.batch);
        EXPECT_LE(charge, chargeCeilingForV1Bytes(v1)) << c.what << ": the ratio is not a ceiling";
        EXPECT_GE(charge, encodeWriteBatch(c.batch, kWriteBatchFormatV2).size()) << c.what;
    }

    // ...and the boolean case really is close to the ratio, so the constant is not slack
    // that happens to hold. (If this drifts far below, the ratio has stopped describing the
    // format and the chain is being sized against a fiction.)
    const size_t v1 = encodeWriteBatch(cases[0].batch, kWriteBatchFormatV1).size();
    EXPECT_GT(maxEncodedBytes(cases[0].batch) * 100 / v1, 121u) << "boolean charge should sit at ~11/9 of v1";
}

// THE REGRESSION REVIEW F1 FOUND. A frame filled to `kMaxOutboundFrameBytes` in the
// PESSIMAL encoding (v1) must still be proposable -- otherwise a forwarded write that the
// data plane happily admits is refused with a terminal 413 at the entry bound. At a 12 MiB
// proposal bound this failed for booleans by ~1.09 MB and passed for floats by ONE byte.
TEST(WriteSliceSizeChainTest, AMaximalV1FrameIsProposableForEveryType) {
    const size_t frame = kMaxOutboundFrameBytes;

    auto fillToFrame = [&](TSMValueType type) {
        // Per-point v1 cost: 8-byte timestamp + the value column.
        const size_t perPoint = type == TSMValueType::Boolean ? 9 : 16;
        const size_t points = (frame - 64) / perPoint;
        WriteBatch b;
        b.series.push_back(type == TSMValueType::Boolean ? boolSeries("m,host=a v", points, 1)
                                                         : floatSeries("m,host=a v", points, 1));
        // NO REVISIONS, which is both what a forwarded slice really carries (they are
        // assigned at APPLY from the log position, ADR 0003) and the pessimal shape for the
        // charge ratio -- a revision column adds 8 v1 bytes per point and no charge, so
        // carrying one would make the test easier to pass and prove less.
        b.series.back().revisions.clear();
        return b;
    };

    for (TSMValueType type : {TSMValueType::Boolean, TSMValueType::Float}) {
        WriteBatch b = fillToFrame(type);
        const size_t v1 = encodeWriteBatch(b, kWriteBatchFormatV1).size();
        ASSERT_LE(v1, frame) << "the test must build a frame the data plane would actually SEND";
        ASSERT_GT(v1, frame - 4096) << "...and one that is essentially FULL, or it proves nothing";

        VShardBatches groups;
        groups.emplace_back(uint16_t{11}, std::move(b));
        EXPECT_FALSE(firstUnproposableSlice(viewOf(groups), timestar::raft::RaftGroup::kMaxProposalBytes).has_value())
            << (type == TSMValueType::Boolean ? "boolean" : "float")
            << ": a maximal frame the wire admits must be proposable, charge and all";
    }
}
