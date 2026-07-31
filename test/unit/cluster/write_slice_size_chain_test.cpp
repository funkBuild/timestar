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
    static_assert(kMaxOutboundFrameBytes + kWriteCommandFramingBytes < timestar::raft::RaftGroup::kMaxProposalBytes,
                  "any frame the data plane will send must be proposable");
    static_assert(timestar::raft::RaftGroup::kMaxProposalBytes < 2 * kMaxOutboundFrameBytes,
                  "the entry bound is the wire bound plus margin, not an independent opinion");

    EXPECT_EQ(timestar::raft::kMaxRaftPayloadBytes, size_t{12} << 20);
    EXPECT_EQ(timestar::raft::kMaxRaftSendBytes, size_t{16} << 20);
    EXPECT_EQ(kMaxOutboundFrameBytes, (size_t{128} << 20) / 12);
}
