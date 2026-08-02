// The v1 wire frame and its Raft entry share one WriteBatch layout. These tests pin the
// allocation-free size calculation and the proposal bound built from it.

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
// 1. The allocation-free size is exact for every type
// ---------------------------------------------------------------------------

TEST(WriteSliceSizeChainTest, EncodedBytesMatchesEveryType) {
    WriteBatch b;
    b.schemaVersion = 4;
    b.series.push_back(floatSeries("cpu,host=a value", 500, 1000));
    b.series.push_back(intSeries("cpu,host=a count", 300));
    b.series.push_back(boolSeries("cpu,host=a up", 400, 5));
    b.series.push_back(stringSeries("cpu,host=a state", 200));

    EXPECT_EQ(encodedWriteBatchBytes(b), encodeWriteBatch(b).size());
}

TEST(WriteSliceSizeChainTest, TheCommandBoundCoversTheFramingTheEntryReallyCarries) {
    WriteBatch b;
    b.series.push_back(floatSeries("m,host=a v", 100, 10));
    // encodeWriteCommand's own framing: kind tag + length prefix + FNV trailer.
    EXPECT_EQ(encodedWriteCommandBytes(b), encodedWriteBatchBytes(b) + kWriteCommandFramingBytes);
    EXPECT_EQ(encodeWriteCommand(b).size(), encodedWriteCommandBytes(b));

    WriteBatch empty;
    EXPECT_EQ(encodeWriteCommand(empty).size(), encodedWriteCommandBytes(empty));
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
    const size_t small = encodedWriteCommandBytes(groups[0].second);
    const size_t big = encodedWriteCommandBytes(groups[1].second);
    ASSERT_LT(small, big);
    auto over = firstUnproposableSlice(view, small + 1);
    ASSERT_TRUE(over.has_value());
    EXPECT_EQ(over->vshard, 19);
    EXPECT_EQ(over->bytes, big);

    // A bound at exactly the largest slice admits it: the comparison is `>`, so a slice
    // that measures exactly the bound is proposable.
    const size_t biggest = encodedWriteCommandBytes(groups[2].second);
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
    static_assert(kMaxOutboundFrameBytes + kWriteCommandFramingBytes <=
                      timestar::raft::RaftGroup::kMaxProposalBytes,
                  "any v1 frame the data plane sends must be proposable");
    static_assert(timestar::raft::RaftGroup::kMaxProposalBytes < 2 * kMaxOutboundFrameBytes,
                  "the entry bound is the wire bound plus margin, not an independent opinion");

    EXPECT_EQ(timestar::raft::kMaxRaftPayloadBytes, size_t{14} << 20);
    EXPECT_EQ(timestar::raft::kMaxRaftSendBytes, size_t{18} << 20);
    EXPECT_EQ(kMaxOutboundFrameBytes, (size_t{128} << 20) / 12);
}
