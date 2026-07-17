#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/query_result.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/tsm.hpp"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

// Last-write-wins (LWW) semantics: writing the same series + timestamp again
// REPLACES the earlier point. These tests pin the invariant at each layer:
// memory-store ingest (dedup + stats recompute), TSM file ranking (dataSeq),
// and the query-time TSM result merge (newest rank wins, same-source runs
// keep their last copy).

// ---------------------------------------------------------------------------
// Memory store ingest
// ---------------------------------------------------------------------------

TEST(LwwMemoryStoreTest, OverwriteAcrossBatchesReplacesValueAndStats) {
    InMemorySeries<double> series;

    TimeStarInsert<double> a("m", "f");
    a.addValue(1000, 10.0);
    a.addValue(2000, 20.0);
    a.addValue(3000, 30.0);
    series.insert(std::move(a));

    ASSERT_EQ(series.stats.count, 3u);
    EXPECT_DOUBLE_EQ(series.stats.compensatedSum(), 60.0);
    EXPECT_DOUBLE_EQ(series.stats.max, 30.0);

    // Rewrite the middle point (merge path: batch starts before the tail)
    TimeStarInsert<double> b("m", "f");
    b.addValue(2000, 200.0);
    series.insert(std::move(b));

    ASSERT_EQ(series.timestamps.size(), 3u);
    EXPECT_EQ(series.timestamps[1], 2000u);
    EXPECT_DOUBLE_EQ(series.values[1], 200.0);

    // Running stats were recomputed over the surviving points only —
    // the overwritten value must not linger in sum/max/count.
    EXPECT_EQ(series.stats.count, 3u);
    EXPECT_DOUBLE_EQ(series.stats.compensatedSum(), 240.0);
    EXPECT_DOUBLE_EQ(series.stats.min, 10.0);
    EXPECT_DOUBLE_EQ(series.stats.max, 200.0);
    EXPECT_EQ(series.stats.firstTimestamp, 1000u);
    EXPECT_DOUBLE_EQ(series.stats.firstValue, 10.0);
    EXPECT_EQ(series.stats.latestTimestamp, 3000u);
    EXPECT_DOUBLE_EQ(series.stats.latestValue, 30.0);
}

TEST(LwwMemoryStoreTest, OverwriteTailPointBoundaryEqual) {
    InMemorySeries<double> series;

    TimeStarInsert<double> a("m", "f");
    a.addValue(1000, 1.0);
    a.addValue(2000, 2.0);
    series.insert(std::move(a));

    // New batch begins exactly at the old tail timestamp — the common
    // "update the current value" pattern.
    TimeStarInsert<double> b("m", "f");
    b.addValue(2000, 5.0);
    b.addValue(3000, 3.0);
    series.insert(std::move(b));

    ASSERT_EQ(series.timestamps.size(), 3u);
    EXPECT_DOUBLE_EQ(series.values[0], 1.0);
    EXPECT_DOUBLE_EQ(series.values[1], 5.0);
    EXPECT_DOUBLE_EQ(series.values[2], 3.0);

    EXPECT_EQ(series.stats.count, 3u);
    EXPECT_DOUBLE_EQ(series.stats.compensatedSum(), 9.0);
    EXPECT_DOUBLE_EQ(series.stats.max, 5.0);
    EXPECT_EQ(series.stats.latestTimestamp, 3000u);
    EXPECT_DOUBLE_EQ(series.stats.latestValue, 3.0);
}

TEST(LwwMemoryStoreTest, RepeatedOverwriteOfSamePoint) {
    InMemorySeries<double> series;
    for (int i = 1; i <= 5; ++i) {
        TimeStarInsert<double> w("m", "f");
        w.addValue(1000, static_cast<double>(i));
        series.insert(std::move(w));
    }
    ASSERT_EQ(series.timestamps.size(), 1u);
    EXPECT_DOUBLE_EQ(series.values[0], 5.0);
    EXPECT_EQ(series.stats.count, 1u);
    EXPECT_DOUBLE_EQ(series.stats.compensatedSum(), 5.0);
    EXPECT_DOUBLE_EQ(series.stats.min, 5.0);
    EXPECT_DOUBLE_EQ(series.stats.max, 5.0);
}

TEST(LwwMemoryStoreTest, WithinBatchLastOccurrenceWinsEvenWhenUnsorted) {
    InMemorySeries<double> series;

    // Unsorted batch containing three writes to ts=2000; the LAST in request
    // order must win (stable sort preserves request order within equal ts).
    TimeStarInsert<double> w("m", "f");
    w.addValue(2000, 1.0);
    w.addValue(1000, 10.0);
    w.addValue(2000, 2.0);
    w.addValue(3000, 30.0);
    w.addValue(2000, 3.0);
    series.insert(std::move(w));

    ASSERT_EQ(series.timestamps.size(), 3u);
    EXPECT_EQ(series.timestamps[1], 2000u);
    EXPECT_DOUBLE_EQ(series.values[1], 3.0);
}

TEST(LwwMemoryStoreTest, StringOverwrite) {
    InMemorySeries<std::string> series;

    TimeStarInsert<std::string> a("m", "f");
    a.addValue(1000, std::string("old"));
    a.addValue(2000, std::string("keep"));
    series.insert(std::move(a));

    TimeStarInsert<std::string> b("m", "f");
    b.addValue(1000, std::string("new"));
    series.insert(std::move(b));

    ASSERT_EQ(series.timestamps.size(), 2u);
    EXPECT_EQ(series.values[0], "new");
    EXPECT_EQ(series.values[1], "keep");
}

TEST(LwwMemoryStoreTest, BoolOverwrite) {
    InMemorySeries<bool> series;

    TimeStarInsert<bool> a("m", "f");
    a.addValue(1000, true);
    series.insert(std::move(a));

    TimeStarInsert<bool> b("m", "f");
    b.addValue(1000, false);
    series.insert(std::move(b));

    ASSERT_EQ(series.timestamps.size(), 1u);
    EXPECT_FALSE(static_cast<bool>(series.values[0]));
}

TEST(LwwMemoryStoreTest, Int64Overwrite) {
    InMemorySeries<int64_t> series;

    TimeStarInsert<int64_t> a("m", "f");
    a.addValue(1000, int64_t{7});
    series.insert(std::move(a));

    TimeStarInsert<int64_t> b("m", "f");
    b.addValue(1000, int64_t{42});
    series.insert(std::move(b));

    ASSERT_EQ(series.timestamps.size(), 1u);
    EXPECT_EQ(series.values[0], 42);
}

// ---------------------------------------------------------------------------
// TSM file ranking: dataSeq (write recency) decides duplicates, not tier
// ---------------------------------------------------------------------------

TEST(LwwTsmRankTest, FilenameParsingAndDataRank) {
    // Flush-created file: no suffix, dataSeq == seqNum
    TSM flush("/nonexistent/tsm/0_7.tsm");
    EXPECT_EQ(flush.tierNum, 0u);
    EXPECT_EQ(flush.seqNum, 7u);
    EXPECT_EQ(flush.dataSeq, 7u);
    EXPECT_EQ(flush.dataRank(), (uint64_t{7} << 4) | 0);

    // Compaction output: fresh seqNum, inherited dataSeq via _d suffix
    TSM compacted("/nonexistent/tsm/1_42_d17.tsm");
    EXPECT_EQ(compacted.tierNum, 1u);
    EXPECT_EQ(compacted.seqNum, 42u);
    EXPECT_EQ(compacted.dataSeq, 17u);
    EXPECT_EQ(compacted.dataRank(), (uint64_t{17} << 4) | 1);

    // File identity (rankAsInteger) is unchanged by the suffix
    EXPECT_EQ(flush.rankAsInteger(), uint64_t{7});
    EXPECT_EQ(compacted.rankAsInteger(), (uint64_t{1} << 60) | 42);
}

TEST(LwwTsmRankTest, NewerFlushOutranksOlderCompactedFile) {
    // THE regression this design exists for: an old point compacted into
    // tier 1 (fresh seq 42, data generation 17) must LOSE duplicate
    // resolution against a tier-0 flush holding a newer rewrite (seq 20).
    // The old tier-dominant rank ((tier << 60) | seq) got this backwards.
    TSM compactedOld("/nonexistent/tsm/1_42_d17.tsm");
    TSM freshRewrite("/nonexistent/tsm/0_20.tsm");
    EXPECT_GT(freshRewrite.dataRank(), compactedOld.dataRank());
}

TEST(LwwTsmRankTest, SameDataSeqHigherTierWins) {
    // A compacted file carrying the same newest generation as a remaining
    // input is a dedup superset — prefer it on ties.
    TSM tier0("/nonexistent/tsm/0_9.tsm");
    TSM tier1("/nonexistent/tsm/1_50_d9.tsm");
    EXPECT_GT(tier1.dataRank(), tier0.dataRank());
}

// ---------------------------------------------------------------------------
// Query-time TSM result merge
// ---------------------------------------------------------------------------

namespace {
std::unique_ptr<TSMBlock<double>> makeBlock(std::vector<uint64_t> ts, std::vector<double> vals) {
    auto b = std::make_unique<TSMBlock<double>>(ts.size());
    b->timestamps = std::move(ts);
    b->values = std::move(vals);
    return b;
}
}  // namespace

TEST(LwwQueryResultMergeTest, TwoWayCrossFileDuplicateNewestRankWins) {
    std::vector<TSMResult<double>> results;
    results.emplace_back(uint64_t{7} << 4);  // older generation
    results.back().appendBlock(makeBlock({100, 200}, {1.0, 2.0}));
    results.emplace_back(uint64_t{9} << 4);  // newer generation (rewrite)
    results.back().appendBlock(makeBlock({200, 300}, {22.0, 3.0}));

    auto merged = QueryResult<double>::fromTsmResults(results);
    ASSERT_EQ(merged.timestamps, (std::vector<uint64_t>{100, 200, 300}));
    EXPECT_DOUBLE_EQ(merged.values[0], 1.0);
    EXPECT_DOUBLE_EQ(merged.values[1], 22.0);  // newest write visible, once
    EXPECT_DOUBLE_EQ(merged.values[2], 3.0);
}

TEST(LwwQueryResultMergeTest, SingleSourceLegacyRunKeepsLastCopy) {
    // Legacy files (written before ingest dedup) may hold intra-file
    // duplicate runs; within a file, later position = later write.
    std::vector<TSMResult<double>> results;
    results.emplace_back(uint64_t{5} << 4);
    results.back().appendBlock(makeBlock({100, 100, 200}, {1.0, 5.0, 2.0}));

    auto merged = QueryResult<double>::fromTsmResults(results);
    ASSERT_EQ(merged.timestamps, (std::vector<uint64_t>{100, 200}));
    EXPECT_DOUBLE_EQ(merged.values[0], 5.0);
    EXPECT_DOUBLE_EQ(merged.values[1], 2.0);
}

TEST(LwwQueryResultMergeTest, SmallNMergeDuplicateResolution) {
    // 3 sources → mergeSmallN path. All share ts=500; rank 3 must win.
    std::vector<TSMResult<double>> results;
    results.emplace_back(uint64_t{1} << 4);
    results.back().appendBlock(makeBlock({400, 500}, {4.0, 1.0}));
    results.emplace_back(uint64_t{3} << 4);
    results.back().appendBlock(makeBlock({500, 600}, {33.0, 6.0}));
    results.emplace_back(uint64_t{2} << 4);
    results.back().appendBlock(makeBlock({500, 700}, {2.0, 7.0}));

    auto merged = QueryResult<double>::fromTsmResults(results);
    ASSERT_EQ(merged.timestamps, (std::vector<uint64_t>{400, 500, 600, 700}));
    EXPECT_DOUBLE_EQ(merged.values[1], 33.0);
}

TEST(LwwQueryResultMergeTest, HeapMergeDuplicateResolution) {
    // 5 sources → heap merge path. ts=500 exists in all; highest rank wins.
    std::vector<TSMResult<double>> results;
    for (uint64_t r = 1; r <= 5; ++r) {
        results.emplace_back(r << 4);
        results.back().appendBlock(makeBlock({500, 500 + r * 100}, {static_cast<double>(r), 0.0}));
    }

    auto merged = QueryResult<double>::fromTsmResults(results);
    ASSERT_EQ(merged.timestamps.size(), 6u);  // 500 once + 5 distinct tails
    EXPECT_EQ(merged.timestamps[0], 500u);
    EXPECT_DOUBLE_EQ(merged.values[0], 5.0);
}

TEST(LwwQueryResultMergeTest, DrainPathKeepsLastCopyOfRuns) {
    // Source 0 exhausts early; source 1's remaining legacy run at ts=900
    // must still collapse to its last copy during the drain.
    std::vector<TSMResult<double>> results;
    results.emplace_back(uint64_t{2} << 4);
    results.back().appendBlock(makeBlock({100}, {1.0}));
    results.emplace_back(uint64_t{1} << 4);
    results.back().appendBlock(makeBlock({900, 900, 950}, {9.0, 90.0, 95.0}));

    auto merged = QueryResult<double>::fromTsmResults(results);
    ASSERT_EQ(merged.timestamps, (std::vector<uint64_t>{100, 900, 950}));
    EXPECT_DOUBLE_EQ(merged.values[1], 90.0);
    EXPECT_DOUBLE_EQ(merged.values[2], 95.0);
}
