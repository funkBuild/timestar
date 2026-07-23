// Behavioral tests for the per-series value-type binding enforced on ingest.
//
// Bug history: SeriesId128 hashes measurement+tags+field only (series_id.hpp) --
// the value type is NOT part of the key. The only type check lived in
// MemoryStore::insertMemory and compared against the LIVE memory store, so once
// that store rolled over and flushed, the same field could be re-written with a
// different type. The result was one series id with Boolean blocks in file A and
// Float blocks in file B, which made compaction refuse the whole tier:
//
//   Compaction failed for tier 0: Series 0602d589... has conflicting value types
//   across input TSM files; refusing to compact
//
// Fix: bind a series to the type of its first write (SERIES_VALUE_TYPE, 0x18)
// and enforce it in Engine before anything durable happens. Later writes of a
// different type are converted when that is lossless, and rejected otherwise.
// A delete spanning all of time releases the binding so the series can be
// re-created with a new type.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

class EngineSeriesTypeBindingTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

// Build a one-series batch for the insertBatch path (the production path).
template <class T>
std::vector<TimeStarInsert<T>> batchOf(const std::string& measurement, const std::string& field,
                                       const std::map<std::string, std::string>& tags, uint64_t ts, T value) {
    TimeStarInsert<T> ins(measurement, field);
    for (const auto& [k, v] : tags)
        ins.addTag(k, v);
    ins.addValue(ts, std::move(value));
    std::vector<TimeStarInsert<T>> out;
    out.push_back(std::move(ins));
    return out;
}

std::string keyOf(const std::string& measurement, const std::string& field,
                  const std::map<std::string, std::string>& tags) {
    TimeStarInsert<double> probe(measurement, field);
    for (const auto& [k, v] : tags)
        probe.addTag(k, v);
    return probe.seriesKey();
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. THE BUG: a type change that straddles a memstore flush.
//
//    Before the fix the boolean write was accepted into a fresh memory store
//    (the live-store check had nothing to compare against), producing two TSM
//    files that disagreed. Now the series stays float.
// ---------------------------------------------------------------------------
TEST_F(EngineSeriesTypeBindingTest, TypeChangeAcrossFlushDoesNotRetypeTheSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "a"}};
        eng->insertBatch<double>(batchOf<double>("conflict_m", "value", tags, 1000, 1.5)).get();

        // Flush, so the live-store type check can no longer see the float data.
        eng->rolloverMemoryStore().get();

        // true is losslessly representable in a float series, so it is accepted
        // AS A FLOAT rather than re-typing the series.
        eng->insertBatch<bool>(batchOf<bool>("conflict_m", "value", tags, 2000, true)).get();

        auto seriesKey = keyOf("conflict_m", "value", tags);
        auto seriesId = SeriesId128::fromSeriesKey(seriesKey);
        auto resultOpt = eng->query(seriesKey, seriesId, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());

        // The whole point: one type, not two.
        ASSERT_TRUE(std::holds_alternative<QueryResult<double>>(resultOpt.value()))
            << "REGRESSION: series was re-typed after a flush; two TSM files now disagree and compaction will wedge";

        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 1.5);
        EXPECT_DOUBLE_EQ(result.values[1], 1.0) << "true must be stored as 1.0 in a float-bound series";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 2. Lossless conversions are accepted
// ---------------------------------------------------------------------------
TEST_F(EngineSeriesTypeBindingTest, IntegerIsWidenedIntoAFloatSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "b"}};
        eng->insertBatch<double>(batchOf<double>("widen_m", "value", tags, 1000, 1.5)).get();
        // A JSON serialiser emitting 10.0 as 10 is the common real-world case.
        eng->insertBatch<int64_t>(batchOf<int64_t>("widen_m", "value", tags, 2000, int64_t{10})).get();

        auto seriesKey = keyOf("widen_m", "value", tags);
        auto resultOpt = eng->query(seriesKey, SeriesId128::fromSeriesKey(seriesKey), 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        ASSERT_TRUE(std::holds_alternative<QueryResult<double>>(resultOpt.value()));

        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[1], 10.0);
    })
        .join()
        .get();
}

TEST_F(EngineSeriesTypeBindingTest, IntegralFloatIsAcceptedIntoAnIntegerSeries) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "c"}};
        eng->insertBatch<int64_t>(batchOf<int64_t>("narrow_m", "value", tags, 1000, int64_t{7})).get();
        eng->insertBatch<double>(batchOf<double>("narrow_m", "value", tags, 2000, 10.0)).get();

        auto seriesKey = keyOf("narrow_m", "value", tags);
        auto resultOpt = eng->query(seriesKey, SeriesId128::fromSeriesKey(seriesKey), 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        ASSERT_TRUE(std::holds_alternative<QueryResult<int64_t>>(resultOpt.value()));

        auto& result = std::get<QueryResult<int64_t>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 2u);
        EXPECT_EQ(result.values[1], 10);
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 3. Lossy or ambiguous conversions are REJECTED, and leave nothing behind
// ---------------------------------------------------------------------------
TEST_F(EngineSeriesTypeBindingTest, UnparseableStringIntoFloatSeriesIsRejected) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "d"}};
        eng->insertBatch<double>(batchOf<double>("reject_m", "value", tags, 1000, 2.5)).get();

        bool threw = false;
        try {
            eng->insertBatch<std::string>(batchOf<std::string>("reject_m", "value", tags, 2000, std::string("abcd")))
                .get();
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "\"abcd\" is not a number; it must be rejected rather than stored as NaN or 0";

        // And the rejection must leave NO durable trace — this is why the check
        // sits upstream of the WAL write.
        auto seriesKey = keyOf("reject_m", "value", tags);
        auto resultOpt = eng->query(seriesKey, SeriesId128::fromSeriesKey(seriesKey), 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 1u) << "a rejected write must not be persisted";
        EXPECT_DOUBLE_EQ(result.values[0], 2.5);
    })
        .join()
        .get();
}

TEST_F(EngineSeriesTypeBindingTest, FractionalFloatIntoIntegerSeriesIsRejected) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "e"}};
        eng->insertBatch<int64_t>(batchOf<int64_t>("frac_m", "value", tags, 1000, int64_t{3})).get();

        bool threw = false;
        try {
            eng->insertBatch<double>(batchOf<double>("frac_m", "value", tags, 2000, 10.5)).get();
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "10.5 -> 10 destroys data; it must be rejected, not truncated";
    })
        .join()
        .get();
}

TEST_F(EngineSeriesTypeBindingTest, NonZeroOneNumberIntoBooleanSeriesIsRejected) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "f"}};
        eng->insertBatch<bool>(batchOf<bool>("bool_m", "flag", tags, 1000, true)).get();

        // 0 and 1 round-trip to a boolean; 5 would discard the 5.
        eng->insertBatch<int64_t>(batchOf<int64_t>("bool_m", "flag", tags, 2000, int64_t{0})).get();

        bool threw = false;
        try {
            eng->insertBatch<int64_t>(batchOf<int64_t>("bool_m", "flag", tags, 3000, int64_t{5})).get();
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "5 has no unambiguous boolean reading";

        auto seriesKey = keyOf("bool_m", "flag", tags);
        auto resultOpt = eng->query(seriesKey, SeriesId128::fromSeriesKey(seriesKey), 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        ASSERT_TRUE(std::holds_alternative<QueryResult<bool>>(resultOpt.value()));

        auto& result = std::get<QueryResult<bool>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 2u);
        EXPECT_TRUE(result.values[0]);
        EXPECT_FALSE(result.values[1]) << "0 must land as false";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 4. Delete releases the binding — the documented way to re-type a series
// ---------------------------------------------------------------------------
TEST_F(EngineSeriesTypeBindingTest, FullRangeDeleteReleasesTheBinding) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "g"}};
        eng->insertBatch<double>(batchOf<double>("retype_m", "value", tags, 1000, 4.5)).get();

        auto seriesKey = keyOf("retype_m", "value", tags);
        eng->deleteRange(seriesKey, 0, UINT64_MAX).get();

        // With the binding gone, the series may be re-created as a string.
        eng->insertBatch<std::string>(batchOf<std::string>("retype_m", "value", tags, 2000, std::string("abcd")))
            .get();

        auto resultOpt = eng->query(seriesKey, SeriesId128::fromSeriesKey(seriesKey), 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        ASSERT_TRUE(std::holds_alternative<QueryResult<std::string>>(resultOpt.value()))
            << "a full-range delete must release the binding so the series can be re-typed";

        auto& result = std::get<QueryResult<std::string>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 1u);
        EXPECT_EQ(result.values[0], "abcd");
    })
        .join()
        .get();
}

TEST_F(EngineSeriesTypeBindingTest, PartialRangeDeleteKeepsTheBinding) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"host", "h"}};
        eng->insertBatch<double>(batchOf<double>("partial_m", "value", tags, 1000, 1.0)).get();
        eng->insertBatch<double>(batchOf<double>("partial_m", "value", tags, 5000, 2.0)).get();

        auto seriesKey = keyOf("partial_m", "value", tags);
        // Delete only the first point. Surviving points still carry the old
        // type, so re-typing around them must stay forbidden.
        eng->deleteRange(seriesKey, 0, 2000).get();

        bool threw = false;
        try {
            eng->insertBatch<std::string>(batchOf<std::string>("partial_m", "value", tags, 6000, std::string("xyz")))
                .get();
        } catch (const std::invalid_argument&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "a bounded-range delete must NOT release the binding";
    })
        .join()
        .get();
}
