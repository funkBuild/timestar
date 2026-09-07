// Regression: Engine::insertBatch must record day-bitmap membership for a
// KNOWN series on every batch, through the real production path.
//
// Production 1.4.3 recorded it from `req.getTimestamps()` AFTER
// walFileManager.insertBatch had moved those timestamps into the memory store
// (InMemorySeries::insert takes the request by rvalue and calls
// takeTimestamps), so the loop iterated an empty vector and recorded nothing.
// An established series then only had membership for the day its first-batch
// MetadataOp announced (and whatever the startup repair restored); every
// time-scoped query whose range started after that came back empty while the
// data was present. Fleet-wide, discoverable stats series decayed
// 194 → 161 → 111 → 2 → 1 → 0 over the six days after the 1.4.3 restart.
//
// The existing index-level test (RecordInsertDaysAddsLaterDays) calls
// recordInsertDays directly with a fresh vector and so could not see this.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/index/index_backend.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

class EngineBatchDayMembershipTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

namespace {

constexpr uint64_t kNsPerDay = timestar::index::keys::NS_PER_DAY;
constexpr uint64_t kFirstDayTs = 22000ULL * kNsPerDay;
const std::string kMeasurement = "stats";
const std::string kField = "uptime";
const std::map<std::string, std::string> kTags{{"deviceId", "d1"}};

// What the write handler does for the FIRST batch of a series it has not seen:
// announce it so the LocalId exists. Later batches for a known series emit no
// MetadataOp — that is the steady state this test is about.
void announceSeries(Engine& engine, uint64_t ts) {
    MetadataOp op;
    op.valueType = TSMValueType::Float;
    op.measurement = kMeasurement;
    op.fieldName = kField;
    op.tags = kTags;
    op.minTs = ts;
    op.maxTs = ts;
    engine.indexMetadataBatch({op}).get();
}

// The write handler's shape: tags and timestamps are shared_ptrs across the
// fields of one multi-field point.
std::vector<TimeStarInsert<double>> sharedBatch(std::vector<uint64_t> timestamps, std::vector<double> values) {
    TimeStarInsert<double> ins(kMeasurement, kField);
    ins.setSharedTags(std::make_shared<const std::map<std::string, std::string>>(kTags));
    ins.setSharedTimestamps(std::make_shared<const std::vector<uint64_t>>(std::move(timestamps)));
    ins.values = std::move(values);
    std::vector<TimeStarInsert<double>> out;
    out.push_back(std::move(ins));
    return out;
}

// The owned-vector shape (addValue), as other producers build it.
std::vector<TimeStarInsert<double>> ownedBatch(const std::vector<uint64_t>& timestamps,
                                               const std::vector<double>& values) {
    TimeStarInsert<double> ins(kMeasurement, kField);
    for (const auto& [k, v] : kTags)
        ins.addTag(k, v);
    for (size_t i = 0; i < timestamps.size(); ++i)
        ins.addValue(timestamps[i], values[i]);
    std::vector<TimeStarInsert<double>> out;
    out.push_back(std::move(ins));
    return out;
}

size_t discoveredIn(Engine& engine, uint64_t fromTs, uint64_t toTs) {
    auto r = engine.getIndex().findSeriesWithMetadataTimeScoped(kMeasurement, kTags, {}, fromTs, toTs, 0).get();
    if (!r.has_value())
        return 0;
    return r->size();
}

size_t pointsIn(Engine& engine, uint64_t fromTs, uint64_t toTs) {
    TimeStarInsert<double> probe(kMeasurement, kField);
    for (const auto& [k, v] : kTags)
        probe.addTag(k, v);
    auto key = probe.seriesKey();
    auto res = engine.query(key, SeriesId128::fromSeriesKey(key), fromTs, toTs).get();
    if (!res.has_value())
        return 0;
    return std::get<QueryResult<double>>(res.value()).timestamps.size();
}

template <class MakeBatch>
void runKnownSeriesScenario(MakeBatch makeBatch) {
    ScopedEngine eng;
    eng.init();
    Engine& engine = *eng.get();

    // Day 0: first batch, announced like the write handler would.
    announceSeries(engine, kFirstDayTs);
    engine.insertBatch<double>(makeBatch({kFirstDayTs}, {1.0})).get();

    // Day 3: a later batch for the now-KNOWN series. No MetadataOp — the only
    // thing that can record this day is the batch path itself.
    const uint64_t laterDayTs = kFirstDayTs + 3 * kNsPerDay;
    engine.insertBatch<double>(makeBatch({laterDayTs, laterDayTs + 1'000'000'000ULL}, {2.0, 3.0})).get();

    const uint64_t laterDayEnd = laterDayTs + kNsPerDay - 1;
    ASSERT_EQ(pointsIn(engine, laterDayTs, laterDayEnd), 2u) << "the data landed; discovery is what is under test";
    EXPECT_EQ(discoveredIn(engine, laterDayTs, laterDayEnd), 1u)
        << "REGRESSION: a known series written on a later day is invisible to a query scoped to that day — "
           "day membership was recorded from timestamps the memory store had already moved out";

    // A range starting on the announced day still finds it either way; that
    // is the 'widen the window and the data reappears' symptom, pinned so the
    // two assertions cannot both pass for the wrong reason.
    EXPECT_EQ(discoveredIn(engine, kFirstDayTs, laterDayEnd), 1u);
}

}  // namespace

TEST_F(EngineBatchDayMembershipTest, SharedTimestampBatchRecordsLaterDaysForKnownSeries) {
    seastar::thread([] { runKnownSeriesScenario(sharedBatch); }).join().get();
}

TEST_F(EngineBatchDayMembershipTest, OwnedTimestampBatchRecordsLaterDaysForKnownSeries) {
    seastar::thread([] { runKnownSeriesScenario(ownedBatch); }).join().get();
}
