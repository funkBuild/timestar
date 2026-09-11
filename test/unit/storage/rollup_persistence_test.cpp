#include "../../../lib/query/query_result.hpp"
#include "../../../lib/storage/rollup_codec.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <filesystem>
#include <fstream>
#include <numeric>
#include <seastar/core/coroutine.hh>

class RollupPersistenceTest : public ::testing::Test {
public:
    static constexpr uint64_t second = 1000000000ULL;
    static constexpr uint64_t minute = 60 * second;
    std::filesystem::path cwd;
    std::unique_ptr<TSMFileManager> manager;
    std::unique_ptr<TSMCompactor> compactor;
    std::vector<seastar::shared_ptr<TSM>> opened;
    SeriesId128 sid = SeriesId128::fromSeriesKey("rollup,device=a#value");
    void SetUp() override {
        cwd = std::filesystem::current_path();
        std::filesystem::create_directories("rollup_persistence_test/shard_0/tsm");
        std::filesystem::current_path("rollup_persistence_test");
        manager = std::make_unique<TSMFileManager>();
        compactor = std::make_unique<TSMCompactor>(manager.get());
    }
    void TearDown() override {
        for (auto& file : opened)
            file->close().get();
        opened.clear();
        compactor.reset();
        manager.reset();
        std::filesystem::current_path(cwd);
        std::filesystem::remove_all("rollup_persistence_test");
    }
    seastar::future<seastar::shared_ptr<TSM>> open(std::string path) {
        auto file = seastar::make_shared<TSM>(path);
        co_await file->open();
        opened.push_back(file);
        co_return file;
    }
    template <typename T>
    seastar::future<seastar::shared_ptr<TSM>> raw(std::vector<uint64_t> ts, std::vector<T> values) {
        auto path = "shard_0/tsm/0_" + std::to_string(manager->allocateSequenceId()) + ".tsm";
        TSMWriter writer(path);
        writer.writeSeries(TSM::getValueType<T>(), sid, ts, values);
        writer.writeIndex();
        writer.close();
        co_return co_await open(path);
    }
    seastar::future<seastar::shared_ptr<TSM>> fold(std::vector<seastar::shared_ptr<TSM>> files, std::string method,
                                                   uint64_t interval = minute, bool policyEnabled = true) {
        RetentionPolicy policy;
        policy.measurement = "rollup";
        DownsamplePolicy tier;
        tier.afterNanos = 3600 * second;
        tier.intervalNanos = interval;
        tier.method = method;
        tier.fieldMethods = std::map<std::string, std::string>{{"value", method}};
        policy.downsampleTiers = {tier};
        std::unordered_map<std::string, RetentionPolicy> policies;
        if (policyEnabled)
            policies.emplace("rollup", policy);
        std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> measurements{{sid, "rollup"}},
            fields{{sid, "value"}};
        auto result = co_await compactor->compact(files, policies, measurements, fields);
        co_return co_await open(result.outputPath);
    }
    template <typename T>
    seastar::future<QueryResult<T>> read(std::vector<seastar::shared_ptr<TSM>> files) {
        std::vector<TSMResult<T>> results;
        for (auto file : files)
            results.push_back(co_await file->template queryWithTombstones<T>(sid, 0, UINT64_MAX));
        co_return QueryResult<T>::fromTsmResults(results);
    }
};

SEASTAR_TEST_F(RollupPersistenceTest, SplitMinuteContributionsSurviveReadAndMerge) {
    std::vector<uint64_t> a, b;
    for (uint64_t i = 0; i < 30; ++i) {
        a.push_back((120 + i) * self->second);
        b.push_back((150 + i) * self->second);
    }
    auto rawA = co_await self->raw<double>(a, std::vector<double>(30, 1));
    auto rawB = co_await self->raw<double>(b, std::vector<double>(30, 1));
    auto foldedA = co_await self->fold({rawA}, "sum");
    auto foldedB = co_await self->fold({rawB}, "sum");
    auto separate = co_await self->read<double>({foldedA, foldedB});
    EXPECT_EQ(separate.values, std::vector<double>{60});
    auto combined = co_await self->fold({foldedA, foldedB}, "sum");
    auto final = co_await self->read<double>({combined});
    EXPECT_EQ(final.values, std::vector<double>{60});
    EXPECT_EQ(final.rollups.at(0).count, 60u);
    // Metadata must survive a later compaction after removal of the policy.
    auto noPolicy = co_await self->fold({combined}, "sum", self->minute, false);
    auto preserved = co_await self->read<double>({noPolicy});
    EXPECT_EQ(preserved.rollups.at(0).count, 60u);
}

SEASTAR_TEST_F(RollupPersistenceTest, LateBooleanCannotDisplaceNewerObservation) {
    auto raw = co_await self->raw<bool>({120 * self->second, 179 * self->second}, {false, true});
    auto folded = co_await self->fold({raw}, "latest");
    auto late = co_await self->raw<bool>({150 * self->second}, {false});
    auto separate = co_await self->read<bool>({folded, late});
    EXPECT_EQ(separate.values, std::vector<bool>{true});
    auto combined = co_await self->fold({folded, late}, "latest");
    auto final = co_await self->read<bool>({combined});
    EXPECT_EQ(final.values, std::vector<bool>{true});
    EXPECT_EQ(final.rollups.at(0).latestTimestamp, 179 * self->second);
}

SEASTAR_TEST_F(RollupPersistenceTest, IntegerSelectorsKeepEveryBitAndSumOverflowFails) {
    for (const auto* method : {"min", "max", "latest"}) {
        for (int64_t value : {INT64_MIN, INT64_MAX, INT64_C(9007199254740993)}) {
            auto raw = co_await self->raw<int64_t>({120 * self->second}, {value});
            auto folded = co_await self->fold({raw}, method);
            auto result = co_await self->read<int64_t>({folded});
            EXPECT_EQ(result.values, std::vector<int64_t>{value}) << method;
        }
    }
    auto raw = co_await self->raw<int64_t>({120 * self->second, 121 * self->second}, {INT64_MAX, 1});
    bool rejected = false;
    try {
        co_await self->fold({raw}, "sum");
    } catch (const std::overflow_error&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    auto unchanged = co_await self->read<int64_t>({raw});
    EXPECT_EQ(unchanged.values, (std::vector<int64_t>{INT64_MAX, 1}));
}

SEASTAR_TEST_F(RollupPersistenceTest, GappyAveragesCarryCountsThroughFifteenMinuteTier) {
    std::vector<uint64_t> ts;
    std::vector<double> values(841, 0);
    values.back() = 100;
    for (uint64_t i = 0; i < 841; ++i)
        ts.push_back((900 + i) * self->second);
    auto raw = co_await self->raw<double>(ts, values);
    auto minute = co_await self->fold({raw}, "avg");
    auto coarse = co_await self->fold({minute}, "avg", 15 * self->minute);
    auto result = co_await self->read<double>({coarse});
    EXPECT_EQ(result.values.size(), 1u);
    EXPECT_DOUBLE_EQ(result.values.at(0), 100.0 / 841);
    EXPECT_EQ(result.rollups.at(0).count, 841u);
}

SEASTAR_TEST_F(RollupPersistenceTest, DerivedNanSurvivesRepeatedCompaction) {
    auto raw = co_await self->raw<double>({120 * self->second, 121 * self->second}, {INFINITY, -INFINITY});
    auto first = co_await self->fold({raw}, "sum");
    auto second = co_await self->fold({first}, "sum");
    auto result = co_await self->read<double>({second});
    EXPECT_EQ(result.values.size(), 1u);
    EXPECT_TRUE(std::isnan(result.values.at(0)));
    EXPECT_EQ(result.rollups.at(0).count, 2u);
}

SEASTAR_TEST_F(RollupPersistenceTest, MetadataStaysAlignedThroughRangeAndTombstoneFiltering) {
    auto raw = co_await self->raw<double>({120 * self->second, 180 * self->second, 240 * self->second}, {1, 2, 3});
    auto folded = co_await self->fold({raw}, "sum");
    auto middle = co_await folded->queryWithTombstones<double>(self->sid, 180 * self->second, 180 * self->second);
    EXPECT_EQ(middle.blocks.at(0)->rollups.size(), 1u);
    EXPECT_EQ(middle.blocks.at(0)->rollups.at(0).latestTimestamp, 180 * self->second);
    co_await folded->deleteRange(self->sid, 180 * self->second, 181 * self->second);
    auto remaining = co_await self->read<double>({folded});
    EXPECT_EQ(remaining.values, (std::vector<double>{1, 3}));
    EXPECT_EQ(remaining.rollups.at(1).latestTimestamp, 240 * self->second);
}

SEASTAR_TEST_F(RollupPersistenceTest, RestartIgnoresInputsOfDurablyPublishedCompaction) {
    auto raw = co_await self->raw<double>({120 * self->second, 121 * self->second}, {1, 1});
    auto folded = co_await self->fold({raw}, "sum");
    // compact() publishes the output but leaves inputs on disk. This is the
    // exact restart window before executeCompaction() removes its source files.
    TSMFileManager restarted;
    co_await restarted.init();
    EXPECT_EQ(restarted.getSequencedTsmFiles().size(), 1u);
    std::vector<seastar::shared_ptr<TSM>> files;
    for (const auto& [rank, file] : restarted.getSequencedTsmFiles())
        files.push_back(file);
    auto result = co_await self->read<double>(files);
    EXPECT_EQ(result.values, std::vector<double>{2});
    co_await restarted.stop();
}

SEASTAR_TEST_F(RollupPersistenceTest, IntegerAverageRetainsWideAccumulatorAcrossStages) {
    auto raw = co_await self->raw<int64_t>({120 * self->second, 121 * self->second, 180 * self->second},
                                           {INT64_MAX, INT64_MAX, INT64_MAX});
    auto first = co_await self->fold({raw}, "avg");
    auto second = co_await self->fold({first}, "avg", 15 * self->minute);
    auto result = co_await self->read<int64_t>({second});
    EXPECT_EQ(result.values, std::vector<int64_t>{INT64_MAX});
    EXPECT_EQ(result.rollups.at(0).count, 3u);
}

SEASTAR_TEST_F(RollupPersistenceTest, ChangedMethodCannotReinterpretExistingRollup) {
    auto raw = co_await self->raw<double>({120 * self->second, 121 * self->second}, {10, 20});
    auto first = co_await self->fold({raw}, "sum");
    bool rejected = false;
    try {
        co_await self->fold({first}, "avg", 15 * self->minute);
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
    auto result = co_await self->read<double>({first});
    EXPECT_EQ(result.values, std::vector<double>{30});
}

TEST(RollupCodecTest, RoundTripAndCorruptionChecks) {
    RollupState state;
    state.interval = 60;
    state.count = 2;
    state.latestTimestamp = 59;
    state.method = 4;
    state.sum = std::numeric_limits<double>::quiet_NaN();
    state.integerSum = static_cast<__int128_t>(INT64_MAX) * 2;
    auto encoded = rollup_codec::encode(std::span(&state, 1));
    auto decoded = rollup_codec::decode(encoded.data(), encoded.size(), 1);
    EXPECT_EQ(decoded.at(0).count, 2u);
    EXPECT_EQ(decoded.at(0).latestTimestamp, 59u);
    EXPECT_TRUE(std::isnan(decoded.at(0).sum));
    EXPECT_TRUE(decoded.at(0).integerSum == state.integerSum);
    EXPECT_THROW(rollup_codec::decode(encoded.data(), encoded.size(), 2), std::runtime_error);
    encoded[0] ^= 1;
    EXPECT_THROW(rollup_codec::decode(encoded.data(), encoded.size(), 1), std::runtime_error);
}

SEASTAR_TEST_F(RollupPersistenceTest, CorruptAncestryCannotRetireUnrelatedFiles) {
    auto raw = co_await self->raw<double>({120 * self->second}, {1});
    auto folded = co_await self->fold({raw}, "sum");
    const auto corruptPath = "shard_0/tsm/0_999.tsm";
    std::filesystem::copy_file(folded->getFilePath(), corruptPath);
    {
        std::fstream bytes(corruptPath, std::ios::in | std::ios::out | std::ios::binary);
        bytes.seekg(9);
        char byte = 0;
        bytes.read(&byte, 1);
        byte ^= 1;
        bytes.seekp(9);
        bytes.write(&byte, 1);
    }
    auto corrupted = seastar::make_shared<TSM>(corruptPath);
    bool rejected = false;
    try {
        co_await corrupted->open();
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
}
