#include "../../../lib/storage/memory_store.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <map>
#include <random>

TEST(InsertOptimizationTest, SharedTimestampPropertiesPreserveGapsAndDuplicates) {
    constexpr uint64_t day = 86400ULL * 1'000'000'000ULL;
    auto batch = std::make_shared<const SharedTimestampBatch>(
        std::make_shared<const std::vector<uint64_t>>(std::vector<uint64_t>{3 * day, day, 3 * day, day + 1}));
    EXPECT_FALSE(batch->sorted);
    EXPECT_FALSE(batch->strictlyIncreasing);
    EXPECT_EQ(batch->min, day);
    EXPECT_EQ(batch->max, 3 * day);
    EXPECT_EQ(batch->days, (std::vector<uint32_t>{1, 3}));

    TimeStarInsert<double> a("m", "a"), b("m", "b");
    a.setSharedTimestamps(batch);
    b.setSharedTimestamps(batch);
    a.values = {30, 10, 31, 11};
    b.values = {300, 100, 310, 110};
    InMemorySeries<double> sa, sb;
    sa.insert(std::move(a));
    sb.insert(std::move(b));
    EXPECT_EQ(sa.timestamps(), (std::vector<uint64_t>{day, day + 1, 3 * day}));
    EXPECT_EQ(sa.values(), (std::vector<double>{10, 11, 31}));
    EXPECT_EQ(sb.values(), (std::vector<double>{100, 110, 310}));
    EXPECT_EQ(batch->timestamps->size(), 4u);
    EXPECT_DOUBLE_EQ(sa.stats.sum, 52);
}

TEST(InsertOptimizationTest, TimestampReplacementCannotReuseStaleProperties) {
    TimeStarInsert<double> insert("m", "f");
    insert.setSharedTimestamps(std::make_shared<const std::vector<uint64_t>>(std::vector<uint64_t>{1, 2, 3}));
    EXPECT_TRUE(insert.sharedTimestampBatch()->strictlyIncreasing);
    insert.setSharedTimestamps(std::make_shared<const std::vector<uint64_t>>(std::vector<uint64_t>{3, 1, 1}));
    insert.values = {30, 10, 11};
    InMemorySeries<double> series;
    series.insert(std::move(insert));
    EXPECT_EQ(series.timestamps(), (std::vector<uint64_t>{1, 3}));
    EXPECT_EQ(series.values(), (std::vector<double>{11, 30}));
}

TEST(InsertOptimizationTest, SmallCorrectionsAndBackfillsMatchLastWriteWinsOracle) {
    InMemorySeries<double> series;
    std::map<uint64_t, double> expected;
    TimeStarInsert<double> initial("m", "f");
    for (uint64_t i = 0; i < 4096; ++i) {
        initial.addValue(i * 2, static_cast<double>(i));
        expected[i * 2] = static_cast<double>(i);
    }
    series.insert(std::move(initial));
    std::mt19937 rng(42);
    for (int round = 0; round < 40; ++round) {
        TimeStarInsert<double> correction("m", "f");
        for (int j = 0; j < 16; ++j) {
            const uint64_t ts = (rng() % 4096) * 2 + (round % 3 == 0 ? 1 : 0);
            const double value = static_cast<double>(rng() % 1000) / 4;
            correction.addValue(ts, value);
            expected[ts] = value;
            if (j % 4 == 0) {
                correction.addValue(ts, value + 1);
                expected[ts] = value + 1;
            }
        }
        series.insert(std::move(correction));
        ASSERT_EQ(series.timestamps().size(), expected.size());
        long double sum = 0, m2 = 0;
        size_t pos = 0;
        for (const auto& [ts, value] : expected) {
            EXPECT_EQ(series.timestamps()[pos], ts);
            EXPECT_DOUBLE_EQ(series.values()[pos++], value);
            sum += value;
        }
        const long double mean = sum / expected.size();
        for (const auto& [ts, value] : expected)
            m2 += (value - mean) * (value - mean);
        EXPECT_EQ(series.stats.count, expected.size());
        EXPECT_NEAR(series.stats.sum, static_cast<double>(sum), 1e-6);
        EXPECT_NEAR(series.stats.m2, static_cast<double>(m2), static_cast<double>(m2) * 1e-12);
    }
}

TEST(InsertOptimizationTest, OverwriteWithNaNRemovesTheOldContribution) {
    InMemorySeries<double> series;
    TimeStarInsert<double> initial("m", "f");
    for (uint64_t i = 0; i < 128; ++i)
        initial.addValue(i, static_cast<double>(i));
    series.insert(std::move(initial));
    TimeStarInsert<double> correction("m", "f");
    correction.addValue(0, std::numeric_limits<double>::quiet_NaN());
    correction.addValue(127, -5);
    series.insert(std::move(correction));
    EXPECT_EQ(series.stats.count, 127u);
    EXPECT_EQ(series.stats.firstTimestamp, 1u);
    EXPECT_DOUBLE_EQ(series.stats.latestValue, -5);
    EXPECT_DOUBLE_EQ(series.stats.min, -5);
    EXPECT_DOUBLE_EQ(series.stats.max, 126);
}

namespace {
void expectCurrentStats(const InMemorySeries<double>& series, double varianceTolerance = 2e-12) {
    const auto& stats = series.stats;
    long double sum = 0, m2 = 0;
    size_t count = 0;
    double min = INFINITY, max = -INFINITY;
    size_t first = 0, last = 0;
    for (size_t i = 0; i < series.values().size(); ++i) {
        const double v = series.values()[i];
        if (std::isnan(v)) continue;
        if (!count) first = i;
        last = i;
        ++count;
        sum += v;
        min = std::min(min, v);
        max = std::max(max, v);
    }
    ASSERT_TRUE(stats.valid);
    EXPECT_EQ(stats.count, count);
    if (!count) return;
    const long double mean = sum / count;
    for (double v : series.values())
        if (!std::isnan(v)) m2 += (v - mean) * (v - mean);
    if (std::isfinite(static_cast<double>(sum)))
        EXPECT_NEAR(stats.sum, static_cast<double>(sum), std::max(1e-8, std::abs(static_cast<double>(sum)) * 2e-15));
    else if (std::isnan(static_cast<double>(sum)))
        EXPECT_TRUE(std::isnan(stats.sum));
    else EXPECT_EQ(stats.sum, static_cast<double>(sum));
    if (std::isfinite(static_cast<double>(m2)))
        EXPECT_NEAR(stats.m2, static_cast<double>(m2), std::max(1e-8, static_cast<double>(m2) * varianceTolerance));
    else EXPECT_TRUE(std::isnan(stats.m2) || std::isinf(stats.m2));
    EXPECT_EQ(stats.min, min);
    EXPECT_EQ(stats.max, max);
    EXPECT_EQ(stats.firstTimestamp, series.timestamps()[first]);
    EXPECT_EQ(stats.latestTimestamp, series.timestamps()[last]);
    EXPECT_EQ(stats.firstValue, series.values()[first]);
    EXPECT_EQ(stats.latestValue, series.values()[last]);
}
}

TEST(InsertOptimizationTest, RepeatedCorrectionsPreserveMomentsAndRequestOrder) {
    for (double offset : {0.0, 1e8}) {
        InMemorySeries<double> series;
        TimeStarInsert<double> initial("m", "v");
        for (size_t i = 0; i < 8192; ++i) initial.addValue(i, offset + double(i % 1000));
        series.insert(std::move(initial));
        std::mt19937 rng(985);
        for (int round = 0; round < 2000; ++round) {
            TimeStarInsert<double> request("m", "v");
            std::map<uint64_t, double> finalValues;
            for (size_t j = 0; j < 32; ++j) {
                const uint64_t ts = rng() % 8192;
                const double value = offset + double(rng() % 4000) / 4;
                request.addValue(ts, value);
                request.addValue(ts, value + .125);
                finalValues[ts] = value + .125;
            }
            series.insert(std::move(request));
            for (const auto& [ts, value] : finalValues) EXPECT_EQ(series.values()[ts], value);
            if (round % 20 == 0) expectCurrentStats(series);
        }
        expectCurrentStats(series);
    }
}

TEST(InsertOptimizationTest, SpecialValuesExtremaAndCancellationMatchFreshMoments) {
    for (double offset : {0.0, 1e12}) {
        InMemorySeries<double> series;
        TimeStarInsert<double> initial("m", "v");
        for (size_t i = 0; i < 8192; ++i) initial.addValue(i, offset + double(i % 127));
        series.insert(std::move(initial));
        for (double value : {-1.0, 10000.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(), 12.25}) {
            TimeStarInsert<double> request("m", "v");
            for (uint64_t ts : {0, 8191, 17, 127, 254}) request.addValue(ts, value);
            series.insert(std::move(request));
            expectCurrentStats(series, offset ? 2e-8 : 2e-12);
        }
    }
    InMemorySeries<double> series;
    TimeStarInsert<double> initial("m", "v");
    for (size_t i = 0; i < 8192; ++i) initial.addValue(i, i < 512 ? 1e8 : 1.0);
    series.insert(std::move(initial));
    TimeStarInsert<double> correction("m", "v");
    for (size_t i = 0; i < 512; ++i) correction.addValue(i, 1.0);
    series.insert(std::move(correction));
    expectCurrentStats(series);
    EXPECT_EQ(series.stats.m2, 0);
}

TEST(InsertOptimizationTest, AppendAfterDeletionCannotReviveStaleStatistics) {
    MemoryStore store(0);
    TimeStarInsert<double> initial("m", "v");
    for (size_t i = 0; i < 256; ++i) initial.addValue(i * 2, double(i));
    const auto id = initial.seriesId128();
    store.insertMemory(std::move(initial));
    store.deleteRange(id, 0, 127 * 2);
    TimeStarInsert<double> append("m", "v");
    append.addValue(512, 300);
    store.insertMemory(std::move(append));
    const auto* series = store.querySeries<double>(id);
    ASSERT_NE(series, nullptr);
    expectCurrentStats(*series);
    EXPECT_EQ(series->stats.count, 129);
    EXPECT_EQ(series->stats.min, 128);
}

TEST(InsertOptimizationTest, GradualVarianceCollapseDoesNotAmplifyOldRoundingError) {
    InMemorySeries<double> series;
    TimeStarInsert<double> initial("m", "v");
    initial.addValue(0, -1);
    initial.addValue(1, 1);
    for (size_t i = 2; i < 65536; ++i) initial.addValue(i, i % 2 ? 1.0 / 3 : -1.0 / 3);
    series.insert(std::move(initial));
    // Each request removes only a small fraction of the remaining variance;
    // the two untouched extrema keep the final variance nonzero.
    for (size_t start = 2; start < 65536; start += 5) {
        TimeStarInsert<double> correction("m", "v");
        for (size_t i = start; i < std::min(start + 5, size_t(65536)); ++i)
            correction.addValue(i, 0);
        series.insert(std::move(correction));
    }
    EXPECT_EQ(series.stats.count, 65536);
    EXPECT_NEAR(series.stats.sum, 0, 1e-12);
    EXPECT_NEAR(series.stats.m2, 2, 1e-12);
}

namespace {
template <typename T>
void checkCorrectionSearchBoundaries() {
    const auto valueFor = [](size_t i) -> T {
        if constexpr (std::is_same_v<T, std::string>) return std::to_string(i);
        else if constexpr (std::is_same_v<T, bool>) return i % 3 == 0;
        else return static_cast<T>(i);
    };
    for (size_t n : {8, 9, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 513}) {
        for (size_t count = 1; count <= std::min(size_t(33), n / 8); ++count) {
            // Include a missing key after complete groups of successful searches,
            // and at the beginning, middle and end of the timestamp range.
            for (int missing : {-1, 0, 1, 2}) {
                SCOPED_TRACE(::testing::Message() << "n=" << n << " count=" << count << " missing=" << missing);
                InMemorySeries<T> series;
                std::map<uint64_t, T> expected;
                TimeStarInsert<T> initial("m", "v");
                std::vector<uint64_t> timestamps;
                for (size_t i = 0; i < n; ++i) {
                    const uint64_t ts = i + 1 == n ? UINT64_MAX - (missing == 2 ? 1 : 0) : 2 * i + 2;
                    timestamps.push_back(ts);
                    initial.addValue(ts, valueFor(i));
                    expected[ts] = valueFor(i);
                }
                series.insert(std::move(initial));
                TimeStarInsert<T> correction("m", "v");
                for (size_t i = 0; i < count; ++i) {
                    // Unsorted requests include both endpoints and repeated keys.
                    uint64_t ts = timestamps[i % 4 == 0 ? 0 : i % 4 == 1 ? n - 1 : (i * 37) % n];
                    if (i + 1 == count && missing >= 0)
                        ts = missing == 0 ? 0 : missing == 1 ? 3 : UINT64_MAX;
                    correction.addValue(ts, valueFor(n + i));
                    expected[ts] = valueFor(n + i);
                }
                series.insert(std::move(correction));
                ASSERT_EQ(series.timestamps().size(), expected.size());
                ASSERT_EQ(series.values().size(), expected.size());
                size_t i = 0;
                for (const auto& [ts, value] : expected) {
                    EXPECT_EQ(series.timestamps()[i], ts);
                    EXPECT_EQ(series.values()[i], value);
                    ++i;
                }
                if constexpr (std::is_same_v<T, double>) expectCurrentStats(series);
            }
        }
    }
}
}

TEST(InsertOptimizationTest, BatchedCorrectionSearchMatchesMapForDouble) { checkCorrectionSearchBoundaries<double>(); }
TEST(InsertOptimizationTest, BatchedCorrectionSearchMatchesMapForInteger) { checkCorrectionSearchBoundaries<int64_t>(); }
TEST(InsertOptimizationTest, BatchedCorrectionSearchMatchesMapForBoolean) { checkCorrectionSearchBoundaries<bool>(); }
TEST(InsertOptimizationTest, BatchedCorrectionSearchMatchesMapForString) { checkCorrectionSearchBoundaries<std::string>(); }
