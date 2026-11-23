// A memory-store series adopts large in-order batches as "pending runs"
// instead of copying them into one growing vector (see InMemorySeries). That
// is purely a representation choice, so these tests hold it to the copying
// path it replaced: the same writes replayed with adoption on, with adoption
// treating every batch as another shard's memory, and with adoption off must
// give the same series, the same running stats bit for bit, and the same TSM
// file byte for byte.

#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/utils/shard_count.hpp"
#include "../../../lib/utils/shard_memory.hpp"

#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <random>
#include <seastar/core/smp.hh>
#include <string>
#include <vector>

namespace {

namespace fs = std::filesystem;

using timestar::memory_store_detail::RunAdoption;

// Scoped setting of the adoption seam.
class AdoptionScope {
    RunAdoption previous_;

public:
    explicit AdoptionScope(RunAdoption adoption) : previous_(timestar::memory_store_detail::runAdoption) {
        timestar::memory_store_detail::runAdoption = adoption;
    }
    ~AdoptionScope() { timestar::memory_store_detail::runAdoption = previous_; }
};

template <class T>
T makeValue(std::mt19937_64& rng) {
    if constexpr (std::is_same_v<T, double>) {
        const uint64_t pick = rng() % 200;
        if (pick == 0)
            return std::numeric_limits<double>::quiet_NaN();
        if (pick == 1)
            return std::numeric_limits<double>::infinity();
        if (pick == 2)
            return -0.0;
        return static_cast<double>(static_cast<int64_t>(rng() % 2000000) - 1000000) / 128.0;
    } else if constexpr (std::is_same_v<T, bool>) {
        return (rng() & 1) != 0;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return static_cast<int64_t>(rng() % 100000) - 50000;
    } else {
        return "v" + std::to_string(rng() % 37);
    }
}

template <class T>
bool sameValues(const std::vector<T>& a, const std::vector<T>& b) {
    if constexpr (std::is_same_v<T, double>) {
        // Bitwise: NaN payloads and the sign of zero are part of the answer.
        return a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(double)) == 0);
    } else {
        return a == b;
    }
}

bool sameBits(double a, double b) {
    return std::bit_cast<uint64_t>(a) == std::bit_cast<uint64_t>(b);
}

::testing::AssertionResult sameStats(const InMemorySeriesStats& a, const InMemorySeriesStats& b) {
    const bool same = a.valid == b.valid && a.count == b.count && sameBits(a.sum, b.sum) &&
                      sameBits(a.sumCompensation, b.sumCompensation) && sameBits(a.min, b.min) &&
                      sameBits(a.max, b.max) && sameBits(a.mean, b.mean) && sameBits(a.m2, b.m2) &&
                      sameBits(a.firstValue, b.firstValue) && a.firstTimestamp == b.firstTimestamp &&
                      sameBits(a.latestValue, b.latestValue) && a.latestTimestamp == b.latestTimestamp;
    if (same)
        return ::testing::AssertionSuccess();
    return ::testing::AssertionFailure() << "stats differ: count " << a.count << " vs " << b.count << ", sum " << a.sum
                                         << " vs " << b.sum << ", m2 " << a.m2 << " vs " << b.m2 << ", valid "
                                         << a.valid << " vs " << b.valid;
}

// One write, described so it can be replayed into several series.
template <class T>
struct Batch {
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    bool shared = false;  // deliver the timestamps as a SharedTimestampBatch
};

template <class T>
TimeStarInsert<T> toInsert(const Batch<T>& batch) {
    TimeStarInsert<T> insert("m", "f");
    insert.values = batch.values;
    if (batch.shared) {
        insert.setSharedTimestamps(std::make_shared<const std::vector<uint64_t>>(batch.timestamps));
    } else {
        insert.timestamps = batch.timestamps;
    }
    return insert;
}

template <class T>
Batch<T> inOrderBatch(std::mt19937_64& rng, uint64_t after, size_t n, bool shared) {
    Batch<T> batch;
    batch.shared = shared;
    uint64_t t = after;
    for (size_t i = 0; i < n; ++i) {
        t += 1 + rng() % 5;
        batch.timestamps.push_back(t);
        batch.values.push_back(makeValue<T>(rng));
    }
    return batch;
}

template <class T>
class SeriesRunsTest : public ::testing::Test {};
using SeriesTypes = ::testing::Types<double, int64_t, bool, std::string>;
TYPED_TEST_SUITE(SeriesRunsTest, SeriesTypes);

// Every write shape, in random order, with reads and deletions in between.
TYPED_TEST(SeriesRunsTest, AdoptedRunsMatchTheCopyingPathOnEveryWriteShape) {
    using T = TypeParam;
    constexpr size_t kBig = InMemorySeries<T>::kMinAdoptedRunPoints;

    for (uint64_t seed = 1; seed <= 12; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        std::mt19937_64 rng(seed * 7919);
        InMemorySeries<T> copying;   // the definition
        InMemorySeries<T> adopting;  // production behaviour for local batches
        InMemorySeries<T> foreign;   // production behaviour for batches from another shard
        const std::array<InMemorySeries<T>*, 2> candidates = {&adopting, &foreign};
        const std::array<InMemorySeries<T>*, 3> all = {&copying, &adopting, &foreign};
        size_t adoptedAtLeastOnce = 0;

        for (int step = 0; step < 60; ++step) {
            SCOPED_TRACE("step " + std::to_string(step));
            const uint64_t last = copying.empty() ? 1000 : copying.lastTimestamp();
            const uint64_t first = copying.empty() ? 1000 : copying.firstTimestamp();
            const uint64_t op = rng() % 16;
            Batch<T> batch;
            bool isWrite = true;

            if (op < 6) {
                // The shape adoption is for: a sizeable, strictly in-order append.
                batch = inOrderBatch<T>(rng, last, kBig + rng() % 700, (rng() & 1) != 0);
            } else if (op == 6) {
                // Sizeable, but its first point overwrites the current tail.
                batch = inOrderBatch<T>(rng, last, kBig + 10, (rng() & 1) != 0);
                batch.timestamps.front() = last;
            } else if (op == 7) {
                // Sizeable, in order, but repeats a timestamp inside itself.
                batch = inOrderBatch<T>(rng, last, kBig + 10, (rng() & 1) != 0);
                batch.timestamps[kBig / 2] = batch.timestamps[kBig / 2 - 1];
            } else if (op == 8) {
                // Sizeable and overlapping what is already held.
                const uint64_t start = first + (last - first) / 2;
                batch = inOrderBatch<T>(rng, start, kBig + 10, (rng() & 1) != 0);
            } else if (op == 9) {
                // Sizeable and unsorted.
                batch = inOrderBatch<T>(rng, last, kBig + 10, (rng() & 1) != 0);
                std::swap(batch.timestamps[3], batch.timestamps[kBig]);
            } else if (op == 10 || op == 11) {
                // Small append: below the adoption threshold.
                batch = inOrderBatch<T>(rng, last, 1 + rng() % 40, (rng() & 1) != 0);
            } else if (op == 12) {
                // Small correction of points already held.
                if (copying.size() < 100) {
                    isWrite = false;
                } else {
                    const auto& held = copying.timestamps();
                    size_t at = rng() % (held.size() - 20);
                    for (int i = 0; i < 8; ++i, at += 1 + rng() % 2) {
                        batch.timestamps.push_back(held[at]);
                        batch.values.push_back(makeValue<T>(rng));
                    }
                }
            } else if (op == 13) {
                // A read: consolidates whatever is pending.
                isWrite = false;
                for (auto* series : candidates) {
                    ASSERT_EQ(series->timestamps(), copying.timestamps());
                    ASSERT_TRUE(sameValues(series->values(), copying.values()));
                    ASSERT_EQ(series->runCount(), copying.runCount());
                }
            } else if (op == 14) {
                // A deletion, exactly as MemoryStore::deleteRange performs it.
                isWrite = false;
                if (copying.size() > 50) {
                    const uint64_t from = first + (last - first) / 3;
                    const uint64_t to = from + (last - first) / 10;
                    for (auto* series : all) {
                        auto& ts = series->timestamps();
                        auto& vals = series->values();
                        const auto lo = std::lower_bound(ts.begin(), ts.end(), from) - ts.begin();
                        const auto hi = std::upper_bound(ts.begin(), ts.end(), to) - ts.begin();
                        ts.erase(ts.begin() + lo, ts.begin() + hi);
                        vals.erase(vals.begin() + lo, vals.begin() + hi);
                        series->stats.valid = false;
                    }
                }
            } else {
                // Delete everything, so the next write starts a series afresh.
                isWrite = false;
                for (auto* series : all) {
                    series->timestamps().clear();
                    series->values().clear();
                    series->stats.valid = false;
                }
            }

            if (isWrite) {
                {
                    AdoptionScope scope(RunAdoption::Off);
                    copying.insert(toInsert(batch));
                }
                {
                    AdoptionScope scope(RunAdoption::Auto);
                    adopting.insert(toInsert(batch));
                }
                {
                    AdoptionScope scope(RunAdoption::Foreign);
                    foreign.insert(toInsert(batch));
                }
                adoptedAtLeastOnce += adopting.runCount() > 1;
                ASSERT_LE(copying.runCount(), 1u);
                // Where a batch's memory came from never decides whether it
                // becomes a run, only whether the run copies it.
                ASSERT_EQ(foreign.runCount(), adopting.runCount());
            }

            // What can be asked without consolidating must already agree.
            for (auto* series : candidates) {
                ASSERT_EQ(series->size(), copying.size());
                ASSERT_EQ(series->empty(), copying.empty());
                if (!copying.empty()) {
                    ASSERT_EQ(series->firstTimestamp(), copying.firstTimestamp());
                    ASSERT_EQ(series->lastTimestamp(), copying.lastTimestamp());
                }
                if constexpr (std::is_same_v<T, double>) {
                    if (series->stats.valid || copying.stats.valid) {
                        ASSERT_TRUE(sameStats(series->stats, copying.stats));
                    }
                }
            }
        }

        EXPECT_GT(adoptedAtLeastOnce, 0u) << "the sequence never exercised adoption";
        for (auto* series : candidates) {
            ASSERT_EQ(series->timestamps(), copying.timestamps());
            ASSERT_TRUE(sameValues(series->values(), copying.values()));
        }
        ASSERT_TRUE(std::ranges::is_sorted(copying.timestamps()));
        ASSERT_EQ(std::ranges::adjacent_find(copying.timestamps()), copying.timestamps().end());
    }
}

// The point of the structure: an in-order batch is held where it arrived.
TEST(SeriesRunsStorageTest, AnInOrderBatchIsAdoptedWithoutCopying) {
    constexpr size_t n = InMemorySeries<double>::kMinAdoptedRunPoints + 500;
    InMemorySeries<double> series;
    std::vector<const double*> valueStorage;
    std::vector<const uint64_t*> timestampStorage;

    uint64_t t = 0;
    for (int b = 0; b < 3; ++b) {
        auto shared = std::make_shared<std::vector<uint64_t>>();
        TimeStarInsert<double> insert("m", "f");
        for (size_t i = 0; i < n; ++i) {
            shared->push_back(++t);
            insert.values.push_back(static_cast<double>(t));
        }
        valueStorage.push_back(insert.values.data());
        timestampStorage.push_back(shared->data());
        insert.setSharedTimestamps(std::shared_ptr<const std::vector<uint64_t>>(shared));
        series.insert(std::move(insert));
    }

    EXPECT_EQ(series.size(), 3 * n);
    EXPECT_EQ(series.firstTimestamp(), 1u);
    EXPECT_EQ(series.lastTimestamp(), 3 * n);
    ASSERT_EQ(series.runCount(), 3u);
    size_t run = 0;
    series.forEachRun([&](const std::vector<uint64_t>& ts, const std::vector<double>& vals) {
        EXPECT_EQ(vals.data(), valueStorage[run]) << "run " << run << " values were copied";
        EXPECT_EQ(ts.data(), timestampStorage[run]) << "run " << run << " timestamps were copied";
        EXPECT_EQ(ts.size(), n);
        ++run;
    });
    EXPECT_EQ(run, 3u);

    // Stats cover every adopted point without anything having been read.
    EXPECT_EQ(series.stats.count, 3 * n);
    EXPECT_EQ(series.stats.firstTimestamp, 1u);
    EXPECT_EQ(series.stats.latestTimestamp, 3 * n);

    // The first read sees one sorted series.
    const auto& all = series.timestamps();
    ASSERT_EQ(all.size(), 3 * n);
    EXPECT_EQ(series.runCount(), 1u);
    for (size_t i = 0; i < all.size(); ++i) {
        ASSERT_EQ(all[i], i + 1);
        ASSERT_EQ(series.values()[i], static_cast<double>(i + 1));
    }
}

// A batch that is another shard's memory still becomes a run, but the run
// owns exact-size copies: nothing of the other shard's is kept.
TEST(SeriesRunsStorageTest, ABatchFromAnotherShardIsCopiedIntoExactSizeStorage) {
    constexpr size_t n = InMemorySeries<double>::kMinAdoptedRunPoints + 500;
    AdoptionScope scope(RunAdoption::Foreign);
    InMemorySeries<double> series;
    std::vector<const double*> requestValues;
    std::vector<const uint64_t*> requestTimestamps;
    std::vector<std::weak_ptr<const std::vector<uint64_t>>> sharedTimestamps;

    uint64_t t = 0;
    for (int b = 0; b < 2; ++b) {
        auto shared = std::make_shared<std::vector<uint64_t>>();
        TimeStarInsert<double> insert("m", "f");
        insert.values.reserve(4 * n);  // slack the run must not inherit
        for (size_t i = 0; i < n; ++i) {
            shared->push_back(++t);
            insert.values.push_back(static_cast<double>(t));
        }
        requestValues.push_back(insert.values.data());
        requestTimestamps.push_back(shared->data());
        sharedTimestamps.emplace_back(shared);
        insert.setSharedTimestamps(std::shared_ptr<const std::vector<uint64_t>>(std::move(shared)));
        series.insert(std::move(insert));
    }

    ASSERT_EQ(series.runCount(), 2u);
    for (const auto& weak : sharedTimestamps)
        EXPECT_TRUE(weak.expired()) << "the series kept the request's timestamps alive";
    size_t run = 0;
    series.forEachRun([&](const std::vector<uint64_t>& ts, const std::vector<double>& vals) {
        EXPECT_NE(vals.data(), requestValues[run]);
        EXPECT_NE(ts.data(), requestTimestamps[run]);
        EXPECT_EQ(vals.size(), n);
        EXPECT_EQ(vals.capacity(), n);
        EXPECT_EQ(ts.capacity(), n);
        ++run;
    });
    EXPECT_EQ(series.stats.count, 2 * n);
    EXPECT_EQ(series.timestamps().size(), 2 * n);
    EXPECT_EQ(series.timestamps().back(), 2 * n);
}

TEST(SeriesRunsStorageTest, WritesThatAreNotPlainAppendsAreNotAdopted) {
    constexpr size_t n = InMemorySeries<int64_t>::kMinAdoptedRunPoints;
    auto batch = [](uint64_t from, size_t count) {
        TimeStarInsert<int64_t> insert("m", "f");
        for (size_t i = 0; i < count; ++i) {
            insert.timestamps.push_back(from + i);
            insert.values.push_back(static_cast<int64_t>(from + i));
        }
        return insert;
    };

    InMemorySeries<int64_t> series;
    series.insert(batch(1, n));
    series.insert(batch(1 + n, n));
    ASSERT_EQ(series.runCount(), 2u);

    series.insert(batch(1 + 2 * n, n - 1));  // below the threshold
    EXPECT_EQ(series.runCount(), 1u);
    series.insert(batch(3 * n - 1, n));  // starts ON the tail: an overwrite
    EXPECT_EQ(series.runCount(), 1u);
    series.insert(batch(10, n));  // overlaps
    EXPECT_EQ(series.runCount(), 1u);

    EXPECT_EQ(series.size(), 4 * n - 2);
    EXPECT_TRUE(std::ranges::is_sorted(series.timestamps()));
}

// ---------------------------------------------------------------------------
// Real cross-shard memory (needs a second shard)
// ---------------------------------------------------------------------------

// A three-field write parsed on shard 1, as the HTTP handler would hand it over.
struct ForeignWrite {
    std::vector<TimeStarInsert<double>> fields;
    const uint64_t* timestampStorage = nullptr;
    std::vector<const double*> valueStorage;
};

ForeignWrite buildWrite(size_t n) {
    ForeignWrite write;
    auto timestamps = std::make_shared<std::vector<uint64_t>>();
    for (size_t i = 0; i < n; ++i)
        timestamps->push_back(1000 + i);
    write.timestampStorage = timestamps->data();
    auto batch = std::make_shared<const SharedTimestampBatch>(
        std::shared_ptr<const std::vector<uint64_t>>(std::move(timestamps)));
    for (const char* field : {"a", "b", "c"}) {
        TimeStarInsert<double> insert("xshard", field);
        for (size_t i = 0; i < n; ++i)
            insert.values.push_back(static_cast<double>(i) + (field[0] - 'a'));
        write.valueStorage.push_back(insert.values.data());
        insert.setSharedTimestamps(batch);
        write.fields.push_back(std::move(insert));
    }
    return write;
}

TEST(SeriesRunsCrossShardTest, AStoreKeepsNothingOfAnotherShardsMemory) {
    if (timestar::shardCount() < 2) {
        // Same convention as the other multi-shard tests: CI pins two shards,
        // so there a missing second shard is a failure, not a skip.
        if (std::getenv("CI")) {
            FAIL() << "needs at least two shards (CI runs with -c 2)";
        }
        GTEST_SKIP() << "needs at least two shards";
    }
    if (!timestar::hasShardMemoryPools()) {
        // A property of the build, not of how the suite was started: Seastar's
        // debug mode uses the system allocator, so there is no foreign memory.
        GTEST_SKIP() << "this Seastar build has no per-shard memory pools (system allocator)";
    }
    constexpr size_t n = InMemorySeries<double>::kMinAdoptedRunPoints * 3;

    ForeignWrite write = seastar::smp::submit_to(1, [] { return buildWrite(n); }).get();
    ASSERT_FALSE(timestar::isShardLocalMemory(write.timestampStorage));
    ASSERT_FALSE(timestar::isShardLocalMemory(write.valueStorage[0]));
    std::vector<int> probe(64);
    ASSERT_TRUE(timestar::isShardLocalMemory(probe.data()));

    MemoryStore store(1);
    for (auto& field : write.fields)
        store.insertMemory(std::move(field));

    ASSERT_EQ(store.series.size(), 3u);
    std::vector<const uint64_t*> timestampStorage;
    for (const auto& [id, variant] : store.series) {
        const auto& series = std::get<InMemorySeries<double>>(variant);
        ASSERT_EQ(series.size(), n);
        ASSERT_EQ(series.runCount(), 1u);
        series.forEachRun([&](const std::vector<uint64_t>& ts, const std::vector<double>& vals) {
            EXPECT_TRUE(timestar::isShardLocalMemory(ts.data()));
            EXPECT_TRUE(timestar::isShardLocalMemory(vals.data()));
            EXPECT_EQ(vals.capacity(), n);
            EXPECT_EQ(ts.front(), 1000u);
            EXPECT_EQ(ts.back(), 1000u + n - 1);
            timestampStorage.push_back(ts.data());
        });
    }
    // One local copy of the write's timestamps, shared by its fields.
    ASSERT_EQ(timestampStorage.size(), 3u);
    EXPECT_EQ(timestampStorage[0], timestampStorage[1]);
    EXPECT_EQ(timestampStorage[1], timestampStorage[2]);
    EXPECT_NE(timestampStorage[0], write.timestampStorage);

    // The same write parsed here is kept as it stands.
    ForeignWrite local = buildWrite(n);
    MemoryStore localStore(2);
    for (auto& field : local.fields)
        localStore.insertMemory(std::move(field));
    size_t i = 0;
    for (const char* field : {"a", "b", "c"}) {
        TimeStarInsert<double> key("xshard", field);
        const auto* series = localStore.querySeries<double>(key.seriesId128());
        ASSERT_NE(series, nullptr);
        series->forEachRun([&](const std::vector<uint64_t>& ts, const std::vector<double>& vals) {
            EXPECT_EQ(ts.data(), local.timestampStorage);
            EXPECT_EQ(vals.data(), local.valueStorage[i]);
        });
        ++i;
    }
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

class SeriesRunsFlushTest : public ::testing::Test {
protected:
    std::string testDir = "./test_memory_store_runs_files";
    void SetUp() override { fs::create_directories(testDir); }
    void TearDown() override { fs::remove_all(testDir); }

    static std::string slurp(const std::string& path) {
        std::ifstream in(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
};

template <class T>
void expectRunsWriteTheSameFile(const std::string& dir, TSMValueType type, uint64_t seed) {
    std::mt19937_64 rng(seed);
    const SeriesId128 seriesId = SeriesId128::fromSeriesKey("runs.series");

    // Run lengths chosen to hit every relation to the block size: runs far
    // smaller than a block, exactly one block, several blocks, and boundaries
    // that fall one point either side of a block edge.
    const size_t block = MaxPointsPerBlock();
    const std::vector<size_t> shapes = {1, 2, 7, block - 1, block, block + 1, 2 * block, 2 * block + 3, 5, block / 2};
    std::vector<std::vector<uint64_t>> runTimestamps;
    std::vector<std::vector<T>> runValues;
    std::vector<uint64_t> allTimestamps;
    std::vector<T> allValues;
    uint64_t t = 1'700'000'000'000'000'000ULL;
    const size_t runCount = 6 + rng() % 10;
    for (size_t r = 0; r < runCount; ++r) {
        const size_t n = shapes[rng() % shapes.size()];
        auto& ts = runTimestamps.emplace_back();
        auto& vals = runValues.emplace_back();
        for (size_t i = 0; i < n; ++i) {
            t += 1'000'000 + rng() % 1000;
            ts.push_back(t);
            vals.push_back(makeValue<T>(rng));
        }
        allTimestamps.insert(allTimestamps.end(), ts.begin(), ts.end());
        allValues.insert(allValues.end(), vals.begin(), vals.end());
    }

    const std::string whole = dir + "/whole_" + std::to_string(seed) + ".tsm";
    const std::string pieces = dir + "/pieces_" + std::to_string(seed) + ".tsm";
    {
        TSMWriter writer(whole);
        writer.writeSeries(type, seriesId, allTimestamps, allValues);
        writer.writeIndex();
        writer.close();
    }
    {
        std::vector<TSMWriter::SeriesRun<T>> runs;
        for (size_t r = 0; r < runCount; ++r)
            runs.push_back({&runTimestamps[r], &runValues[r]});
        TSMWriter writer(pieces);
        writer.writeSeriesRuns<T>(type, seriesId, runs);
        writer.writeIndex();
        writer.close();
    }

    std::ifstream a(whole, std::ios::binary), b(pieces, std::ios::binary);
    const std::string wholeBytes((std::istreambuf_iterator<char>(a)), std::istreambuf_iterator<char>());
    const std::string pieceBytes((std::istreambuf_iterator<char>(b)), std::istreambuf_iterator<char>());
    ASSERT_FALSE(wholeBytes.empty());
    EXPECT_TRUE(wholeBytes == pieceBytes) << "seed " << seed << ": " << runCount << " runs, " << allTimestamps.size()
                                          << " points, sizes " << wholeBytes.size() << " vs " << pieceBytes.size();
}

TEST_F(SeriesRunsFlushTest, RunsAreWrittenAsTheSameBytesAsOneVector) {
    for (uint64_t seed = 1; seed <= 8; ++seed) {
        expectRunsWriteTheSameFile<double>(testDir, TSMValueType::Float, seed);
        expectRunsWriteTheSameFile<int64_t>(testDir, TSMValueType::Integer, seed + 100);
        expectRunsWriteTheSameFile<bool>(testDir, TSMValueType::Boolean, seed + 200);
    }
}

// End to end: a store that adopted its batches flushes to the same file as one
// that copied them.
TEST_F(SeriesRunsFlushTest, AStoreOfAdoptedRunsFlushesToTheSameFile) {
    constexpr size_t n = InMemorySeries<double>::kMinAdoptedRunPoints + 333;

    auto fill = [&](MemoryStore& store, RunAdoption adoption) {
        AdoptionScope scope(adoption);
        std::mt19937_64 rng(4242);
        uint64_t t = 1'700'000'000'000'000'000ULL;
        for (int round = 0; round < 9; ++round) {
            auto shared = std::make_shared<std::vector<uint64_t>>();
            for (size_t i = 0; i < n; ++i) {
                t += 1'000'000'000ULL;
                shared->push_back(t);
            }
            const std::shared_ptr<const std::vector<uint64_t>> timestamps = shared;
            auto write = [&]<class T>(const std::string& field) {
                TimeStarInsert<T> insert("runs", field);
                insert.addTag("host", "a");
                for (size_t i = 0; i < n; ++i)
                    insert.values.push_back(makeValue<T>(rng));
                insert.setSharedTimestamps(timestamps);
                store.insertMemory(std::move(insert));
            };
            write.template operator()<double>("load");
            write.template operator()<int64_t>("requests");
            write.template operator()<bool>("up");
            write.template operator()<std::string>("state");
            if (round == 4) {
                // A small late correction on one field forces that series
                // through the general path mid-stream.
                TimeStarInsert<double> fix("runs", "load");
                fix.addTag("host", "a");
                fix.addValue(t - 5'000'000'000ULL, 123.5);
                store.insertMemory(std::move(fix));
            }
        }
    };

    auto copying = seastar::make_shared<MemoryStore>(1);
    auto adopting = seastar::make_shared<MemoryStore>(2);
    auto foreign = seastar::make_shared<MemoryStore>(3);
    fill(*copying, RunAdoption::Off);
    fill(*adopting, RunAdoption::Auto);
    fill(*foreign, RunAdoption::Foreign);

    for (const auto& store : {adopting, foreign}) {
        size_t multiRunSeries = 0;
        for (const auto& [id, series] : store->series)
            multiRunSeries += std::visit([](const auto& s) { return s.runCount() > 1; }, series);
        EXPECT_GE(multiRunSeries, 3u) << "store " << store->sequenceNumber << " holds no runs to flush";
    }

    const std::string copiedFile = testDir + "/0_1.tsm";
    const std::string adoptedFile = testDir + "/0_2.tsm";
    const std::string foreignFile = testDir + "/0_3.tsm";
    TSMWriter::run(copying, copiedFile);
    TSMWriter::run(adopting, adoptedFile);
    TSMWriter::run(foreign, foreignFile);

    const std::string copiedBytes = slurp(copiedFile);
    const std::string adoptedBytes = slurp(adoptedFile);
    const std::string foreignBytes = slurp(foreignFile);
    ASSERT_FALSE(copiedBytes.empty());
    EXPECT_TRUE(adoptedBytes == copiedBytes) << "sizes " << adoptedBytes.size() << " vs " << copiedBytes.size();
    EXPECT_TRUE(foreignBytes == copiedBytes) << "sizes " << foreignBytes.size() << " vs " << copiedBytes.size();
}

}  // namespace
