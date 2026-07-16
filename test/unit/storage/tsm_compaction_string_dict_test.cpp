// Regression tests for the string-dictionary compaction bug.
//
// ROOT CAUSE (pre-fix): TSMCompactor's zero-copy path copied compressed STR2
// (dictionary-encoded) string blocks verbatim into the compacted file, but the
// output file's TSM index entry never received the source file's string
// dictionary (writeCompressedBlockWithStats never set it, and
// SeriesCompactionData had no field to carry it). The compacted file's index
// stored dictSize=0 for the series, so every read of the carried STR2 blocks
// fell into the raw STRG decoder and threw "Invalid magic number in string
// encoding". The query layer swallows per-series read errors, so string-field
// series silently vanished from query results after their first compaction —
// permanently, surviving restarts — while sibling numeric fields (self-
// contained encodings) kept working. Tier-N -> N+1 compactions of the broken
// file then failed forever with the same error.
//
// STRUCTURE CAUGHT: the string dictionary of the compacted file's TSM index
// entry (TSMIndexEntry::stringDictionary / on-disk dictSize field), plus full
// value round-trip through the compacted file across reopen and a second
// compaction cycle.

#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_compactor.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

class TSMCompactionStringDictTest : public ::testing::Test {
public:
    std::string testDir = "./test_compaction_string_dict";
    fs::path savedCwd;
    std::unique_ptr<TSMFileManager> fileManager;
    std::unique_ptr<TSMCompactor> compactor;

    void SetUp() override {
        savedCwd = fs::current_path();
        if (fs::current_path().filename() == "test_compaction_string_dict") {
            fs::current_path(savedCwd.parent_path());
            savedCwd = fs::current_path();
        }
        fs::remove_all(testDir);
        fs::create_directories(testDir + "/shard_0/tsm");
        fs::current_path(testDir);

        fileManager = std::make_unique<TSMFileManager>();
        compactor = std::make_unique<TSMCompactor>(fileManager.get());
    }

    void TearDown() override {
        compactor.reset();
        fileManager.reset();
        fs::current_path(savedCwd);
        fs::remove_all(testDir);
    }

    // Write a TSM file containing the given string series (and optionally a
    // float series so multi-file compactions have numeric content too).
    static void writeFile(const std::string& filename, const SeriesId128& stringSeries,
                          const std::vector<uint64_t>& ts, const std::vector<std::string>& vals,
                          bool includeFloatSeries = false) {
        TSMWriter writer(filename);
        if (!ts.empty()) {
            writer.writeSeries(TSMValueType::String, stringSeries, ts, vals);
        }
        if (includeFloatSeries) {
            std::vector<uint64_t> fts;
            std::vector<double> fvals;
            for (int i = 0; i < 20; ++i) {
                fts.push_back(1000 + i * 100);
                fvals.push_back(i * 1.5);
            }
            writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey("floats.sibling"), fts, fvals);
        }
        writer.writeIndex();
        writer.close();
    }

    static seastar::future<seastar::shared_ptr<TSM>> openFile(const std::string& filename, uint64_t tier,
                                                              uint64_t seq) {
        auto tsm = seastar::make_shared<TSM>(filename);
        tsm->tierNum = tier;
        tsm->seqNum = seq;
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        co_return tsm;
    }

    // Read all points of a string series from a freshly-opened file
    // (simulates a reopen: no shared in-memory state with the writer).
    static seastar::future<std::map<uint64_t, std::string>> readAll(const std::string& filename,
                                                                    const SeriesId128& seriesId) {
        auto tsm = seastar::make_shared<TSM>(filename);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        TSMResult<std::string> result(0);
        co_await tsm->readSeries(seriesId, 0, UINT64_MAX, result);
        std::map<uint64_t, std::string> points;
        for (const auto& block : result.blocks) {
            for (size_t i = 0; i < block->timestamps.size(); ++i) {
                points[block->timestamps[i]] = block->values[i];
            }
        }
        co_await tsm->close();
        co_return points;
    }
};

// Single-source zero-copy carry: the string series exists in one input file
// with a dictionary (repeated values => STR2 blocks). Pre-fix, the compacted
// file's index entry lost the dictionary and every read threw. Post-fix, the
// dictionary is carried into the output index entry and values round-trip.
SEASTAR_TEST_F(TSMCompactionStringDictTest, ZeroCopyCarryPreservesDictionary) {
    SeriesId128 sid = SeriesId128::fromSeriesKey("strings.dictseries");

    // 8 points from a 2-value alphabet => buildDictionary succeeds => STR2.
    std::vector<uint64_t> ts;
    std::vector<std::string> vals;
    for (int i = 0; i < 8; ++i) {
        ts.push_back(1000 + i * 1000);
        vals.push_back(i % 2 == 0 ? "state_on" : "state_off");
    }
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_1.tsm", sid, ts, vals, /*includeFloatSeries=*/true);

    // Second input file without the string series, so the compaction is a
    // realistic multi-file job while the string series stays single-source
    // (the common case that took the zero-copy path).
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_2.tsm", sid, {}, {}, /*includeFloatSeries=*/true);

    std::vector<seastar::shared_ptr<TSM>> files;
    files.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_1.tsm", 0, 1));
    files.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_2.tsm", 0, 2));

    auto result = co_await self->compactor->compact(files);
    EXPECT_FALSE(result.outputPath.empty());
    EXPECT_TRUE(fs::exists(result.outputPath));
    if (result.outputPath.empty()) {
        co_return;
    }

    // Structural check: the compacted file's index entry must carry the
    // string dictionary (this is the structure the bug dropped).
    {
        auto compacted = seastar::make_shared<TSM>(result.outputPath);
        co_await compacted->open();
        co_await compacted->readSparseIndex();
        auto* entry = co_await compacted->getFullIndexEntry(sid);
        EXPECT_NE(entry, nullptr);
        if (entry != nullptr) {
            EXPECT_TRUE(entry->stringDictionary != nullptr && !entry->stringDictionary->empty())
                << "compacted index entry lost the string dictionary";
        }
        co_await compacted->close();
    }

    // Full round-trip through a fresh open (reopen semantics).
    auto points = co_await TSMCompactionStringDictTest::readAll(result.outputPath, sid);
    EXPECT_EQ(points.size(), 8u);
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(points[1000 + i * 1000], i % 2 == 0 ? "state_on" : "state_off");
    }

    co_return;
}

// Multi-source dictionaries: the same string series exists in TWO input files,
// each with its own dictionary and non-overlapping time ranges. Zero-copy
// carry is unsound here (the two files' dictionary IDs are incompatible), so
// the series must be re-encoded. Pre-fix this produced dictionary-less STR2
// blocks that could never be decoded.
SEASTAR_TEST_F(TSMCompactionStringDictTest, MultiSourceDictionariesReencodeCorrectly) {
    SeriesId128 sid = SeriesId128::fromSeriesKey("strings.multisource");

    std::vector<uint64_t> tsA, tsB;
    std::vector<std::string> valsA, valsB;
    for (int i = 0; i < 6; ++i) {
        tsA.push_back(1000 + i * 1000);
        valsA.push_back(i % 2 == 0 ? "alpha" : "bravo");  // dict A: {alpha, bravo}
        tsB.push_back(1000000 + i * 1000);                // disjoint, later range
        valsB.push_back(i % 2 == 0 ? "charlie" : "delta");  // dict B: {charlie, delta}
    }
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_1.tsm", sid, tsA, valsA);
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_2.tsm", sid, tsB, valsB);

    std::vector<seastar::shared_ptr<TSM>> files;
    files.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_1.tsm", 0, 1));
    files.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_2.tsm", 0, 2));

    auto result = co_await self->compactor->compact(files);
    EXPECT_FALSE(result.outputPath.empty());
    if (result.outputPath.empty()) {
        co_return;
    }

    auto points = co_await TSMCompactionStringDictTest::readAll(result.outputPath, sid);
    EXPECT_EQ(points.size(), 12u);
    for (int i = 0; i < 6; ++i) {
        EXPECT_EQ(points[1000 + i * 1000], i % 2 == 0 ? "alpha" : "bravo");
        EXPECT_EQ(points[1000000 + i * 1000], i % 2 == 0 ? "charlie" : "delta");
    }

    co_return;
}

// Raw (STRG) string blocks are self-contained; the zero-copy carry must keep
// working for them. Use >MAX_DICT_ENTRIES unique values so buildDictionary
// declines and the writer emits raw blocks.
SEASTAR_TEST_F(TSMCompactionStringDictTest, RawStringBlocksStillZeroCopy) {
    SeriesId128 sid = SeriesId128::fromSeriesKey("strings.rawseries");

    std::vector<uint64_t> ts;
    std::vector<std::string> vals;
    for (int i = 0; i < 60; ++i) {  // 60 unique values > MAX_DICT_ENTRIES (50)
        ts.push_back(1000 + i * 1000);
        vals.push_back("unique_value_" + std::to_string(i));
    }
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_1.tsm", sid, ts, vals, /*includeFloatSeries=*/true);
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_2.tsm", sid, {}, {}, /*includeFloatSeries=*/true);

    std::vector<seastar::shared_ptr<TSM>> files;
    files.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_1.tsm", 0, 1));
    files.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_2.tsm", 0, 2));

    auto result = co_await self->compactor->compact(files);
    EXPECT_FALSE(result.outputPath.empty());
    if (result.outputPath.empty()) {
        co_return;
    }

    auto points = co_await TSMCompactionStringDictTest::readAll(result.outputPath, sid);
    EXPECT_EQ(points.size(), 60u);
    for (int i = 0; i < 60; ++i) {
        EXPECT_EQ(points[1000 + i * 1000], "unique_value_" + std::to_string(i));
    }

    co_return;
}

// Durability across repeated compaction cycles: tier0 -> tier1 -> tier2, with
// a fresh file open after each cycle. Pre-fix, the FIRST compaction already
// corrupted the series and the second failed outright.
SEASTAR_TEST_F(TSMCompactionStringDictTest, DictionarySurvivesRepeatedCompactionAndReopen) {
    SeriesId128 sid = SeriesId128::fromSeriesKey("strings.recompact");

    std::vector<uint64_t> ts;
    std::vector<std::string> vals;
    for (int i = 0; i < 10; ++i) {
        ts.push_back(1000 + i * 1000);
        vals.push_back(i % 3 == 0 ? "red" : (i % 3 == 1 ? "green" : "blue"));
    }
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_1.tsm", sid, ts, vals, /*includeFloatSeries=*/true);
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/0_2.tsm", sid, {}, {}, /*includeFloatSeries=*/true);

    // Cycle 1: tier0 -> tier1
    std::vector<seastar::shared_ptr<TSM>> tier0;
    tier0.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_1.tsm", 0, 1));
    tier0.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/0_2.tsm", 0, 2));
    auto r1 = co_await self->compactor->compact(tier0);
    EXPECT_FALSE(r1.outputPath.empty());
    if (r1.outputPath.empty()) {
        co_return;
    }

    // Cycle 2: compact the tier1 output (fresh open = reopen) with another file.
    TSMCompactionStringDictTest::writeFile("shard_0/tsm/1_50.tsm", sid, {}, {}, /*includeFloatSeries=*/true);
    std::vector<seastar::shared_ptr<TSM>> tier1;
    tier1.push_back(co_await TSMCompactionStringDictTest::openFile(r1.outputPath, 1, 40));
    tier1.push_back(co_await TSMCompactionStringDictTest::openFile("shard_0/tsm/1_50.tsm", 1, 50));
    auto r2 = co_await self->compactor->compact(tier1);
    EXPECT_FALSE(r2.outputPath.empty());
    if (r2.outputPath.empty()) {
        co_return;
    }

    auto points = co_await TSMCompactionStringDictTest::readAll(r2.outputPath, sid);
    EXPECT_EQ(points.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(points[1000 + i * 1000], i % 3 == 0 ? "red" : (i % 3 == 1 ? "green" : "blue"));
    }

    co_return;
}
