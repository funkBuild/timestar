#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <seastar/core/coroutine.hh>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

class NativeIndexVShardExtractTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }
    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

seastar::future<> testExtractVShardSeriesMetadata() {
    timestar::index::NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    std::vector<SeriesId128> ids;
    ids.push_back(co_await index.getOrCreateSeriesId("weather", {{"host", "a"}}, "temp"));
    ids.push_back(co_await index.getOrCreateSeriesId("weather", {{"host", "b"}}, "temp"));
    ids.push_back(co_await index.getOrCreateSeriesId("weather", {{"host", "c"}}, "humidity"));
    ids.push_back(co_await index.getOrCreateSeriesId("pressure", {{"loc", "west"}}, "value"));

    // Expected: group the created series by their derived VShard.
    std::map<uint16_t, std::set<SeriesId128>> expected;
    for (const auto& id : ids)
        expected[timestar::virtualShard(id)].insert(id);

    // Each VShard's extraction returns exactly its series (all created series are
    // known, so no foreign series can appear).
    for (const auto& [vs, want] : expected) {
        auto extracted = co_await index.extractVShardSeriesMetadata(vs);
        std::set<SeriesId128> got;
        for (const auto& [id, meta] : extracted) {
            EXPECT_EQ(timestar::virtualShard(id), vs) << "extraction returned a foreign-VShard series";
            got.insert(id);
        }
        EXPECT_EQ(got, want) << "vshard " << vs << " extraction mismatch";
    }

    // A VShard with no series extracts nothing. Find one not in `expected`.
    uint16_t emptyVs = 0;
    while (expected.count(emptyVs))
        ++emptyVs;
    auto none = co_await index.extractVShardSeriesMetadata(emptyVs);
    EXPECT_TRUE(none.empty()) << "an unused VShard must extract no series";
    co_return;
}

TEST_F(NativeIndexVShardExtractTest, ExtractSeriesMetadataByVShard) {
    testExtractVShardSeriesMetadata().get();
}

seastar::future<> testBoundedVShardPatternExpansion() {
    timestar::index::NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    const std::string firstKey = timestar::buildSeriesKey("cpu", {{"host", "h0"}, {"env", "prod"}}, "usage");
    const uint16_t target = timestar::virtualShard(SeriesId128::fromSeriesKey(firstKey));
    std::vector<std::string> sameVShardHosts{"h0"};
    for (unsigned i = 1; sameVShardHosts.size() < 3 && i < 100'000; ++i) {
        const std::string host = "h" + std::to_string(i);
        const std::string field = sameVShardHosts.size() == 2 ? "idle" : "usage";
        const std::string key = timestar::buildSeriesKey("cpu", {{"host", host}, {"env", "prod"}}, field);
        if (timestar::virtualShard(SeriesId128::fromSeriesKey(key)) == target)
            sameVShardHosts.push_back(host);
    }
    EXPECT_EQ(sameVShardHosts.size(), 3u);
    if (sameVShardHosts.size() != 3) {
        co_await index.close();
        co_return;
    }

    co_await index.getOrCreateSeriesId("cpu", {{"host", sameVShardHosts[0]}, {"env", "prod"}}, "usage");
    co_await index.getOrCreateSeriesId("cpu", {{"host", sameVShardHosts[1]}, {"env", "prod"}}, "usage");
    co_await index.getOrCreateSeriesId("cpu", {{"host", sameVShardHosts[2]}, {"env", "prod"}}, "idle");
    co_await index.getOrCreateSeriesId("memory", {{"host", "other"}}, "usage");

    auto tooBroad = co_await index.findVShardSeriesKeys(target, "cpu", {{"env", "prod"}}, {"usage"}, 1, 1024);
    EXPECT_FALSE(tooBroad.has_value()) << "the scan must stop and report that its bound was exceeded";

    auto oneHost = co_await index.findVShardSeriesKeys(target, "cpu", {{"env", "prod"}, {"host", sameVShardHosts[0]}},
                                                       {"usage"}, 10, 1024);
    EXPECT_TRUE(oneHost.has_value());
    if (oneHost.has_value()) {
        EXPECT_EQ(oneHost->size(), 1u);
        if (oneHost->size() == 1)
            EXPECT_EQ((*oneHost)[0], firstKey);
    }

    auto idle = co_await index.findVShardSeriesKeys(target, "cpu", {{"env", "prod"}}, {"idle"}, 10, 1024);
    EXPECT_TRUE(idle.has_value());
    if (idle.has_value()) {
        EXPECT_EQ(idle->size(), 1u);
        if (idle->size() == 1)
            EXPECT_EQ((*idle)[0],
                      timestar::buildSeriesKey("cpu", {{"host", sameVShardHosts[2]}, {"env", "prod"}}, "idle"));
    }

    auto byteBound = co_await index.findVShardSeriesKeys(target, "cpu", {{"env", "prod"}}, {"idle"}, 10, 4);
    EXPECT_FALSE(byteBound.has_value()) << "encoded byte admission must stop before retaining an oversized key set";

    co_await index.close();
}

TEST_F(NativeIndexVShardExtractTest, PatternExpansionIsVShardScopedFilteredAndBounded) {
    testBoundedVShardPatternExpansion().get();
}

seastar::future<> testPagedVShardSeriesWalk() {
    timestar::index::NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();

    const std::string first = timestar::buildSeriesKey("retention_page", {{"host", "h0"}}, "value");
    const uint16_t target = timestar::virtualShard(SeriesId128::fromSeriesKey(first));
    std::set<std::string> expected{first};
    for (unsigned i = 1; expected.size() < 5 && i < 100'000; ++i) {
        const std::string key =
            timestar::buildSeriesKey("retention_page", {{"host", "h" + std::to_string(i)}}, "value");
        if (timestar::virtualShard(SeriesId128::fromSeriesKey(key)) == target)
            expected.insert(key);
    }
    EXPECT_EQ(expected.size(), 5u);
    if (expected.size() != 5) {
        co_await index.close();
        co_return;
    }
    for (const auto& key : expected) {
        const auto split = key.find(",host=");
        const auto field = key.rfind(' ');
        EXPECT_NE(split, std::string::npos);
        EXPECT_NE(field, std::string::npos);
        if (split == std::string::npos || field == std::string::npos) {
            co_await index.close();
            co_return;
        }
        co_await index.getOrCreateSeriesId("retention_page", {{"host", key.substr(split + 6, field - split - 6)}},
                                           "value");
    }

    // A foreign measurement in the same VShard must be scanned past without
    // consuming the page budget or appearing in the result.
    for (unsigned i = 0;; ++i) {
        const std::string host = "x" + std::to_string(i);
        const auto id =
            SeriesId128::fromSeriesKey(timestar::buildSeriesKey("retention_foreign", {{"host", host}}, "value"));
        if (timestar::virtualShard(id) == target) {
            co_await index.getOrCreateSeriesId("retention_foreign", {{"host", host}}, "value");
            break;
        }
    }

    std::set<std::string> actual;
    std::string cursor;
    size_t pages = 0;
    do {
        auto page = co_await index.pageVShardSeriesKeys(target, "retention_page", std::move(cursor), 2);
        EXPECT_LE(page.seriesKeys.size(), 2u);
        actual.insert(page.seriesKeys.begin(), page.seriesKeys.end());
        cursor = std::move(page.resumeAfter);
        ++pages;
    } while (!cursor.empty());
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(pages, 3u);

    co_await index.close();
}

TEST_F(NativeIndexVShardExtractTest, RetentionCatalogWalkIsVShardScopedAndBoundedByPage) {
    testPagedVShardSeriesWalk().get();
}

seastar::future<> testReplaceVShardCatalogRemovesObsoleteDiscoveryState() {
    const std::map<std::string, std::string> keepTags{{"env", "prod"}, {"host", "keep"}};
    const std::string keepKey = timestar::buildSeriesKey("cpu", keepTags, "usage");
    const SeriesId128 keepId = SeriesId128::fromSeriesKey(keepKey);
    const uint16_t target = timestar::virtualShard(keepId);

    std::map<std::string, std::string> dropTags;
    SeriesId128 dropId;
    for (unsigned i = 0; i < 100'000; ++i) {
        dropTags = {{"env", "prod"}, {"host", "drop" + std::to_string(i)}, {"retired", "yes"}};
        dropId = SeriesId128::fromSeriesKey(timestar::buildSeriesKey("cpu", dropTags, "usage"));
        if (timestar::virtualShard(dropId) == target)
            break;
    }
    EXPECT_EQ(timestar::virtualShard(dropId), target);
    if (timestar::virtualShard(dropId) != target)
        co_return;

    std::map<std::string, std::string> foreignTags;
    SeriesId128 foreignId;
    for (unsigned i = 0; i < 100'000; ++i) {
        foreignTags = {{"env", "prod"}, {"host", "foreign" + std::to_string(i)}};
        foreignId = SeriesId128::fromSeriesKey(timestar::buildSeriesKey("cpu", foreignTags, "usage"));
        if (timestar::virtualShard(foreignId) != target)
            break;
    }
    EXPECT_NE(timestar::virtualShard(foreignId), target);
    if (timestar::virtualShard(foreignId) == target)
        co_return;

    auto assertReplacement = [&](timestar::index::NativeIndex& index) -> seastar::future<> {
        auto targetRows = co_await index.extractVShardSeriesMetadata(target);
        EXPECT_EQ(targetRows.size(), 1u);
        if (targetRows.size() == 1)
            EXPECT_EQ(targetRows.front().first, keepId);
        EXPECT_TRUE((co_await index.getSeriesId("cpu", keepTags, "usage")).has_value());
        EXPECT_FALSE((co_await index.getSeriesId("cpu", dropTags, "usage")).has_value());
        EXPECT_FALSE((co_await index.getSeriesValueType(dropId)).has_value());

        auto all = co_await index.findSeries("cpu");
        EXPECT_TRUE(all.has_value());
        if (all.has_value())
            EXPECT_EQ(std::set<SeriesId128>(all->begin(), all->end()), (std::set<SeriesId128>{keepId, foreignId}));

        auto sharedTag = co_await index.findSeries("cpu", {{"env", "prod"}});
        EXPECT_TRUE(sharedTag.has_value());
        if (sharedTag.has_value())
            EXPECT_EQ(std::set<SeriesId128>(sharedTag->begin(), sharedTag->end()),
                      (std::set<SeriesId128>{keepId, foreignId}));

        auto emptiedTag = co_await index.findSeries("cpu", {{"retired", "yes"}});
        EXPECT_TRUE(emptiedTag.has_value());
        if (emptiedTag.has_value())
            EXPECT_TRUE(emptiedTag->empty()) << "an emptied postings row must be deleted durably, not left stale";
        co_return;
    };

    {
        timestar::index::NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        EXPECT_EQ(co_await index.getOrCreateSeriesId(keepId, "cpu", keepTags, "usage"), keepId);
        EXPECT_EQ(co_await index.getOrCreateSeriesId(dropId, "cpu", dropTags, "usage"), dropId);
        EXPECT_EQ(co_await index.getOrCreateSeriesId(foreignId, "cpu", foreignTags, "usage"), foreignId);
        co_await index.putSeriesValueType(keepId, TSMValueType::Float);
        co_await index.putSeriesValueType(dropId, TSMValueType::Float);
        co_await index.putSeriesValueType(foreignId, TSMValueType::Float);

        auto before = co_await index.findSeries("cpu", {{"retired", "yes"}});
        EXPECT_TRUE(before.has_value());
        if (before.has_value())
            EXPECT_EQ(*before, (std::vector<SeriesId128>{dropId}));

        auto removed = co_await index.removeVShardSeriesMetadataExcept(target, {keepId});
        EXPECT_EQ(removed, (std::vector<SeriesId128>{dropId}));
        EXPECT_TRUE((co_await index.removeVShardSeriesMetadataExcept(target, {keepId})).empty())
            << "catalog replacement must be idempotent after an interrupted snapshot apply is replayed";
        co_await assertReplacement(index);
        co_await index.close();
    }

    // Reopen from the durable IndexWAL/SSTable view. This distinguishes real
    // replacement from merely evicting the old series from process-local caches.
    {
        timestar::index::NativeIndex reopened(timestar::StorageLayout("."), 0);
        co_await reopened.open();
        co_await assertReplacement(reopened);
        co_await reopened.close();
    }
    co_return;
}

TEST_F(NativeIndexVShardExtractTest, CatalogReplacementRemovesObsoleteDiscoveryStateDurably) {
    testReplaceVShardCatalogRemovesObsoleteDiscoveryState().get();
}
