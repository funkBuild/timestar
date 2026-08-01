#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/index/native/native_index.hpp"

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
