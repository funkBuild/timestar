#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/index/native/native_index.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <seastar/core/coroutine.hh>
#include <set>
#include <string>
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
