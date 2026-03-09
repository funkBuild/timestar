/*
 * Minimal test to debug LevelDB index segfault
 */

#include <gtest/gtest.h>
#include "../../seastar_gtest.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/core/series_id.hpp"

class LevelDBIndexMinimalTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_0");
    }

    void TearDown() override {
        std::filesystem::remove_all("shard_0");
    }
};

SEASTAR_TEST_F(LevelDBIndexMinimalTest, OpenAndClose) {
    LevelDBIndex index(0);

    std::cerr << "About to open index..." << std::endl;
    co_await index.open();
    std::cerr << "Index opened successfully" << std::endl;

    co_await index.close();
    std::cerr << "Index closed successfully" << std::endl;

    co_return;
}

SEASTAR_TEST_F(LevelDBIndexMinimalTest, CreateSimpleSeries) {
    LevelDBIndex index(0);

    co_await index.open();
    std::cerr << "Index opened" << std::endl;

    // Try calling getOrCreateSeriesId directly
    std::map<std::string, std::string> emptyTags;
    std::cerr << "About to call getOrCreateSeriesId..." << std::endl;

    SeriesId128 seriesId = co_await index.getOrCreateSeriesId("test", emptyTags, "value");
    std::cerr << "Created series with ID: " << seriesId.toHex() << std::endl;

    EXPECT_FALSE(seriesId.isZero());

    co_await index.close();
    std::cerr << "Index closed" << std::endl;
    co_return;
}

SEASTAR_TEST_F(LevelDBIndexMinimalTest, CreateSeriesWithTags) {
    LevelDBIndex index(0);

    co_await index.open();
    std::cerr << "Index opened" << std::endl;

    // Create TimeStarInsert with tags
    TimeStarInsert<double> insert("test", "value");
    insert.addTag("host", "server1");
    std::cerr << "Created TimeStarInsert with tags" << std::endl;

    SeriesId128 seriesId = co_await index.indexInsert(insert);
    std::cerr << "Created series with ID: " << seriesId.toHex() << std::endl;

    EXPECT_FALSE(seriesId.isZero());

    co_await index.close();
    co_return;
}
