/*
 * Minimal test to debug LevelDB index segfault
 */

#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <filesystem>
#include <functional>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"

using namespace seastar;

// Wrapper to run Seastar code within Google Test
static int run_in_seastar(std::function<seastar::future<>()> func) {
    char prog_name[] = "test";
    char* argv[] = { prog_name, nullptr };
    int argc = 1;
    
    seastar::app_template app;
    try {
        return app.run(argc, argv, [func = std::move(func)]() {
            return func().handle_exception([](std::exception_ptr ep) {
                try {
                    std::rethrow_exception(ep);
                } catch (const std::exception& e) {
                    std::cerr << "Test failed with exception: " << e.what() << std::endl;
                    return make_exception_future<>(ep);
                }
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Failed to run Seastar app: " << e.what() << std::endl;
        return 1;
    }
}

class LevelDBIndexMinimalTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_995");
    }
    
    void TearDown() override {
        std::filesystem::remove_all("shard_995");
    }
};

TEST_F(LevelDBIndexMinimalTest, OpenAndClose) {
    int result = run_in_seastar([]() -> seastar::future<> {
        LevelDBIndex index(995);
        
        std::cerr << "About to open index..." << std::endl;
        co_await index.open();
        std::cerr << "Index opened successfully" << std::endl;
        
        co_await index.close();
        std::cerr << "Index closed successfully" << std::endl;
        
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(LevelDBIndexMinimalTest, CreateSimpleSeries) {
    int result = run_in_seastar([]() -> seastar::future<> {
        LevelDBIndex index(995);
        
        co_await index.open();
        std::cerr << "Index opened" << std::endl;
        
        // Try calling getOrCreateSeriesId directly
        std::map<std::string, std::string> emptyTags;
        std::cerr << "About to call getOrCreateSeriesId..." << std::endl;
        
        uint64_t seriesId = co_await index.getOrCreateSeriesId("test", emptyTags, "value");
        std::cerr << "Created series with ID: " << seriesId << std::endl;
        
        EXPECT_GT(seriesId, 0);
        
        co_await index.close();
        std::cerr << "Index closed" << std::endl;
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}

TEST_F(LevelDBIndexMinimalTest, CreateSeriesWithTags) {
    int result = run_in_seastar([]() -> seastar::future<> {
        LevelDBIndex index(995);
        
        co_await index.open();
        std::cerr << "Index opened" << std::endl;
        
        // Create TSDBInsert with tags
        TSDBInsert<double> insert("test", "value");
        insert.addTag("host", "server1");
        std::cerr << "Created TSDBInsert with tags" << std::endl;
        
        uint64_t seriesId = co_await index.indexInsert(insert);
        std::cerr << "Created series with ID: " << seriesId << std::endl;
        
        EXPECT_GT(seriesId, 0);
        
        co_await index.close();
        co_return;
    });
    
    EXPECT_EQ(result, 0);
}