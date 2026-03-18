// Regression test: opening a TSM file smaller than 13 bytes must throw
// instead of underflowing length - sizeof(uint64_t).

#include "../../../lib/storage/tsm.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>

class TSMSmallFileTest : public ::testing::Test {
protected:
    std::string testDir = "./test_tsm_small";
    void SetUp() override { std::filesystem::create_directories(testDir); }
    void TearDown() override { std::filesystem::remove_all(testDir); }
};

static seastar::future<> testTooSmallFile(std::string path) {
    // Create a 4-byte file — well below the 13-byte minimum
    { std::ofstream(path, std::ios::binary) << "TASM"; }

    TSM tsm(path);
    bool threw = false;
    try {
        co_await tsm.open();
    } catch (const std::runtime_error& e) {
        threw = true;
        EXPECT_NE(std::string(e.what()).find("too small"), std::string::npos)
            << "Error message should mention file too small, got: " << e.what();
    }
    EXPECT_TRUE(threw) << "Opening a <13 byte TSM file must throw";
}

TEST_F(TSMSmallFileTest, TooSmallFileThrowsInsteadOfUnderflow) {
    testTooSmallFile(testDir + "/0_1.tsm").get();
}

static seastar::future<> testEmptyFile(std::string path) {
    { std::ofstream(path, std::ios::binary); }  // 0 bytes

    TSM tsm(path);
    bool threw = false;
    try {
        co_await tsm.open();
    } catch (const std::runtime_error&) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "Opening a 0-byte TSM file must throw";
}

TEST_F(TSMSmallFileTest, EmptyFileThrows) {
    testEmptyFile(testDir + "/0_2.tsm").get();
}
