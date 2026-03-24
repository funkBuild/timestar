#include "../../lib/config/timestar_config.hpp"
#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>

namespace {

class SeastarSectionParseTest : public ::testing::Test {
protected:
    std::string tmpPath;

    void SetUp() override {
        tmpPath = "/tmp/timestar_test_config_" + std::to_string(::getpid()) + ".toml";
    }

    void TearDown() override {
        std::filesystem::remove(tmpPath);
    }

    timestar::TimestarConfig loadFromContent(const std::string& content) {
        std::ofstream ofs(tmpPath);
        ofs << content;
        ofs.close();
        return timestar::loadConfigFile(tmpPath);
    }
};

TEST_F(SeastarSectionParseTest, BasicKeyValue) {
    auto cfg = loadFromContent("[seastar]\nsmp = 4\n");
    EXPECT_TRUE(cfg.seastar.has("smp"));
    EXPECT_EQ(cfg.seastar.get("smp"), "4");
}

TEST_F(SeastarSectionParseTest, InlineComment) {
    auto cfg = loadFromContent("[seastar]\nsmp = 4 # use 4 cores\n");
    EXPECT_TRUE(cfg.seastar.has("smp"));
    EXPECT_EQ(cfg.seastar.get("smp"), "4");
}

TEST_F(SeastarSectionParseTest, QuotedValueWithHash) {
    auto cfg = loadFromContent("[seastar]\nreactor_backend = \"linux-aio\" # default\n");
    EXPECT_TRUE(cfg.seastar.has("reactor_backend"));
    EXPECT_EQ(cfg.seastar.get("reactor_backend"), "linux-aio");
}

TEST_F(SeastarSectionParseTest, EmptySection) {
    auto cfg = loadFromContent("[seastar]\n");
    EXPECT_TRUE(cfg.seastar.settings.empty());
}

TEST_F(SeastarSectionParseTest, MultipleKeys) {
    auto cfg = loadFromContent("[seastar]\nsmp = 2\ntask_quota_ms = 5\n");
    EXPECT_EQ(cfg.seastar.get("smp"), "2");
    EXPECT_EQ(cfg.seastar.get("task_quota_ms"), "5");
}

TEST_F(SeastarSectionParseTest, NoSeastarSection) {
    auto cfg = loadFromContent("# no seastar section here\n");
    EXPECT_TRUE(cfg.seastar.settings.empty());
}

TEST_F(SeastarSectionParseTest, CommentLineSkipped) {
    auto cfg = loadFromContent("[seastar]\n# smp = 8\nsmp = 2\n");
    EXPECT_EQ(cfg.seastar.get("smp"), "2");
}

TEST_F(SeastarSectionParseTest, WhitespaceAroundEquals) {
    auto cfg = loadFromContent("[seastar]\nsmp  =  4\n");
    EXPECT_TRUE(cfg.seastar.has("smp"));
    EXPECT_EQ(cfg.seastar.get("smp"), "4");
}

TEST_F(SeastarSectionParseTest, CarriageReturnOnlyValueTreatedAsEmpty) {
    // A value consisting only of \r\n should be trimmed to empty and not stored.
    // This tests the trimWs fix for the e < b unsigned underflow edge case.
    auto cfg = loadFromContent("[seastar]\nkey = \r\n");
    EXPECT_FALSE(cfg.seastar.has("key"))
        << "A value of only \\r\\n should be trimmed to empty and not stored";
}

TEST_F(SeastarSectionParseTest, ValueWithTrailingCRLF) {
    // Normal value with trailing \r\n should work
    auto cfg = loadFromContent("[seastar]\nsmp = 4\r\n");
    EXPECT_TRUE(cfg.seastar.has("smp"));
    EXPECT_EQ(cfg.seastar.get("smp"), "4");
}

}  // namespace
