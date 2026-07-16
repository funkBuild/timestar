// Regression tests for [server] data_dir wiring.
//
// Bug: timestar.toml's data_dir key was parsed into ServerConfig but every
// storage component (Engine, TSMFileManager, TSMCompactor, WAL, NativeIndex)
// hardcoded CWD-relative "shard_N/..." paths, so the key was silently ignored.
//
// Covers:
//   1. dataRootPath()/shardDataPath() derivation: default ".", empty value,
//      absolute and relative roots, trailing-slash normalization.
//   2. End-to-end: an Engine running with a custom data_dir creates shard_0
//      (tsm/, WAL, native_index/) under that directory and NOT under the CWD.

#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/thread.hh>
#include <string>
#include <unistd.h>

namespace fs = std::filesystem;

namespace {

// RAII: install a global config with the given data_dir, restore the default
// config on scope exit so later tests see the legacy CWD-relative layout.
class ScopedDataDir {
public:
    explicit ScopedDataDir(const std::string& dataDir) {
        timestar::TimestarConfig cfg{};
        cfg.server.data_dir = dataDir;
        timestar::setGlobalConfig(cfg);
    }
    ~ScopedDataDir() { timestar::setGlobalConfig(timestar::TimestarConfig{}); }
    ScopedDataDir(const ScopedDataDir&) = delete;
    ScopedDataDir& operator=(const ScopedDataDir&) = delete;
};

}  // namespace

class DataDirTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// ---------------------------------------------------------------------------
// Path derivation
// ---------------------------------------------------------------------------

TEST_F(DataDirTest, ServerConfigDefaultsToDot) {
    timestar::ServerConfig cfg{};
    EXPECT_EQ(cfg.data_dir, ".");
}

TEST_F(DataDirTest, DefaultDataDirKeepsLegacyCwdRelativePaths) {
    ScopedDataDir guard(".");
    EXPECT_EQ(timestar::dataRootPath(), ".");
    // Legacy behavior: no "./" prefix, exactly the strings used before the fix.
    EXPECT_EQ(timestar::shardDataPath(0), "shard_0");
    EXPECT_EQ(timestar::shardDataPath(17), "shard_17");
}

TEST_F(DataDirTest, EmptyDataDirNormalizesToCwd) {
    ScopedDataDir guard("");
    EXPECT_EQ(timestar::dataRootPath(), ".");
    EXPECT_EQ(timestar::shardDataPath(3), "shard_3");
}

TEST_F(DataDirTest, DotWithTrailingSlashBehavesLikeDefault) {
    ScopedDataDir guard("./");
    EXPECT_EQ(timestar::dataRootPath(), ".");
    EXPECT_EQ(timestar::shardDataPath(0), "shard_0");
}

TEST_F(DataDirTest, AbsoluteDataDir) {
    ScopedDataDir guard("/var/lib/timestar");
    EXPECT_EQ(timestar::dataRootPath(), "/var/lib/timestar");
    EXPECT_EQ(timestar::shardDataPath(2), "/var/lib/timestar/shard_2");
}

TEST_F(DataDirTest, TrailingSlashesAreStripped) {
    ScopedDataDir guard("/var/lib/timestar///");
    EXPECT_EQ(timestar::dataRootPath(), "/var/lib/timestar");
    EXPECT_EQ(timestar::shardDataPath(0), "/var/lib/timestar/shard_0");
}

TEST_F(DataDirTest, RelativeDataDir) {
    ScopedDataDir guard("data/ts");
    EXPECT_EQ(timestar::dataRootPath(), "data/ts");
    EXPECT_EQ(timestar::shardDataPath(1), "data/ts/shard_1");
}

TEST_F(DataDirTest, FilesystemRootIsPreserved) {
    ScopedDataDir guard("/");
    EXPECT_EQ(timestar::dataRootPath(), "/");
    EXPECT_EQ(timestar::shardDataPath(0), "/shard_0");
}

// ---------------------------------------------------------------------------
// End-to-end: Engine writes all shard data under the configured data_dir
// ---------------------------------------------------------------------------

TEST_F(DataDirTest, EngineCreatesShardDataUnderConfiguredDataDir) {
    seastar::thread([] {
        const fs::path tmpRoot = fs::temp_directory_path() / ("timestar_data_dir_test_" + std::to_string(::getpid()));
        fs::remove_all(tmpRoot);

        {
            // Trailing slash on purpose: exercises normalization end-to-end.
            // The directory does not exist yet: the engine must create it.
            ScopedDataDir guard(tmpRoot.string() + "/");

            ScopedEngine eng;
            eng.init();

            TimeStarInsert<double> insert("datadir_metric", "value");
            insert.addValue(1000, 1.5);
            insert.addValue(2000, 2.5);
            eng->insert(std::move(insert)).get();

            const fs::path shardDir = tmpRoot / "shard_0";
            EXPECT_TRUE(fs::exists(shardDir)) << "missing " << shardDir;
            EXPECT_TRUE(fs::exists(shardDir / "tsm")) << "missing " << (shardDir / "tsm");
            EXPECT_TRUE(fs::exists(shardDir / "native_index")) << "missing " << (shardDir / "native_index");

            // The WAL for the insert must land in the shard dir under data_dir.
            bool foundWal = false;
            if (fs::exists(shardDir)) {
                for (const auto& entry : fs::directory_iterator(shardDir)) {
                    if (entry.path().extension() == ".wal") {
                        foundWal = true;
                        break;
                    }
                }
            }
            EXPECT_TRUE(foundWal) << "no .wal file under " << shardDir;

            // Nothing may leak into the CWD (the pre-fix behavior).
            EXPECT_FALSE(fs::exists("shard_0")) << "shard_0 leaked into the CWD despite data_dir";

            // ScopedEngine stops the engine here, while the custom data_dir
            // config is still installed (mirrors real shutdown ordering).
        }

        fs::remove_all(tmpRoot);
    })
        .join()
        .get();
}
