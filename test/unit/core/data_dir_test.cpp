// Regression tests for [server] data_dir wiring.
//
// Bug: timestar.toml's data_dir key was parsed into ServerConfig but every
// storage component (Engine, TSMFileManager, TSMCompactor, WAL, NativeIndex)
// hardcoded CWD-relative "shard_N/..." paths, so the key was silently ignored.
//
// Covers:
//   1. dataRootPath()/shardDataPath() derivation: default ".", empty value,
//      absolute and relative roots, trailing-slash normalization.
//   2. End-to-end: an Engine with an injected StorageLayout creates shard_0
//      (tsm/, WAL, native_index/) there and ignores later global path changes.

#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <seastar/core/thread.hh>
#include <string>
#include <type_traits>

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
    static_assert(!std::is_default_constructible_v<Engine>);
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
// End-to-end: Engine writes all shard data under its injected layout and does
// not re-read global path configuration after construction.
// ---------------------------------------------------------------------------

TEST_F(DataDirTest, EngineUsesInjectedDataDirDespiteGlobalConfigChanges) {
    seastar::thread([] {
        const fs::path tmpRoot = fs::temp_directory_path() / ("timestar_data_dir_test_" + std::to_string(::getpid()));
        const fs::path decoyRoot = tmpRoot.string() + "_global_decoy";
        fs::remove_all(tmpRoot);
        fs::remove_all(decoyRoot);
        ScopedDataDir guard(decoyRoot.string());
        std::string seriesKey;

        {
            // Engine storage ownership is fixed by the injected layout. A
            // later global-config change must not redirect any artifact.
            ScopedEngine eng(timestar::StorageLayout(tmpRoot.string() + "/"));
            eng.init();

            TimeStarInsert<double> insert("datadir_metric", "value");
            insert.addValue(1000, 1.5);
            insert.addValue(2000, 2.5);
            seriesKey = insert.seriesKey();
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

            // ScopedEngine stops the engine here while the conflicting global
            // config is still installed.
        }

        {
            ScopedEngine restarted{timestar::StorageLayout(tmpRoot)};
            restarted.init();

            const auto result = restarted->query(seriesKey, 0, 3000).get();
            ASSERT_TRUE(result.has_value());
            ASSERT_TRUE(std::holds_alternative<QueryResult<double>>(*result));
            const auto& points = std::get<QueryResult<double>>(*result);
            EXPECT_EQ(points.timestamps, (std::vector<uint64_t>{1000, 2000}));
            EXPECT_EQ(points.values, (std::vector<double>{1.5, 2.5}));

            const auto measurements = restarted->getAllMeasurements().get();
            EXPECT_NE(std::find(measurements.begin(), measurements.end(), "datadir_metric"), measurements.end());
        }

        EXPECT_FALSE(fs::exists("shard_0")) << "shard_0 leaked into the CWD despite data_dir";
        EXPECT_FALSE(fs::exists(decoyRoot / "shard_0"))
            << "Engine re-read the global data_dir after its layout was injected";

        fs::remove_all(tmpRoot);
        fs::remove_all(decoyRoot);
    })
        .join()
        .get();
}
