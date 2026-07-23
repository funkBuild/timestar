/*
 * Integration test for the cluster storage-layout foundation
 * (docs/clustering-starting-point.md).
 *
 * Proves the system property that path injection is supposed to guarantee: a
 * non-default data root holds ALL persistent state, a restart from that root
 * recovers the original series (both TSM-persisted and WAL-recovered data), and
 * nothing leaks into the process working directory.
 */

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/query_result.hpp"
#include "../../../lib/storage/storage_layout.hpp"
#include "../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <set>
#include <string>
#include <variant>

namespace fs = std::filesystem;

class DataDirRestartTest : public ::testing::Test {
public:
    fs::path dataRoot;

    void SetUp() override {
        static std::atomic_uint64_t sequence{0};
        dataRoot = fs::temp_directory_path() / (std::string("timestar_datadir_restart_") + std::to_string(::getpid()) +
                                                "_" + std::to_string(sequence.fetch_add(1)));
        // Deliberately do NOT create it: startup must create its own root.
        fs::remove_all(dataRoot);
    }

    void TearDown() override { fs::remove_all(dataRoot); }

    static std::set<std::string> shardEntriesIn(const fs::path& dir) {
        std::set<std::string> names;
        std::error_code ec;
        for (const auto& entry : fs::directory_iterator(dir, ec)) {
            const auto name = entry.path().filename().string();
            if (name.rfind("shard_", 0) == 0)
                names.insert(name);
        }
        return names;
    }
};

SEASTAR_TEST_F(DataDirRestartTest, IngestRestartQueryUnderNonDefaultRoot) {
    const timestar::StorageLayout layout(self->dataRoot.string());
    const int64_t baseTime = 1638360000000000000LL;  // Dec 1, 2021

    // Snapshot shard_* directories in the CWD so we can prove none leak there.
    const auto cwdShardsBefore = DataDirRestartTest::shardEntriesIn(fs::current_path());

    // ---- Phase 1: ingest; roll one batch to TSM; leave a batch in the WAL ----
    {
        Engine engine(layout);
        co_await engine.init();
        co_await engine.startBackgroundTasks();

        TimeStarInsert<double> first("weather", "temperature");
        first.addTag("location", "us-west");
        for (int i = 0; i < 5; ++i)
            first.addValue(baseTime + i * 1000000000LL, 20.0 + i);
        co_await engine.insert(first);

        // Force the first batch out to a TSM file under the data root.
        co_await engine.rolloverMemoryStore();
        co_await seastar::sleep(std::chrono::milliseconds(300));

        // The second batch stays in the active WAL, exercising WAL recovery on
        // the next startup rather than TSM open.
        TimeStarInsert<double> second("weather", "temperature");
        second.addTag("location", "us-west");
        for (int i = 5; i < 10; ++i)
            second.addValue(baseTime + i * 1000000000LL, 20.0 + i);
        co_await engine.insert(second);

        co_await engine.stop();
    }

    // Every persistent artifact must live under the configured root...
    EXPECT_TRUE(fs::exists(layout.shardDir(0))) << "shard dir missing under data root: " << layout.shardDir(0).string();
    EXPECT_TRUE(fs::exists(layout.tsmDir(0))) << "TSM dir missing under data root";
    EXPECT_TRUE(fs::exists(layout.nativeIndexDir(0))) << "native index missing under data root";
    // ...and nothing may leak into the working directory.
    EXPECT_EQ(DataDirRestartTest::shardEntriesIn(fs::current_path()), cwdShardsBefore)
        << "shard artifacts leaked into the working directory when a non-default data_dir was configured";

    // ---- Phase 2: restart from the same root and recover the series ----
    {
        Engine engine(layout);
        co_await engine.init();  // WAL recovery + open existing TSM files

        TimeStarInsert<double> probe("weather", "temperature");
        probe.addTag("location", "us-west");
        const std::string seriesKey = probe.seriesKey();

        auto result = co_await engine.query(seriesKey, baseTime, baseTime + 20LL * 1000000000LL);
        EXPECT_TRUE(result.has_value()) << "original series not discoverable after restart";
        if (result.has_value() && std::holds_alternative<QueryResult<double>>(*result)) {
            const auto& floats = std::get<QueryResult<double>>(*result);
            EXPECT_EQ(floats.timestamps.size(), 10u)
                << "expected all 10 points after restart (TSM batch + WAL-recovered batch)";
            EXPECT_EQ(floats.values.size(), 10u);
        } else {
            ADD_FAILURE() << "expected a float query result after restart";
        }

        co_await engine.stop();
    }

    co_return;
}
