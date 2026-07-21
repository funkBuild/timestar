// Regression tests for compaction scheduling placement and non-blocking ingest.
//
// Background: TimeStar declared a `ts_compact` scheduling group at 10 shares
// specifically so foreground writes could preempt background compaction. In
// production it was never used. Engine::init() forwarded the group to
// TSMFileManager only `if (_schedulingGroupsCreated)`, but the server calls
// init() BEFORE create_scheduling_group(), so the flag was always false at that
// point and setIOSchedulingGroups() had no path back to TSMFileManager. Every
// with_scheduling_group() site silently took its inline fallback and all
// compaction ran in `main` alongside writes -- a 76s tier-3 merge was 76s of
// unavailability, while ts_compact reported zero runtime.
//
// Nothing could observe this: no test asserted placement after startup, and the
// only symptom was a metric nobody was looking at. These tests pin the wiring
// itself, in the order the server actually uses.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/storage/tsm_file_manager.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <sstream>
#include <string>

namespace fs = std::filesystem;

class CompactionSchedulingPlacementTest : public ::testing::Test {
protected:
    std::string shardDir = "./shard_0";

    void SetUp() override {
        fs::remove_all(shardDir);
        fs::create_directories(shardDir);
    }
    void TearDown() override { fs::remove_all(shardDir); }
};

// ---------------------------------------------------------------------------
// The reported bug, pinned directly: init() BEFORE the groups are created.
// This is the server's real order (bin/timestar_http_server.cpp). Under the old
// code hasCompactionGroup() was false here for the whole process lifetime.
// ---------------------------------------------------------------------------
seastar::future<> testGroupsWiredWhenCreatedAfterInit() {
    Engine engine;
    co_await engine.init();

    auto compactGrp = co_await seastar::create_scheduling_group("test_compact", 10);
    auto flushGrp = co_await seastar::create_scheduling_group("test_flush", 200);
    auto queryGrp = co_await seastar::create_scheduling_group("test_query", 100);
    auto writeGrp = co_await seastar::create_scheduling_group("test_write", 50);

    engine.setIOSchedulingGroups(queryGrp, writeGrp, compactGrp, flushGrp);

    const auto& fm = engine.getTSMFileManager();
    EXPECT_TRUE(fm.hasCompactionGroup())
        << "Compaction scheduling group was not forwarded to TSMFileManager. Compaction "
           "will run in `main` and block foreground writes for the length of a tier merge.";
    EXPECT_TRUE(fm.hasFlushGroup()) << "Flush scheduling group was not forwarded to TSMFileManager. "
                                       "WAL->TSM conversion will not be prioritised over tier merges.";
    EXPECT_EQ(fm.compactionGroup(), compactGrp);
    EXPECT_EQ(fm.flushGroup(), flushGrp);

    // Placement is only meaningful if the two are actually distinct: conversion
    // must be able to outrank merges rather than queue behind them.
    EXPECT_NE(fm.compactionGroup(), fm.flushGroup())
        << "Conversion and compaction share a scheduling group, so a deep tier merge can "
           "starve the WAL drain that frees disk and keeps ingest flowing.";

    co_await engine.stop();
}

TEST_F(CompactionSchedulingPlacementTest, GroupsWiredWhenCreatedAfterInit) {
    testGroupsWiredWhenCreatedAfterInit().get();
}

// ---------------------------------------------------------------------------
// The reverse order must work too -- embedders and tests may create the groups
// first. The init()-side guard covers this direction.
// ---------------------------------------------------------------------------
seastar::future<> testGroupsWiredWhenCreatedBeforeInit() {
    auto compactGrp = co_await seastar::create_scheduling_group("test_compact2", 10);
    auto flushGrp = co_await seastar::create_scheduling_group("test_flush2", 200);
    auto queryGrp = co_await seastar::create_scheduling_group("test_query2", 100);
    auto writeGrp = co_await seastar::create_scheduling_group("test_write2", 50);

    Engine engine;
    engine.setIOSchedulingGroups(queryGrp, writeGrp, compactGrp, flushGrp);
    co_await engine.init();

    const auto& fm = engine.getTSMFileManager();
    EXPECT_TRUE(fm.hasCompactionGroup());
    EXPECT_TRUE(fm.hasFlushGroup());
    EXPECT_EQ(fm.compactionGroup(), compactGrp);
    EXPECT_EQ(fm.flushGroup(), flushGrp);

    co_await engine.stop();
}

TEST_F(CompactionSchedulingPlacementTest, GroupsWiredWhenCreatedBeforeInit) {
    testGroupsWiredWhenCreatedBeforeInit().get();
}

// ---------------------------------------------------------------------------
// Without groups, the background loop must refuse to start rather than run
// merges in `main`. Starting it unguarded would reintroduce the exact
// regression: compaction at 1000 shares next to foreground writes.
// ---------------------------------------------------------------------------
seastar::future<> testBackgroundCompactionRefusesWithoutGroups() {
    Engine engine;
    co_await engine.init();

    // No setIOSchedulingGroups() call.
    co_await engine.startBackgroundCompaction();

    EXPECT_FALSE(engine.getTSMFileManager().hasCompactionGroup())
        << "Background compaction started without a scheduling group, so merges would run "
           "in `main` at 1000 shares and preempt foreground writes.";

    co_await engine.stop();
}

TEST_F(CompactionSchedulingPlacementTest, BackgroundCompactionRefusesWithoutGroups) {
    testBackgroundCompactionRefusesWithoutGroups().get();
}

// ---------------------------------------------------------------------------
// Source-inspection: writeMemstore() must NOT await compaction.
//
// This is the coupling that caused the outage. writeMemstore() runs on the WAL
// conversion fiber holding _conversionSemaphore (capacity 1), and its caller
// only unlinks the WAL file once it returns. Awaiting a tier merge here means a
// deep merge holds the shard's single conversion slot for its full duration:
// WAL disk is not reclaimed, the memory-store backlog grows, and rollover --
// and therefore ingest -- stalls behind it.
//
// Checked at the source level because the failure is structural (who awaits
// whom), not observable from writeMemstore()'s return value.
// ---------------------------------------------------------------------------
class WriteMemstoreCouplingTest : public ::testing::Test {
protected:
    std::string source;

    void SetUp() override {
        std::ifstream f(TSM_FILE_MANAGER_CPP_SOURCE_PATH);
        ASSERT_TRUE(f.is_open()) << "Could not open tsm_file_manager.cpp at: " << TSM_FILE_MANAGER_CPP_SOURCE_PATH;
        source.assign(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
        ASSERT_FALSE(source.empty());
    }

    // Extract a method body by signature, tracking brace depth.
    std::string extractMethodBody(const std::string& signature) {
        auto pos = source.find(signature);
        if (pos == std::string::npos)
            return "";
        auto bracePos = source.find('{', pos);
        if (bracePos == std::string::npos)
            return "";
        int depth = 1;
        size_t i = bracePos + 1;
        while (i < source.size() && depth > 0) {
            if (source[i] == '{')
                depth++;
            else if (source[i] == '}')
                depth--;
            i++;
        }
        return source.substr(pos, i - pos);
    }

    // Strip // and /* */ comments so prose about compaction is not mistaken
    // for a call to it.
    static std::string stripComments(const std::string& in) {
        std::string out;
        out.reserve(in.size());
        for (size_t i = 0; i < in.size();) {
            if (in[i] == '/' && i + 1 < in.size() && in[i + 1] == '/') {
                while (i < in.size() && in[i] != '\n')
                    ++i;
            } else if (in[i] == '/' && i + 1 < in.size() && in[i + 1] == '*') {
                i += 2;
                while (i + 1 < in.size() && !(in[i] == '*' && in[i + 1] == '/'))
                    ++i;
                i = std::min(i + 2, in.size());
            } else {
                out += in[i++];
            }
        }
        return out;
    }
};

TEST_F(WriteMemstoreCouplingTest, WriteMemstoreDoesNotTriggerCompactionInline) {
    std::string body = extractMethodBody("TSMFileManager::writeMemstore(");
    ASSERT_FALSE(body.empty()) << "Could not find TSMFileManager::writeMemstore() in source";

    std::string code = stripComments(body);
    EXPECT_EQ(code.find("checkAndTriggerCompaction"), std::string::npos)
        << "writeMemstore() triggers compaction inline. It runs on the WAL conversion fiber "
           "holding _conversionSemaphore (capacity 1), and the WAL file is only unlinked after "
           "it returns -- so a deep tier merge here blocks WAL reclamation and stalls ingest. "
           "Tier merges belong on the background loop in the compaction scheduling group.";
    EXPECT_EQ(code.find("compactOneTier"), std::string::npos) << "writeMemstore() compacts a tier inline; see above.";
}

// ---------------------------------------------------------------------------
// Source-inspection: rollover must not block on the conversion backlog.
//
// The old code spun in rolloverMemoryStore() until the backlog drained, while
// holding compactionSemaphore (capacity 1) -- so one slow conversion queued
// every other rollover on the shard and surfaced as client write timeouts.
// Policy is now to absorb the burst and shed at the request edge instead.
// ---------------------------------------------------------------------------
TEST_F(WriteMemstoreCouplingTest, RolloverDoesNotBlockOnConversionBacklog) {
    // rolloverMemoryStore lives in wal_file_manager.cpp; load it separately.
    std::ifstream f(WAL_FILE_MANAGER_CPP_SOURCE_PATH);
    ASSERT_TRUE(f.is_open()) << "Could not open wal_file_manager.cpp at: " << WAL_FILE_MANAGER_CPP_SOURCE_PATH;
    std::string walSource((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    ASSERT_FALSE(walSource.empty());

    auto pos = walSource.find("WALFileManager::rolloverMemoryStore()");
    ASSERT_NE(pos, std::string::npos) << "Could not find WALFileManager::rolloverMemoryStore()";
    auto bracePos = walSource.find('{', pos);
    int depth = 1;
    size_t i = bracePos + 1;
    while (i < walSource.size() && depth > 0) {
        if (walSource[i] == '{')
            depth++;
        else if (walSource[i] == '}')
            depth--;
        i++;
    }
    std::string code = stripComments(walSource.substr(pos, i - pos));

    // Match `co_await seastar::sleep` specifically, not any sleep. The method
    // also spawns a DETACHED retry fiber that legitimately sleeps 30s before
    // re-attempting a failed conversion; that does not block rollover. Only an
    // awaited sleep suspends the rollover coroutine itself.
    EXPECT_EQ(code.find("co_await seastar::sleep"), std::string::npos)
        << "rolloverMemoryStore() awaits a sleep, i.e. it blocks waiting for the conversion "
           "backlog to drain. It holds compactionSemaphore (capacity 1) while doing so, which "
           "queues every other rollover on the shard behind it and surfaces to clients as write "
           "timeouts. Bursts must be absorbed; load is shed at the request edge with 503.";
    EXPECT_EQ(code.find("while (memoryStores.size() >="), std::string::npos)
        << "rolloverMemoryStore() spins on the backlog; see above.";
    EXPECT_EQ(code.find("co_await seastar::get_units(_conversionSemaphore"), std::string::npos)
        << "rolloverMemoryStore() awaits the conversion semaphore, coupling rollover latency to "
           "conversion progress. Rollover must complete regardless of conversion backlog.";
}
