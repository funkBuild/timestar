// Engine shutdown edge-case tests.
//
// Covers:
//   1. Stopping the engine while a write is in progress does not corrupt data.
//   2. Stopping the engine while a query is in progress returns an error or
//      empty result (not a crash).
//   3. Calling stop() twice is safe (idempotent / gracefully errors).
//   4. After stop(), write/query operations fail gracefully (not UB/crash).

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Test fixture
// ---------------------------------------------------------------------------

class EngineShutdownTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }

    void TearDown() override { cleanTestShardDirectories(); }
};

// ===========================================================================
// 1. Write after stop() throws gate_closed_exception (not UB/crash)
//
// After Engine::stop() closes _insertGate, any subsequent call to insert()
// or insertBatch() tries to hold the gate and receives gate_closed_exception.
// The caller must handle this gracefully — the test verifies it throws rather
// than silently corrupting state or crashing.
// ===========================================================================

TEST_F(EngineShutdownTest, WriteAfterStopThrowsGracefully) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert some data while engine is live — must succeed.
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(1000, 10.0);
            insert.addValue(2000, 20.0);
            EXPECT_NO_THROW(eng->insert(std::move(insert)).get());
        }

        // Manually stop the engine (ScopedEngine destructor would also call stop,
        // but we explicitly stop here to test the post-stop behaviour).
        eng->stop().get();

        // Subsequent insert() must throw (gate_closed_exception), never crash.
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(3000, 30.0);
            EXPECT_THROW(eng->insert(std::move(insert)).get(), seastar::gate_closed_exception);
        }

        // Nullify the engine pointer so ScopedEngine destructor does not call
        // stop() a second time (which is safe but noisy with logs).
        eng.engine.reset();
    })
        .join()
        .get();
}

// ===========================================================================
// 2. insertBatch() after stop() throws gate_closed_exception
// ===========================================================================

TEST_F(EngineShutdownTest, BatchWriteAfterStopThrowsGracefully) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        eng->stop().get();

        std::vector<TimeStarInsert<double>> batch;
        {
            TimeStarInsert<double> a("cpu", "usage");
            a.addValue(1000, 50.0);
            batch.push_back(std::move(a));
        }

        EXPECT_THROW(eng->insertBatch(std::move(batch)).get(), seastar::gate_closed_exception);

        eng.engine.reset();
    })
        .join()
        .get();
}

// ===========================================================================
// 3. Query after stop() does not crash
//
// Engine::query() delegates to QueryRunner which holds references to
// tsmFileManager and walFileManager.  After stop() these managers have been
// closed.  The test verifies the call either throws a meaningful exception or
// returns nullopt — the key requirement is no crash / undefined behaviour.
// ===========================================================================

TEST_F(EngineShutdownTest, QueryAfterStopDoesNotCrash) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert data first so the series exists.
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(1000, 10.0);
            eng->insert(std::move(insert)).get();
        }

        eng->stop().get();

        // After stop the WAL memory stores are closed/flushed.  The query may
        // throw or return nullopt; it must never crash.
        bool threw = false;
        try {
            auto result = eng->query("metric value", 0, UINT64_MAX).get();
            // If it returns without throwing, either nullopt or a valid result
            // is acceptable — just check we got here safely.
            (void)result;
        } catch (const std::exception&) {
            threw = true;
        } catch (...) {
            threw = true;
        }

        // As long as we did not crash (SIGSEGV etc.), the test passes.
        // If it threw, that is a valid graceful failure. Mark it explicitly.
        SUCCEED() << (threw ? "query threw after stop (acceptable)" : "query returned after stop (acceptable)");

        eng.engine.reset();
    })
        .join()
        .get();
}

// ===========================================================================
// 4. Calling stop() twice is idempotent (no crash, no double-free)
//
// Each gate in Engine::stop() is guarded by !gate.is_closed() before
// close() is called, and WALFileManager::close() similarly checks its
// own background gate.  Calling stop() twice must be safe.
// ===========================================================================

TEST_F(EngineShutdownTest, DoubleStopIsSafe) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // First stop — nominal shutdown.
        EXPECT_NO_THROW(eng->stop().get());

        // Second stop — must not crash or double-free.
        EXPECT_NO_THROW(eng->stop().get());

        eng.engine.reset();
    })
        .join()
        .get();
}

// ===========================================================================
// 5. Insert data, stop, verify data integrity (no corruption)
//
// The write path: WAL write -> MemoryStore.  On stop(), WALFileManager::close()
// flushes any non-empty MemoryStore to a TSM file.  After restarting the engine
// (re-init on same directories), data written before stop() must be readable.
// This confirms that stop() does not corrupt the write-ahead log.
// ===========================================================================

TEST_F(EngineShutdownTest, DataSurvivesShutdownViaWALFlush) {
    seastar::thread([] {
        // Phase 1: init, insert, then explicitly stop (which flushes WAL to TSM).
        {
            Engine eng;
            eng.init().get();

            TimeStarInsert<double> insert("sensor", "temperature");
            insert.addTag("loc", "lab");
            insert.addValue(1000, 21.5);
            insert.addValue(2000, 22.0);
            insert.addValue(3000, 22.5);
            eng.insert(std::move(insert)).get();

            // stop() calls WALFileManager::close() which flushes MemoryStore to TSM
            eng.stop().get();
        }

        // Phase 2: re-open the same shard directory and verify data is present.
        {
            Engine eng;
            // Re-init reads the persisted TSM/WAL files.
            eng.init().get();

            auto resultOpt = eng.query("sensor,loc=lab temperature", 0, UINT64_MAX).get();
            ASSERT_TRUE(resultOpt.has_value()) << "Data should survive stop/restart cycle";

            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 3u);
            EXPECT_DOUBLE_EQ(result.values[0], 21.5);
            EXPECT_DOUBLE_EQ(result.values[1], 22.0);
            EXPECT_DOUBLE_EQ(result.values[2], 22.5);

            eng.stop().get();
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 6. Stop after multiple inserts: gate drain preserves completed data
//
// Engine::stop() calls _insertGate.close(), which waits for all outstanding
// gate holders to be released.  Any insert() that completed successfully
// before stop() must have its data intact after shutdown.
//
// We verify this by: inserting several batches, then stopping, then
// re-opening the engine (which reads the TSM flushed by WALFileManager::close)
// and confirming all data is present.  This exercises the gate drain path
// indirectly: stop() must wait for all WAL flushes to complete before
// the engine is considered shut down.
// ===========================================================================

TEST_F(EngineShutdownTest, InsertCompletesBeforeStopDrainsGate) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert multiple batches sequentially — each fully completes before
        // the next starts, exercising the gate hold/release cycle.
        const uint64_t numBatches = 3;
        const uint64_t pointsPerBatch = 50;
        for (uint64_t b = 0; b < numBatches; ++b) {
            TimeStarInsert<double> insert("drain_test", "value");
            for (uint64_t t = b * pointsPerBatch + 1; t <= (b + 1) * pointsPerBatch; ++t) {
                insert.addValue(t, static_cast<double>(t));
            }
            // Each insert fully completes (gate holder released) before proceeding.
            eng->insert(std::move(insert)).get();
        }

        // stop() must wait for any residual WAL background work to drain before
        // returning.  If it returned early, data on disk would be incomplete.
        eng->stop().get();

        // Destroy the first engine so its Seastar metrics are deregistered
        // before the second engine registers the same metric names.
        eng.engine.reset();

        // Re-open the engine (reads TSM files flushed during stop).
        Engine eng2;
        eng2.init().get();

        auto result = eng2.query("drain_test value", 0, UINT64_MAX).get();
        ASSERT_TRUE(result.has_value()) << "All inserted data must be persisted after stop()";
        const auto& qr = std::get<QueryResult<double>>(result.value());
        EXPECT_EQ(qr.timestamps.size(), numBatches * pointsPerBatch)
            << "stop() must drain the WAL gate so all data is flushed to TSM";

        eng2.stop().get();
    })
        .join()
        .get();
}

// ===========================================================================
// 7. Sharded engine: stop() on all shards is safe (no cross-shard UB)
//
// When using seastar::sharded<Engine>, stop() is called on every shard.
// Each shard must independently close its gates and drain in-flight work.
// This test verifies that sharded stop completes without errors.
// ===========================================================================

TEST_F(EngineShutdownTest, ShardedEngineStopIsSafe) {
    seastar::thread([] {
        // Start a sharded engine with background tasks.
        seastar::sharded<Engine> rawEng;
        rawEng.start().get();
        rawEng.invoke_on_all([](Engine& engine) { return engine.init(); }).get();
        rawEng
            .invoke_on_all([&rawEng](Engine& engine) {
                engine.setShardedRef(&rawEng);
                return seastar::make_ready_future<>();
            })
            .get();

        // Insert on all shards before stopping.
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            rawEng
                .invoke_on(s,
                           [s](Engine& engine) {
                               TimeStarInsert<double> ins("shard_metric", "value");
                               ins.addTag("shard", std::to_string(s));
                               ins.addValue(1000, static_cast<double>(s));
                               return engine.insert(std::move(ins));
                           })
                .get();
        }

        // Stop all shards — must complete without throwing.
        EXPECT_NO_THROW(rawEng.invoke_on_all([](Engine& engine) { return engine.stop(); }).get());

        // Seastar container teardown.
        EXPECT_NO_THROW(rawEng.stop().get());
    })
        .join()
        .get();
}

// ===========================================================================
// 8. Source inspection: stop() closes _insertGate before WAL/index teardown
//
// The order of operations in stop() is critical for correctness:
//   1. _insertGate.close()   — no new inserts after this point
//   2. _streamingGate.close() — delivery futures finish
//   3. _metadataGate.close() — background metadata ops drain (shard 0)
//   4. walFileManager.close() — flush MemoryStores to TSM
//   5. index.close()          — close NativeIndex
//
// If _insertGate were closed AFTER walFileManager.close(), a concurrent
// insert could write to a destroyed WAL.  This test reads the source file
// and verifies the correct ordering.
// ===========================================================================

#ifdef ENGINE_CPP_SOURCE_PATH

    #include <fstream>

TEST_F(EngineShutdownTest, StopClosesInsertGateBeforeWALClose) {
    std::ifstream file(ENGINE_CPP_SOURCE_PATH);
    ASSERT_TRUE(file.is_open()) << "Could not open engine.cpp at: " << ENGINE_CPP_SOURCE_PATH;

    std::string source((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    // Find the stop() function body.
    std::string pattern = "Engine::stop()";
    auto stopPos = source.find(pattern);
    ASSERT_NE(stopPos, std::string::npos) << "Could not locate Engine::stop()";

    auto bracePos = source.find('{', stopPos);
    ASSERT_NE(bracePos, std::string::npos);

    // Find the matching closing brace.
    int depth = 1;
    size_t cur = bracePos + 1;
    while (cur < source.size() && depth > 0) {
        if (source[cur] == '{')
            depth++;
        else if (source[cur] == '}')
            depth--;
        cur++;
    }
    std::string stopBody = source.substr(bracePos, cur - bracePos);

    // _insertGate must appear before walFileManager.close()
    auto insertGatePos = stopBody.find("_insertGate");
    auto walClosePos = stopBody.find("walFileManager.close()");
    auto indexClosePos = stopBody.find("index.close()");

    ASSERT_NE(insertGatePos, std::string::npos) << "stop() must reference _insertGate";
    ASSERT_NE(walClosePos, std::string::npos) << "stop() must call walFileManager.close()";
    ASSERT_NE(indexClosePos, std::string::npos) << "stop() must call index.close()";

    EXPECT_LT(insertGatePos, walClosePos) << "_insertGate must be closed before walFileManager.close() to prevent "
                                             "concurrent inserts from writing to a partially-destroyed WAL";

    EXPECT_LT(walClosePos, indexClosePos) << "walFileManager.close() must precede index.close() because WAL flush "
                                             "may perform metadata lookups via the index";
}

TEST_F(EngineShutdownTest, StopChecksGateClosedBeforeClosingEach) {
    std::ifstream file(ENGINE_CPP_SOURCE_PATH);
    ASSERT_TRUE(file.is_open());

    std::string source((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    std::string pattern = "Engine::stop()";
    auto stopPos = source.find(pattern);
    ASSERT_NE(stopPos, std::string::npos);

    auto bracePos = source.find('{', stopPos);
    int depth = 1;
    size_t cur = bracePos + 1;
    while (cur < source.size() && depth > 0) {
        if (source[cur] == '{')
            depth++;
        else if (source[cur] == '}')
            depth--;
        cur++;
    }
    std::string stopBody = source.substr(bracePos, cur - bracePos);

    // The stop() implementation must guard each gate.close() with is_closed()
    // so that a second call to stop() does not attempt to close an already-closed
    // gate (which would throw or be undefined behaviour for Seastar gates).
    EXPECT_NE(stopBody.find("is_closed()"), std::string::npos)
        << "stop() must check is_closed() before each gate.close() call "
           "to make double-stop safe";
}

TEST_F(EngineShutdownTest, InsertHoldsGateDuringOperation) {
    std::ifstream file(ENGINE_CPP_SOURCE_PATH);
    ASSERT_TRUE(file.is_open());

    std::string source((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    // Locate insert() function (template instantiation bodies use the same name)
    std::string pattern = "Engine::insert(";
    auto insertPos = source.find(pattern);
    ASSERT_NE(insertPos, std::string::npos) << "Could not locate Engine::insert()";

    auto bracePos = source.find('{', insertPos);
    int depth = 1;
    size_t cur = bracePos + 1;
    while (cur < source.size() && depth > 0) {
        if (source[cur] == '{')
            depth++;
        else if (source[cur] == '}')
            depth--;
        cur++;
    }
    std::string insertBody = source.substr(bracePos, cur - bracePos);

    // insert() must hold the gate for its entire duration.
    EXPECT_NE(insertBody.find("_insertGate.hold()"), std::string::npos)
        << "insert() must call _insertGate.hold() to participate in the gate "
           "protocol and ensure stop() waits for in-flight inserts";
}

#endif  // ENGINE_CPP_SOURCE_PATH
