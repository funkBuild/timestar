// Engine concurrent stop+insert tests.
//
// Verifies that stopping the engine while inserts are in flight does not
// cause crashes, data corruption, or use-after-free. The _insertGate must
// drain all in-flight inserts before WAL/index teardown begins.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

class EngineConcurrentStopTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// Start an insert and stop() concurrently. Both must complete without crash.
// The insert either succeeds or throws gate_closed_exception.
TEST_F(EngineConcurrentStopTest, InsertAndStopConcurrentlyNoCrash) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Fire insert without waiting
        TimeStarInsert<double> insert("concurrent_metric", "value");
        insert.addValue(1000, 42.0);
        auto insertFut = eng->insert(std::move(insert));

        // Immediately request stop
        auto stopFut = eng->stop();

        // Both must resolve — insert may succeed or throw gate_closed
        try {
            insertFut.get();
        } catch (const seastar::gate_closed_exception&) {
            // Expected if stop() closed the gate before insert acquired it
        }
        stopFut.get();

        eng.engine.reset();
    })
        .join()
        .get();
}

// Multiple in-flight inserts before stop. stop() must wait for all to drain.
TEST_F(EngineConcurrentStopTest, MultipleInFlightInsertsBeforeStop) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Launch several inserts without waiting
        std::vector<seastar::future<>> futures;
        for (int i = 0; i < 10; ++i) {
            TimeStarInsert<double> insert("batch_metric", "value");
            insert.addValue(static_cast<uint64_t>(i) * 1000 + 1000, static_cast<double>(i));
            futures.push_back(eng->insert(std::move(insert)));
        }

        // Stop while inserts may still be in flight
        auto stopFut = eng->stop();

        // Collect all insert results — each either succeeds or throws gate_closed
        for (auto& f : futures) {
            try {
                f.get();
            } catch (const seastar::gate_closed_exception&) {
                // Acceptable
            }
        }
        stopFut.get();

        eng.engine.reset();
    })
        .join()
        .get();
}

// After stop completes, the insert gate count must be zero.
TEST_F(EngineConcurrentStopTest, InsertAfterStopThrowsImmediately) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Insert successfully first
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(1000, 10.0);
            eng->insert(std::move(insert)).get();
        }

        eng->stop().get();

        // Any insert after stop must throw immediately
        {
            TimeStarInsert<double> insert("metric", "value");
            insert.addValue(2000, 20.0);
            EXPECT_THROW(eng->insert(std::move(insert)).get(), seastar::gate_closed_exception);
        }

        eng.engine.reset();
    })
        .join()
        .get();
}

// Batch insert after stop also throws.
TEST_F(EngineConcurrentStopTest, BatchInsertAfterStopThrows) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        eng->stop().get();

        std::vector<TimeStarInsert<double>> batch;
        TimeStarInsert<double> insert("metric", "value");
        insert.addValue(1000, 10.0);
        batch.push_back(std::move(insert));

        EXPECT_THROW(eng->insertBatch(std::move(batch)).get(), seastar::gate_closed_exception);

        eng.engine.reset();
    })
        .join()
        .get();
}

// Double stop is safe (idempotent).
TEST_F(EngineConcurrentStopTest, DoubleStopIsSafe) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        eng->stop().get();
        // Second stop should not crash or throw
        EXPECT_NO_THROW(eng->stop().get());

        eng.engine.reset();
    })
        .join()
        .get();
}
