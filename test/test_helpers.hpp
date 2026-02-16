#pragma once

#include <filesystem>
#include <string>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include "engine.hpp"
#include "series_id.hpp"

namespace fs = std::filesystem;

/**
 * Insert data into the correct shard of a sharded engine, mimicking HTTP handler routing.
 * Routes the insert to the shard determined by the series key hash, and indexes
 * metadata on shard 0. This ensures data is queryable through the standard query path.
 */
template <class T>
inline void shardedInsert(seastar::sharded<Engine>& eng, TSDBInsert<T> insert) {
    // Calculate target shard from series key hash (same as HTTP write handler)
    std::string seriesKey = insert.seriesKey();
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    unsigned shard = SeriesId128::Hash{}(seriesId) % seastar::smp::count;

    // Index metadata on shard 0 first
    eng.invoke_on(0, [insert](Engine& engine) mutable {
        return engine.indexMetadata(insert);
    }).get();

    // Insert data on the target shard
    eng.invoke_on(shard, [insert = std::move(insert)](Engine& engine) mutable {
        return engine.insert(std::move(insert));
    }).get();
}

/**
 * Clean up all shard directories created by tests.
 * Call from SetUp/TearDown to ensure test isolation.
 */
inline void cleanTestShardDirectories(int maxShards = 64) {
    for (int i = 0; i < maxShards; ++i) {
        std::string shardPath = "shard_" + std::to_string(i);
        if (fs::exists(shardPath)) {
            fs::remove_all(shardPath);
        }
    }
}

/**
 * RAII wrapper for Engine that ensures proper cleanup even on test failure.
 * Use in seastar::thread or seastar::async contexts.
 *
 * Usage:
 *   seastar::thread([this] {
 *       ScopedEngine eng;
 *       eng.init();
 *       // ... test code ...
 *   }).join().get();
 *   // Engine::stop() is automatically called when ScopedEngine goes out of scope
 */
class ScopedEngine {
public:
    std::unique_ptr<Engine> engine;

    ScopedEngine() : engine(std::make_unique<Engine>()) {}

    void init() {
        engine->init().get();
    }

    void initWithBackground() {
        engine->init().get();
        engine->startBackgroundTasks().get();
    }

    Engine* operator->() { return engine.get(); }
    Engine* get() { return engine.get(); }

    ~ScopedEngine() {
        if (engine) {
            try {
                engine->stop().get();
            } catch (...) {
                // Swallow errors during cleanup
            }
        }
    }

    // Non-copyable, non-movable
    ScopedEngine(const ScopedEngine&) = delete;
    ScopedEngine& operator=(const ScopedEngine&) = delete;
};

/**
 * RAII wrapper for seastar::sharded<Engine> that ensures proper cleanup
 * even on test failure (e.g., ASSERT_* early returns or exceptions).
 *
 * Usage:
 *   seastar::thread([this] {
 *       ScopedShardedEngine eng;
 *       eng.start();
 *       // ... test code ...
 *   }).join().get();
 *   // Engine cleanup is automatic when ScopedShardedEngine goes out of scope
 */
class ScopedShardedEngine {
public:
    seastar::sharded<Engine> eng;

    ScopedShardedEngine() = default;

    void start() {
        eng.start().get();
        eng.invoke_on_all([](Engine& engine) {
            return engine.init();
        }).get();
        // Set back-reference for cross-shard metadata indexing
        eng.invoke_on_all([this](Engine& engine) {
            engine.setShardedRef(&eng);
            return seastar::make_ready_future<>();
        }).get();
    }

    void startWithBackground() {
        eng.start().get();
        eng.invoke_on_all([](Engine& engine) {
            return engine.init();
        }).get();
        // Set back-reference for cross-shard metadata indexing
        eng.invoke_on_all([this](Engine& engine) {
            engine.setShardedRef(&eng);
            return seastar::make_ready_future<>();
        }).get();
        eng.invoke_on_all([](Engine& engine) {
            return engine.startBackgroundTasks();
        }).get();
    }

    seastar::sharded<Engine>& operator*() { return eng; }
    seastar::sharded<Engine>* operator->() { return &eng; }

    ~ScopedShardedEngine() {
        try {
            eng.invoke_on_all([](Engine& engine) {
                return engine.stop();
            }).get();
            eng.stop().get();
        } catch (...) {
            // Swallow errors during cleanup
        }
    }

    // Non-copyable, non-movable
    ScopedShardedEngine(const ScopedShardedEngine&) = delete;
    ScopedShardedEngine& operator=(const ScopedShardedEngine&) = delete;
};
