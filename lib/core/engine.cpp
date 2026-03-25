#include "engine.hpp"

#include "aggregator.hpp"
#include "key_encoding.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "placement_table.hpp"
#include "query_runner.hpp"
#include "series_key.hpp"
#include "tsm_compactor.hpp"
#include "tsm_writer.hpp"
#include "util.hpp"

#include <chrono>
#include <filesystem>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <unordered_set>
#include <vector>

namespace fs = std::filesystem;

Engine::Engine() : index(seastar::this_shard_id()) {
    shardId = seastar::this_shard_id();
    // Directory creation moved to init() to avoid blocking the reactor thread
};

seastar::future<> Engine::init() {
    co_await createDirectoryStructure();
    co_await index.open();
    co_await tsmFileManager.init();
    if (_schedulingGroupsCreated) {
        tsmFileManager.setCompactionGroup(_compactionGroup);
    }
    co_await walFileManager.init(*this, tsmFileManager);

    // Register per-shard Prometheus metrics
    _metrics.setup(*this);

    // Retention policy loading is done post-startup via loadAndBroadcastRetentionPolicies()
    // after setShardedRef() has been called on all shards.

    // Background compaction loop is optional - inline compaction is triggered automatically
    // in TSMFileManager::writeMemstore() after each WAL rollover creates a new TSM file.
    // When a tier reaches 4 files, they are compacted into 1 file at the next tier level.
    // The background loop provides additional periodic checks but is not required for
    // normal operation. Uncomment to enable periodic compaction checks:
    // co_await tsmFileManager.startCompactionLoop();
};

seastar::future<> Engine::createDirectoryStructure() {
    std::string shardPath = basePath() + "/tsm";
    // Wrap blocking std::filesystem call in seastar::async to avoid
    // blocking the Seastar reactor thread.
    co_await seastar::async([&shardPath]() { fs::create_directories(shardPath); });
}

std::string Engine::basePath() {
    return std::string("shard_" + std::to_string(shardId));
}

seastar::future<> Engine::stop() {
    auto stopStart = std::chrono::steady_clock::now();
    auto elapsedMs = [&]() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - stopStart)
            .count();
    };

    timestar::engine_log.info("[ENGINE_STOP] Starting shutdown on shard {}", shardId);

    if (!_insertGate.is_closed()) {
        timestar::engine_log.info("[ENGINE_STOP] Closing insert gate ({} in-flight) on shard {}",
                                  _insertGate.get_count(), shardId);
        co_await _insertGate.close();
        timestar::engine_log.info("[ENGINE_STOP] Insert gate closed ({}ms) on shard {}", elapsedMs(), shardId);
    }

    if (!_streamingGate.is_closed()) {
        co_await _streamingGate.close();
        timestar::engine_log.info("[ENGINE_STOP] Streaming gate closed ({}ms) on shard {}", elapsedMs(), shardId);
    }

    if (shardId == 0) {
        _retentionTimer.cancel();
    }
    if (!_retentionGate.is_closed()) {
        co_await _retentionGate.close();
        if (shardId == 0) {
            timestar::engine_log.info("[ENGINE_STOP] Retention drained ({}ms) on shard 0", elapsedMs());
        }
    }

    co_await tsmFileManager.stopCompactionLoop();
    timestar::engine_log.info("[ENGINE_STOP] Compaction loop stopped ({}ms) on shard {}", elapsedMs(), shardId);

    co_await tsmFileManager.stop();
    timestar::engine_log.info("[ENGINE_STOP] TSM files closed ({}ms) on shard {}", elapsedMs(), shardId);

    co_await walFileManager.close();
    timestar::engine_log.info("[ENGINE_STOP] WAL closed ({}ms) on shard {}", elapsedMs(), shardId);

    co_await index.close();
    timestar::engine_log.info("[ENGINE_STOP] Index closed ({}ms) on shard {}", elapsedMs(), shardId);

    timestar::engine_log.info("[ENGINE_STOP] Shutdown complete ({}ms) on shard {}", elapsedMs(), shardId);
}

template <class T>
seastar::future<> Engine::insert(TimeStarInsert<T> insertRequest, bool skipMetadataIndexing) {
    auto holder = _insertGate.hold();

    ++_metrics.inserts_total;
    _metrics.insert_points_total += insertRequest.values.size();

    LOG_INSERT_PATH(timestar::engine_log, debug,
                    "[ENGINE] Insert called for series: '{}', measurement: '{}', field: '{}', {} values",
                    insertRequest.seriesKey(), insertRequest.measurement, insertRequest.field,
                    insertRequest.values.size());

    // Index metadata locally — each shard maintains its own NativeIndex
    // for the series it owns. Schema changes are broadcast via indexMetadataSync.
    if (!skipMetadataIndexing) {
        LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Indexing metadata locally for series: '{}'",
                        insertRequest.seriesKey());
        co_await index.indexInsert(insertRequest);
    }

    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Processing data storage for series: '{}'",
                    insertRequest.seriesKey());

    // Notify streaming subscribers BEFORE WAL insert, because the WAL+MemoryStore
    // path moves data out of the insert (insertMemory takes by rvalue).
    if (_subscriptionManager.hasSubscribers(insertRequest.measurement)) {
        auto remotes = _subscriptionManager.notifySubscribers(insertRequest.measurement, insertRequest.getTags(),
                                                              insertRequest.field, insertRequest.getTimestamps(),
                                                              insertRequest.values);
        for (auto& rd : remotes) {
            if (shardedRef) {
                (void)seastar::with_gate(_streamingGate, [this, rd = std::move(rd)]() mutable {
                    return shardedRef
                        ->invoke_on(rd.targetShard,
                                    [subId = rd.subscriptionId, b = std::move(rd.batch)](Engine& engine) mutable {
                                        engine.getSubscriptionManager().deliverBatch(subId, std::move(b));
                                        return seastar::make_ready_future<>();
                                    })
                        .handle_exception([](std::exception_ptr ep) {
                            try {
                                std::rethrow_exception(ep);
                            } catch (const std::exception& e) {
                                timestar::engine_log.warn("[STREAM] Cross-shard delivery failed: {}", e.what());
                            } catch (...) {
                                timestar::engine_log.warn("[STREAM] Cross-shard delivery failed: unknown error");
                            }
                        });
                }).handle_exception([](std::exception_ptr) {
                    // Gate is closed (shutting down) — delivery silently dropped, which is correct
                });
            }
        }
    }

    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Starting WAL insert for single series");
    co_await walFileManager.insert(insertRequest);
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] WAL insert completed for single series");
}

template <class T>
seastar::future<WALTimingInfo> Engine::insertBatch(std::vector<TimeStarInsert<T>> insertRequests) {
    auto holder = _insertGate.hold();

    if (insertRequests.empty()) {
        co_return WALTimingInfo{};  // No work to do
    }

    // Update Prometheus metrics for batch inserts
    ++_metrics.inserts_total;
    for (const auto& req : insertRequests) {
        _metrics.insert_points_total += req.getTimestamps().size();
    }

    // Metadata indexing is now handled at the HTTP handler level on shard 0
    // This Engine method now only handles data storage (WAL + MemoryStore)
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Processing batch data storage for {} requests",
                    insertRequests.size());

    // Notify streaming subscribers BEFORE WAL insert, because the WAL+MemoryStore
    // path moves data out of the request elements (insertMemory takes by rvalue).
    // Fast-path: skip the entire loop when there are no subscribers at all.
    if (_subscriptionManager.subscriptionCount() > 0) {
        for (const auto& req : insertRequests) {
            if (_subscriptionManager.hasSubscribers(req.measurement)) {
                auto remotes = _subscriptionManager.notifySubscribers(req.measurement, req.getTags(), req.field,
                                                                      req.getTimestamps(), req.values);
                for (auto& rd : remotes) {
                    if (shardedRef) {
                        (void)seastar::with_gate(_streamingGate, [this, rd = std::move(rd)]() mutable {
                            return shardedRef
                                ->invoke_on(
                                    rd.targetShard,
                                    [subId = rd.subscriptionId, b = std::move(rd.batch)](Engine& engine) mutable {
                                        engine.getSubscriptionManager().deliverBatch(subId, std::move(b));
                                        return seastar::make_ready_future<>();
                                    })
                                .handle_exception([](std::exception_ptr ep) {
                                    try {
                                        std::rethrow_exception(ep);
                                    } catch (const std::exception& e) {
                                        timestar::engine_log.warn("[STREAM] Cross-shard delivery failed: {}", e.what());
                                    } catch (...) {
                                        timestar::engine_log.warn(
                                            "[STREAM] Cross-shard delivery failed: unknown error");
                                    }
                                });
                        }).handle_exception([](std::exception_ptr) {
                            // Gate is closed (shutting down) — delivery silently dropped, which is correct
                        });
                    }
                }
            }
        }
    }

    // Use WAL file manager batch insert
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Starting unified WAL batch insert for {} requests",
                    insertRequests.size());

#if TIMESTAR_LOG_INSERT_PATH
    auto start_wal_batch = std::chrono::high_resolution_clock::now();
#endif
    co_await walFileManager.insertBatch(insertRequests);

    // Create timing info
    WALTimingInfo walTiming;
#if TIMESTAR_LOG_INSERT_PATH
    auto end_wal_batch = std::chrono::high_resolution_clock::now();
    walTiming.walWriteTime = std::chrono::duration_cast<std::chrono::microseconds>(end_wal_batch - start_wal_batch);
#endif
    walTiming.walWriteCount = insertRequests.size();

    co_return walTiming;
}

template <class T>
seastar::future<SeriesId128> Engine::indexMetadata(TimeStarInsert<T> insertRequest) {
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Indexing metadata for series: '{}' on shard {}",
                    insertRequest.seriesKey(), shardId);
    SeriesId128 seriesId = co_await index.indexInsert(insertRequest);
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Metadata indexed, series ID: {}", seriesId.toHex());
    co_return seriesId;
}

seastar::future<> Engine::indexMetadataBatch(const std::vector<MetadataOp>& ops) {
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Batch indexing metadata for {} ops on shard {}", ops.size(),
                    shardId);
    co_await index.indexMetadataBatch(ops);
    LOG_INSERT_PATH(timestar::engine_log, debug, "[ENGINE] Batch metadata indexing complete on shard {}", shardId);
}

seastar::future<> Engine::indexMetadataSync(std::vector<MetadataOp> metaOps) {
    if (metaOps.empty()) [[unlikely]] {
        co_return;
    }

    if (shardedRef == nullptr) [[unlikely]] {
        timestar::engine_log.warn("[METADATA] No shardedRef, dropping {} metadata ops", metaOps.size());
        co_return;
    }

    // Group MetadataOps by target shard based on series hash
    unsigned shardCount = seastar::smp::count;
    std::vector<std::vector<MetadataOp>> opsByShard(shardCount);

    for (auto& op : metaOps) {
        // Use the same hash as the write handler to ensure metadata lands on the
        // same shard as the data. buildSeriesKey + fromSeriesKey is the canonical path.
        std::string seriesKey = timestar::buildSeriesKey(op.measurement, op.tags, op.fieldName);
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        unsigned targetShard = timestar::routeToCore(seriesId);
        opsByShard[targetShard].push_back(std::move(op));
    }

    // Dispatch each group to its owning shard IN PARALLEL (not sequential co_await).
    // Sequential dispatch was a critical scaling bottleneck: N cross-shard RPCs in series.
    std::vector<seastar::future<timestar::index::SchemaUpdate>> futures;
    futures.reserve(shardCount);
    for (unsigned s = 0; s < shardCount; ++s) {
        if (opsByShard[s].empty())
            continue;
        futures.push_back(shardedRef->invoke_on(
            s,
            [ops = std::move(opsByShard[s])](Engine& engine) mutable -> seastar::future<timestar::index::SchemaUpdate> {
                co_return co_await engine.index.indexMetadataBatchWithSchema(ops);
            }));
    }

    timestar::index::SchemaUpdate combined;
    if (!futures.empty()) {
        auto results = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        for (auto& update : results) {
            combined.merge(update);
        }
    }

    // Broadcast schema changes — fire-and-forget (don't block write response).
    // Schema caches are eventually consistent; queries will see updates on next read.
    if (!combined.empty()) {
        (void)seastar::try_with_gate(_insertGate, [this, combined = std::move(combined)]() mutable {
            return broadcastSchemaUpdate(std::move(combined));
        }).handle_exception([](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // Shutting down — safe to discard.
            } catch (const std::exception& e) {
                timestar::engine_log.error("[METADATA] Schema broadcast failed: {}", e.what());
            }
        });
    }
}

seastar::future<> Engine::broadcastSchemaUpdate(timestar::index::SchemaUpdate update) {
    if (!shardedRef || update.empty())
        co_return;
    co_await shardedRef->invoke_on_all([update = std::move(update)](Engine& e) {
        e.getIndex().applySchemaUpdate(update);
        return seastar::make_ready_future<>();
    });
}

seastar::future<std::optional<VariantQueryResult>> Engine::query(std::string series, uint64_t startTime,
                                                                 uint64_t endTime) {
    // Compute SeriesId128 once and pass through to avoid redundant SHA1 hashing
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(series);
    co_return co_await query(std::move(series), seriesId, startTime, endTime);
}

seastar::future<std::optional<VariantQueryResult>> Engine::query(std::string series, const SeriesId128& seriesId,
                                                                 uint64_t startTime, uint64_t endTime) {
    QueryRunner runner(&tsmFileManager, &walFileManager);
    try {
        co_return co_await runner.runQuery(series, seriesId, startTime, endTime);
    } catch (const SeriesNotFoundException&) {
        co_return std::nullopt;
    }
}

seastar::future<std::optional<timestar::PushdownResult>> Engine::queryAggregated(const std::string& seriesKey,
                                                                                 const SeriesId128& seriesId,
                                                                                 uint64_t startTime, uint64_t endTime,
                                                                                 uint64_t aggregationInterval,
                                                                                 timestar::AggregationMethod method) {
    QueryRunner runner(&tsmFileManager, &walFileManager);
    co_return co_await runner.queryTsmAggregated(std::string(seriesKey), seriesId, startTime, endTime,
                                                 aggregationInterval, method);
}

seastar::future<std::vector<std::optional<timestar::PushdownResult>>> Engine::queryAggregatedMultiField(
    const std::vector<std::pair<std::string, SeriesId128>>& entries, uint64_t startTime, uint64_t endTime,
    uint64_t aggregationInterval, timestar::AggregationMethod method) {
    QueryRunner runner(&tsmFileManager, &walFileManager);
    co_return co_await runner.queryTsmAggregatedBatch(entries, startTime, endTime, aggregationInterval, method);
}

seastar::future<> Engine::batchLatest(std::vector<BatchLatestEntry>& entries, uint64_t startTime, uint64_t endTime,
                                      bool wantFirst) {
    if (entries.empty())
        co_return;

    // --- Phase 1: TSM sparse index scan (zero I/O) ---
    // Snapshot TSM files once.  getSequencedTsmFiles() is ordered by (tier, seqNum);
    // reverse for LATEST (newest files first) so the first sparse hit is the best.
    std::vector<seastar::shared_ptr<TSM>> tsmFiles;
    tsmFiles.reserve(tsmFileManager.getSequencedTsmFiles().size());
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        tsmFiles.push_back(tsmFile);
    }
    if (!wantFirst) {
        std::reverse(tsmFiles.begin(), tsmFiles.end());
    }

    // Track how many series are still unresolved for early termination.
    size_t unresolvedCount = entries.size();

    for (const auto& tsmFile : tsmFiles) {
        if (unresolvedCount == 0)
            break;
        if (tsmFile->hasTombstones())
            continue;

        for (auto& entry : entries) {
            if (entry.resolved)
                continue;

            auto pt =
                wantFirst ? tsmFile->getFirstFromSparse(entry.seriesId) : tsmFile->getLatestFromSparse(entry.seriesId);
            if (!pt.has_value())
                continue;
            if (pt->timestamp < startTime || pt->timestamp > endTime)
                continue;

            if (!entry.resolved) {
                entry.timestamp = pt->timestamp;
                entry.value = pt->value;
                entry.resolved = true;
                --unresolvedCount;
            } else if (wantFirst ? (pt->timestamp < entry.timestamp) : (pt->timestamp > entry.timestamp)) {
                entry.timestamp = pt->timestamp;
                entry.value = pt->value;
            }
        }
    }

    // --- Phase 2: If sparse index missed some (no extended stats or tombstones),
    // fall back to selective DMA reads for unresolved series. ---
    if (unresolvedCount > 0) {
        for (const auto& tsmFile : tsmFiles) {
            if (unresolvedCount == 0)
                break;
            for (auto& entry : entries) {
                if (entry.resolved)
                    continue;
                if (!tsmFile->seriesMayOverlapTime(entry.seriesId, startTime, endTime))
                    continue;

                timestar::BlockAggregator agg(
                    0, startTime, endTime,
                    wantFirst ? timestar::AggregationMethod::FIRST : timestar::AggregationMethod::LATEST, true);
                agg.enableFoldToSingleState();
                size_t pts =
                    co_await tsmFile->aggregateSeriesSelective(entry.seriesId, startTime, endTime, agg, !wantFirst, 1);
                if (pts > 0) {
                    auto state = agg.takeSingleState();
                    if (wantFirst) {
                        entry.timestamp = state.firstTimestamp;
                        entry.value = state.first;
                    } else {
                        entry.timestamp = state.latestTimestamp;
                        entry.value = state.latest;
                    }
                    entry.resolved = true;
                    --unresolvedCount;
                }
            }
        }
    }

    // --- Phase 3: Memory stores (may have newer/older data than TSM) ---
    // Iterate stores in outer loop (fewer stores than entries) for better
    // cache locality on the memory store's internal hash map.
    // Helper lambda to check a typed memory series and update entry.value as double.
    auto checkMemSeries = [&](const auto* series, auto& entry) {
        if (!series || series->timestamps.empty())
            return;
        if (!wantFirst) {
            // LATEST: check last element (timestamps are sorted ascending)
            auto it = std::upper_bound(series->timestamps.begin(), series->timestamps.end(), endTime);
            if (it == series->timestamps.begin())
                return;
            --it;
            if (*it < startTime)
                return;
            size_t idx = static_cast<size_t>(it - series->timestamps.begin());
            if (!entry.resolved || series->timestamps[idx] > entry.timestamp) {
                entry.timestamp = series->timestamps[idx];
                entry.value = static_cast<double>(series->values[idx]);
                entry.resolved = true;
            }
        } else {
            // FIRST: check first element in range
            auto it = std::lower_bound(series->timestamps.begin(), series->timestamps.end(), startTime);
            if (it == series->timestamps.end() || *it > endTime)
                return;
            size_t idx = static_cast<size_t>(it - series->timestamps.begin());
            if (!entry.resolved || series->timestamps[idx] < entry.timestamp) {
                entry.timestamp = series->timestamps[idx];
                entry.value = static_cast<double>(series->values[idx]);
                entry.resolved = true;
            }
        }
    };

    for (const auto& memStore : walFileManager.getMemoryStores()) {
        for (auto& entry : entries) {
            checkMemSeries(memStore->querySeries<double>(entry.seriesId), entry);
            checkMemSeries(memStore->querySeries<int64_t>(entry.seriesId), entry);
            checkMemSeries(memStore->querySeries<bool>(entry.seriesId), entry);
        }
    }
}

seastar::future<> Engine::prefetchSeriesIndices(const std::vector<SeriesId128>& seriesIds) {
    // Snapshot TSM file pointers so compaction cannot invalidate iterators
    // across co_await suspension points.
    std::vector<seastar::shared_ptr<TSM>> tsmSnapshot;
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        tsmSnapshot.push_back(tsmFile);
    }
    // Prefetch all TSM files in parallel (was sequential: one co_await per file).
    co_await seastar::parallel_for_each(tsmSnapshot, [&seriesIds](seastar::shared_ptr<TSM>& tsmFile) {
        return tsmFile->prefetchFullIndexEntries(seriesIds);
    });
}

seastar::future<> Engine::startBackgroundTasks() {
    // Background TSM conversions are launched per-rollover via seastar::try_with_gate
    // in WALFileManager::rolloverMemoryStore(). No separate startup needed.
    co_return;
}

seastar::future<VariantQueryResult> Engine::queryBySeries(std::string measurement,
                                                          std::map<std::string, std::string> tags, std::string field,
                                                          uint64_t startTime, uint64_t endTime) {
    // Get series ID from index
    auto seriesIdOpt = co_await index.getSeriesId(measurement, tags, field);

    if (!seriesIdOpt.has_value()) {
        // Series doesn't exist, return empty result
        co_return VariantQueryResult{QueryResult<double>{}};
    }

    // Convert back to series key for now (until we fully migrate to numeric IDs)
    TimeStarInsert<double> temp(measurement, field);
    temp.tags = tags;
    std::string seriesKey = temp.seriesKey();

    // Pre-compute SeriesId128 once and pass through to avoid redundant SHA1
    SeriesId128 dataSeriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Use existing query infrastructure - handle optional return
    auto resultOpt = co_await query(seriesKey, dataSeriesId, startTime, endTime);
    if (resultOpt.has_value()) {
        co_return std::move(resultOpt.value());
    }
    // Series data not found, return empty result
    co_return VariantQueryResult{QueryResult<double>{}};
}

seastar::future<std::vector<std::string>> Engine::getAllMeasurements() {
    auto measurements = co_await index.getAllMeasurements();
    // Convert set to vector (already sorted since std::set maintains order)
    std::vector<std::string> result(measurements.begin(), measurements.end());
    co_return result;
}

seastar::future<std::set<std::string>> Engine::getMeasurementFields(const std::string& measurement) {
    co_return co_await index.getFields(measurement);
}

seastar::future<std::set<std::string>> Engine::getMeasurementTags(const std::string& measurement) {
    co_return co_await index.getTags(measurement);
}

seastar::future<std::set<std::string>> Engine::getTagValues(const std::string& measurement, const std::string& tagKey) {
    co_return co_await index.getTagValues(measurement, tagKey);
}

seastar::future<std::vector<timestar::SeriesResult>> Engine::executeLocalQuery(const timestar::ShardQuery& shardQuery) {
    std::vector<timestar::SeriesResult> results;

    if (shardQuery.seriesIds.empty()) {
        co_return results;
    }

    // Fetch metadata from local index — each shard owns its own series metadata
    auto metadataBatch = co_await index.getSeriesMetadataBatch(shardQuery.seriesIds);

    // Process each series with its pre-fetched metadata
    for (const auto& [seriesId, seriesMetaOpt] : metadataBatch) {
        if (!seriesMetaOpt.has_value()) {
            continue;  // Series no longer exists
        }

        const auto& meta = seriesMetaOpt.value();

        // Convert to old-style series key for now (until we fully migrate)
        TimeStarInsert<double> temp(meta.measurement, meta.field);
        temp.tags = meta.tags;
        std::string seriesKey = temp.seriesKey();

        // Query data using existing infrastructure
        QueryRunner runner(&tsmFileManager, &walFileManager);
        auto variantResult = co_await runner.runQuery(seriesKey, shardQuery.startTime, shardQuery.endTime);

        // Convert to SeriesResult format
        timestar::SeriesResult seriesResult;
        seriesResult.measurement = meta.measurement;
        seriesResult.tags = meta.tags;

        // Handle different value types (all branches are identical)
        std::visit(
            [&](auto&& result) {
                if (!result.timestamps.empty()) {
                    seriesResult.fields[meta.field] =
                        std::make_pair(std::move(result.timestamps), timestar::FieldValues(std::move(result.values)));
                }
            },
            variantResult);

        // Only add if we got data
        if (!seriesResult.fields.empty()) {
            results.push_back(std::move(seriesResult));
        }
    }

    co_return results;
}

seastar::future<bool> Engine::deleteRange(std::string seriesKey, uint64_t startTime, uint64_t endTime) {
    auto gate_holder = _insertGate.hold();
    ++_metrics.deletes_total;
    co_return co_await deleteRangeImpl(std::move(seriesKey), startTime, endTime);
}

seastar::future<bool> Engine::deleteRangeImpl(std::string seriesKey, uint64_t startTime, uint64_t endTime) {
    // Delete from all TSM files that contain this series in the time range
    bool anyDeleted = false;

    // Compute SeriesId128 once for all lookups (avoids redundant XXH3 hashes)
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Check if the series exists in memory stores BEFORE deleting
    bool existsInMemory = false;
    if (walFileManager.queryMemoryStores<double>(seriesId).has_value() ||
        walFileManager.queryMemoryStores<bool>(seriesId).has_value() ||
        walFileManager.queryMemoryStores<std::string>(seriesId).has_value() ||
        walFileManager.queryMemoryStores<int64_t>(seriesId).has_value()) {
        existsInMemory = true;
    }

    // Delete from memory stores and write to WAL
    co_await walFileManager.deleteFromMemoryStores(seriesKey, startTime, endTime);
    if (existsInMemory) {
        anyDeleted = true;
    }

    // Snapshot TSM file pointers to avoid iterator invalidation across co_await
    // (background compaction can mutate getSequencedTsmFiles() during suspension)
    std::vector<seastar::shared_ptr<TSM>> tsmSnapshot;
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        tsmSnapshot.push_back(tsmFile);
    }
    for (const auto& tsmFile : tsmSnapshot) {
        bool deleted = co_await tsmFile->deleteRange(seriesId, startTime, endTime);
        if (deleted) {
            anyDeleted = true;
        }
    }

    co_return anyDeleted;
}

seastar::future<bool> Engine::deleteRangeBySeries(std::string measurement, std::map<std::string, std::string> tags,
                                                  std::string field, uint64_t startTime, uint64_t endTime) {
    auto gate_holder = _insertGate.hold();
    ++_metrics.deletes_total;
    // Look up series ID without creating it — deleting a non-existent series is a no-op.
    // Each shard has its own index now — look up locally.
    auto seriesIdOpt = co_await index.getSeriesId(measurement, tags, field);

    if (!seriesIdOpt.has_value()) {
        co_return false;  // Series doesn't exist, nothing to delete
    }

    // Use canonical series key construction
    TimeStarInsert<double> temp(measurement, field);
    temp.tags = tags;
    std::string seriesKey = temp.seriesKey();

    // Call the internal impl directly — we already hold the gate
    co_return co_await deleteRangeImpl(seriesKey, startTime, endTime);
}

seastar::future<Engine::DeleteResult> Engine::deleteByPattern(const DeleteRequest& request) {
    auto gate_holder = _insertGate.hold();
    ++_metrics.deletes_total;
    DeleteResult result;

    // Debug logging disabled - uncomment for troubleshooting
    // timestar::engine_log.info("deleteByPattern called on shard {} for measurement: {}",
    //                       shardId, request.measurement);

    // Step 1: Find all series IDs that match the pattern from local index
    // Each shard's index only has its own series
    std::vector<SeriesId128> seriesIds;
    if (request.tags.empty()) {
        auto findResult = co_await index.getAllSeriesForMeasurement(request.measurement);
        if (findResult.has_value())
            seriesIds = std::move(findResult.value());
    } else {
        auto findResult = co_await index.findSeries(request.measurement, request.tags);
        if (findResult.has_value())
            seriesIds = std::move(findResult.value());
    }

    // Step 2: Get metadata for each series to check field filters
    // Batch all metadata lookups into a single RPC to shard 0 to avoid N sequential RPCs.
    std::vector<std::pair<SeriesId128, std::string>> seriesToDelete;  // (seriesId, seriesKey)

    // Convert fields to unordered_set for O(1) lookup (vs O(n) linear search)
    std::unordered_set<std::string> fieldFilter(request.fields.begin(), request.fields.end());

    auto filterAndBuild = [&fieldFilter](const std::vector<SeriesId128>& ids, timestar::index::NativeIndex& idx)
        -> seastar::future<std::vector<std::pair<SeriesId128, std::string>>> {
        std::vector<std::pair<SeriesId128, std::string>> result;
        for (const auto& seriesId : ids) {
            auto metadata = co_await idx.getSeriesMetadata(seriesId);
            if (!metadata.has_value())
                continue;
            if (!fieldFilter.empty() && fieldFilter.count(metadata->field) == 0)
                continue;
            TimeStarInsert<double> temp(metadata->measurement, metadata->field);
            temp.tags = metadata->tags;
            result.push_back({seriesId, temp.seriesKey()});
        }
        co_return result;
    };

    // Metadata is local — each shard's index has only its own series
    seriesToDelete = co_await filterAndBuild(seriesIds, index);

    // Step 3: Delete each matching series
    for (const auto& [seriesId, seriesKey] : seriesToDelete) {
        bool deleted = co_await deleteRangeImpl(seriesKey, request.startTime, request.endTime);
        if (deleted) {
            result.seriesDeleted++;
            result.deletedSeries.push_back(seriesKey);

            // Estimate points deleted (this is rough - actual count would require reading the data)
            // For now, we'll just track that deletion occurred
            result.pointsDeleted++;  // This is a placeholder
        }
    }

    // timestar::engine_log.info("[DELETE_DEBUG] Shard {} deleted {} series",
    //                       shardId, result.seriesDeleted);

    co_return result;
}

seastar::future<> Engine::rolloverMemoryStore() {
    auto gate_holder = _insertGate.hold();
    ++_metrics.wal_rollovers_total;
    timestar::engine_log.debug("[ENGINE] Rolling over memory store on shard {}", shardId);
    co_return co_await walFileManager.rolloverMemoryStore();
}

template seastar::future<> Engine::insert<bool>(TimeStarInsert<bool> insertRequest, bool skipMetadataIndexing);
template seastar::future<> Engine::insert<double>(TimeStarInsert<double> insertRequest, bool skipMetadataIndexing);
template seastar::future<> Engine::insert<std::string>(TimeStarInsert<std::string> insertRequest,
                                                       bool skipMetadataIndexing);
template seastar::future<> Engine::insert<int64_t>(TimeStarInsert<int64_t> insertRequest, bool skipMetadataIndexing);

template seastar::future<WALTimingInfo> Engine::insertBatch<bool>(std::vector<TimeStarInsert<bool>> insertRequests);
template seastar::future<WALTimingInfo> Engine::insertBatch<double>(std::vector<TimeStarInsert<double>> insertRequests);
template seastar::future<WALTimingInfo> Engine::insertBatch<std::string>(
    std::vector<TimeStarInsert<std::string>> insertRequests);
template seastar::future<WALTimingInfo> Engine::insertBatch<int64_t>(
    std::vector<TimeStarInsert<int64_t>> insertRequests);

template seastar::future<SeriesId128> Engine::indexMetadata<bool>(TimeStarInsert<bool> insertRequest);
template seastar::future<SeriesId128> Engine::indexMetadata<double>(TimeStarInsert<double> insertRequest);
template seastar::future<SeriesId128> Engine::indexMetadata<std::string>(TimeStarInsert<std::string> insertRequest);
template seastar::future<SeriesId128> Engine::indexMetadata<int64_t>(TimeStarInsert<int64_t> insertRequest);

// --- Retention policy management ---

void Engine::updateRetentionPolicyCache(const RetentionPolicy& policy) {
    _retentionPolicies[policy.measurement] = policy;
}

void Engine::removeRetentionPolicyCache(const std::string& measurement) {
    _retentionPolicies.erase(measurement);
}

void Engine::setRetentionPolicies(std::unordered_map<std::string, RetentionPolicy> policies) {
    _retentionPolicies = std::move(policies);
}

std::optional<RetentionPolicy> Engine::getRetentionPolicy(const std::string& measurement) const {
    auto it = _retentionPolicies.find(measurement);
    if (it != _retentionPolicies.end()) {
        return it->second;
    }
    return std::nullopt;
}

seastar::future<> Engine::loadAndBroadcastRetentionPolicies() {
    if (shardId != 0) {
        co_return;
    }

    if (!shardedRef) {
        co_return;
    }

    auto policies = co_await index.getAllRetentionPolicies();
    std::unordered_map<std::string, RetentionPolicy> policyMap;
    for (auto& p : policies) {
        policyMap[p.measurement] = p;
    }

    timestar::engine_log.info("[RETENTION] Loaded {} retention policies from NativeIndex", policyMap.size());

    // Broadcast to all shards (shared_ptr avoids N deep copies of the map)
    auto sharedPolicies =
        std::make_shared<const std::unordered_map<std::string, RetentionPolicy>>(std::move(policyMap));
    co_await shardedRef->invoke_on_all([sharedPolicies](Engine& engine) {
        engine.setRetentionPolicies(*sharedPolicies);
        return seastar::make_ready_future<>();
    });
}

void Engine::startRetentionSweepTimer() {
    if (shardId != 0)
        return;

    _retentionTimer.set_callback([this] {
        if (!shardedRef)
            return;

        // Use try_with_gate to safely handle the gate being closed during shutdown.
        // This avoids the TOCTOU race between is_closed() check and enter().
        (void)seastar::try_with_gate(_retentionGate, [this] {
            return shardedRef->invoke_on_all([](Engine& engine) {
                return engine.sweepExpiredFiles().then([&engine] { return engine.sweepTombstoneRewrites(); });
            });
        }).handle_exception([](std::exception_ptr ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // Shutdown in progress — expected, ignore.
            } catch (const std::exception& e) {
                timestar::engine_log.warn("[RETENTION] Sweep failed: {}", e.what());
            } catch (...) {
                timestar::engine_log.warn("[RETENTION] Sweep failed with unknown error");
            }
        });
    });

    auto sweepMinutes = timestar::config().engine.retention_sweep_interval_minutes;
    _retentionTimer.arm_periodic(std::chrono::minutes(sweepMinutes));
    timestar::engine_log.info("[RETENTION] Started retention sweep timer ({}min interval)", sweepMinutes);
}

seastar::future<> Engine::sweepExpiredFiles() {
    if (_retentionPolicies.empty()) {
        co_return;
    }

    uint64_t now =
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    // Build measurement -> ttlCutoff map from local cache
    std::unordered_map<std::string, uint64_t> ttlCutoffs;
    for (const auto& [measurement, policy] : _retentionPolicies) {
        if (policy.ttlNanos > 0 && now > policy.ttlNanos) {
            ttlCutoffs[measurement] = now - policy.ttlNanos;
        }
    }

    if (ttlCutoffs.empty()) {
        co_return;
    }

    // Build seriesId -> measurement map from local index
    // Each shard's index only has series owned by this shard
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMeasurementMap;
    for (const auto& [measurement, cutoff] : ttlCutoffs) {
        auto result = co_await index.getAllSeriesForMeasurement(measurement);
        std::vector<SeriesId128> seriesIds;
        if (result.has_value()) {
            seriesIds = std::move(result.value());
        }
        for (const auto& sid : seriesIds) {
            seriesMeasurementMap[sid] = measurement;
        }
    }

    if (seriesMeasurementMap.empty()) {
        co_return;
    }

    // Snapshot TSM file pointers — background compaction can mutate the live map
    // between co_await calls above and this iteration.
    std::vector<seastar::shared_ptr<TSM>> tsmSnapshot;
    for (auto& [rank, tsm] : tsmFileManager.getSequencedTsmFiles()) {
        tsmSnapshot.push_back(tsm);
    }

    // Iterate TSM files, check block metadata against TTL cutoffs
    std::vector<seastar::shared_ptr<TSM>> fullyExpiredFiles;

    for (const auto& tsmFile : tsmSnapshot) {
        bool allExpired = true;

        auto allSeriesIds = tsmFile->getSeriesIds();
        for (const auto& seriesId : allSeriesIds) {
            auto it = seriesMeasurementMap.find(seriesId);
            if (it == seriesMeasurementMap.end()) {
                // Series not under TTL policy - file is not fully expired
                allExpired = false;
                continue;
            }

            uint64_t cutoff = ttlCutoffs[it->second];
            // Use sparse index maxTime (always in memory, no I/O) instead of
            // getSeriesBlocks() which only returns data from the LRU cache.
            // A cache miss would return empty blocks, leaving allExpired=true
            // and causing premature file deletion.
            uint64_t maxTime = tsmFile->getSeriesMaxTime(seriesId);
            if (maxTime >= cutoff) {
                allExpired = false;
            }
        }

        if (allExpired && !allSeriesIds.empty()) {
            fullyExpiredFiles.push_back(tsmFile);
        }
        // Partially expired files will be cleaned up during normal compaction
        // (which now applies TTL filtering)
    }

    if (!fullyExpiredFiles.empty()) {
        timestar::engine_log.info("[RETENTION] Shard {}: removing {} fully expired TSM files", shardId,
                                  fullyExpiredFiles.size());
        co_await tsmFileManager.removeTSMFiles(fullyExpiredFiles);
    }

    // Phase 3: Clean up expired day bitmaps from the index
    for (const auto& [measurement, cutoff] : ttlCutoffs) {
        uint32_t cutoffDay = timestar::index::keys::dayBucketFromNs(cutoff);
        co_await index.removeExpiredDayBitmaps(measurement, cutoffDay);
    }
}

seastar::future<> Engine::sweepTombstoneRewrites() {
    const double DEAD_FRACTION_THRESHOLD = timestar::config().engine.tombstone_dead_fraction_threshold;
    const size_t MAX_REWRITES_PER_SWEEP = timestar::config().engine.max_tombstone_rewrites_per_sweep;

    auto* compactor = tsmFileManager.getCompactor();
    if (!compactor) {
        co_return;
    }

    // --- Phase 1: Snapshot ---
    // Snapshot file pointers before any co_await.  The compaction loop runs
    // as a separate coroutine on this shard; during any suspension it may
    // call addTSMFile / removeTSMFiles, which mutate sequencedTsmFiles and
    // would invalidate a live iterator over the map.
    std::vector<seastar::shared_ptr<TSM>> tombstonedFiles;
    for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles()) {
        if (tsmFile->hasTombstones()) {
            tombstonedFiles.push_back(tsmFile);
        }
    }
    // hasTombstones() is O(1), no suspension points — snapshot is consistent.

    if (tombstonedFiles.empty()) {
        co_return;
    }

    // --- Phase 2: Estimate coverage (suspension points here) ---
    struct Candidate {
        seastar::shared_ptr<TSM> file;
        double deadFraction;
    };
    std::vector<Candidate> candidates;

    for (auto& tsmFile : tombstonedFiles) {
        // Skip files currently being compacted (saves the DMA prefetch cost).
        if (compactor->isFileInActiveCompaction(tsmFile)) {
            continue;
        }

        double deadFraction = co_await tsmFile->estimateTombstoneCoverage();
        if (deadFraction > DEAD_FRACTION_THRESHOLD) {
            candidates.push_back({tsmFile, deadFraction});
        }
    }

    if (candidates.empty()) {
        co_return;
    }

    // Sort by dead fraction descending (worst offenders first)
    std::sort(candidates.begin(), candidates.end(),
              [](const Candidate& a, const Candidate& b) { return a.deadFraction > b.deadFraction; });

    // --- Phase 3: Rewrite (at most MAX_REWRITES_PER_SWEEP) ---
    size_t rewriteCount = std::min(candidates.size(), MAX_REWRITES_PER_SWEEP);

    for (size_t i = 0; i < rewriteCount; ++i) {
        auto& candidate = candidates[i];

        // Non-blocking: skip remaining rewrites if both compaction slots are busy.
        // Single-threaded guarantee: no task can acquire the semaphore between
        // this check and the get_units() call inside executeCompaction(), because
        // there are no suspension points in between.
        if (!compactor->hasCompactionCapacity()) {
            break;
        }

        // Staleness guard: the file may have been removed from the file manager
        // by a compaction that completed during one of our estimation co_awaits.
        // The shared_ptr keeps the TSM object alive, but executeCompaction would
        // try to removeTSMFiles + scheduleDelete on an already-deleted file.
        auto rank = candidate.file->rankAsInteger();
        auto it = tsmFileManager.getSequencedTsmFiles().find(rank);
        if (it == tsmFileManager.getSequencedTsmFiles().end() || it->second.get() != candidate.file.get()) {
            continue;
        }

        // Also re-check active compaction: the compaction loop may have picked
        // up this file while we were estimating other files.
        if (compactor->isFileInActiveCompaction(candidate.file)) {
            continue;
        }

        timestar::engine_log.info(
            "[TOMBSTONE-REWRITE] Shard {}: rewriting file (tier {}, seq {}) "
            "with {:.1f}% estimated dead data",
            shardId, candidate.file->tierNum, candidate.file->seqNum, candidate.deadFraction * 100.0);
        try {
            auto stats = co_await compactor->executeTombstoneRewrite(candidate.file);
            timestar::engine_log.info(
                "[TOMBSTONE-REWRITE] Shard {}: rewrite complete, "
                "{} points written in {}ms",
                shardId, stats.pointsWritten, stats.duration.count());
        } catch (const std::exception& e) {
            timestar::engine_log.warn("[TOMBSTONE-REWRITE] Shard {}: rewrite failed: {}", shardId, e.what());
        }
    }
}