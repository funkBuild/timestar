#include "wal_file_manager.hpp"

#include "engine.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <optional>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <vector>

namespace fs = std::filesystem;

// WAL sequence identity is part of recovery ordering. Accept only the exact
// basename emitted by StorageLayout: at least ten decimal digits followed by
// ".wal", with no redundant leading zero once the value exceeds ten digits.
// Numeric-prefix parsing (for example, std::stoi("0000000042junk")) can alias
// a malformed name to a live sequence.
static std::optional<uint64_t> parseWalSeqNum(const std::string& path) {
    const std::string basename = fs::path(path).filename().string();
    static constexpr size_t kMinimumSequenceDigits = 10;
    static constexpr std::string_view kExtension = ".wal";
    if (basename.size() < kMinimumSequenceDigits + kExtension.size() ||
        basename.compare(basename.size() - kExtension.size(), kExtension.size(), kExtension) != 0) {
        return std::nullopt;
    }

    const size_t sequenceDigits = basename.size() - kExtension.size();
    if (sequenceDigits > kMinimumSequenceDigits && basename.front() == '0') {
        return std::nullopt;
    }

    uint64_t sequence = 0;
    const char* first = basename.data();
    const char* last = first + sequenceDigits;
    const auto [parsedEnd, error] = std::from_chars(first, last, sequence);
    if (error != std::errc{} || parsedEnd != last) {
        return std::nullopt;
    }
    return sequence;
}

WALFileManager::WALFileManager(timestar::StorageLayout layout, unsigned shard)
    : layout_(std::move(layout)),
      shardId(static_cast<int>(shard)),
      directorySync_([](const std::string& directory) { return seastar::sync_directory(directory); }) {}

seastar::future<> WALFileManager::removeRecoveredWal(const std::string& path) {
    // A retry after unlink succeeded but directory sync failed sees no name;
    // it still has to repeat the durability barrier before reporting success.
    if (co_await seastar::file_exists(path)) {
        co_await seastar::remove_file(path);
    }
    fs::path parent = fs::path(path).parent_path();
    co_await directorySync_(parent.empty() ? "." : parent.string());
}

seastar::future<> WALFileManager::runBackgroundConversion(seastar::shared_ptr<MemoryStore> store) {
    auto conversionUnits = co_await seastar::get_units(_conversionSemaphore, 1);
    co_await store->close();

    // Run the CPU-heavy encode + multi-MB DMA write in the dedicated FLUSH
    // scheduling group. Foreground inserts still preempt it, while the WAL
    // ownership handoff cannot queue behind deep tier compaction.
    if (tsmFileManager != nullptr && tsmFileManager->hasFlushGroup()) {
        co_await seastar::with_scheduling_group(tsmFileManager->flushGroup(),
                                                [this, store] { return convertWalToTsm(store); });
    } else {
        co_await convertWalToTsm(store);
    }
}

seastar::future<> WALFileManager::retryConversionUntilSuccess(seastar::shared_ptr<MemoryStore> store) {
    uint64_t attempt = 0;
    while (conversionRetryAbort_.has_value() && !conversionRetryAbort_->abort_requested()) {
        ++attempt;
        if (attempt > 1) {
            try {
                co_await seastar::sleep_abortable(conversionRetryDelay_, *conversionRetryAbort_);
            } catch (const seastar::sleep_aborted&) {
                co_return;
            }
        }

        try {
            if (attempt > 1) {
                timestar::wal_log.info("[BG_CONVERT] Retrying TSM conversion for store {} on shard {} (attempt {})",
                                       store->sequenceNumber, shardId, attempt);
            }
            co_await runBackgroundConversion(store);
            co_return;
        } catch (const std::exception& e) {
            // Never discard ownership on failure. Before publication the store
            // remains in memory queries and pending-conversion accounting;
            // after publication it remains in the admission-counted retirement
            // list until the WAL directory barrier succeeds.
            timestar::wal_log.error(
                "[BG_CONVERT] TSM conversion attempt {} failed for store {} on shard {}: {}. "
                "Data remains queryable; retrying in {}ms.",
                attempt, store->sequenceNumber, shardId, e.what(), conversionRetryDelay_.count());
        }
    }
}

seastar::future<> WALFileManager::init(Engine& engine, TSMFileManager& _tsmFileManager) {
    timestar::wal_log.info("WALFileManager::init starting for shard {}", shardId);
    tsmFileManager = &_tsmFileManager;
    if (!conversionRetryAbort_.has_value() || conversionRetryAbort_->abort_requested()) {
        conversionRetryAbort_.emplace();
    }

    // Size the conversion pool from config. It starts at 1 (header default);
    // adjust to match, using the same signal/consume resize as WAL::_encode_sem
    // since seastar::semaphore has no capacity setter.
    //
    // Conversion concurrency, not tier-merge concurrency, is what sets the
    // sustainable ingest rate: it is the only thing that frees retained-store
    // RAM, and at capacity 1 the backlog hit its ceiling and shed 53% of writes
    // at 5.8M pts/s while tier merges were idle.
    auto maxConversions = static_cast<ssize_t>(timestar::config().storage.conversion_concurrency);
    if (maxConversions < 1)
        maxConversions = 1;
    auto currentConversions = _conversionSemaphore.available_units();
    if (maxConversions > currentConversions) {
        _conversionSemaphore.signal(maxConversions - currentConversions);
    } else if (maxConversions < currentConversions) {
        _conversionSemaphore.consume(currentConversions - maxConversions);
    }

    // Let compaction see the WAL backlog so it can yield to conversion.
    // Safe to capture `this`: TSMFileManager and WALFileManager are both
    // members of the same Engine, and stopCompactionLoop() runs in
    // Engine::stop() before either is destroyed.
    tsmFileManager->setWalConversionProbe([this] { return hasPendingConversions(); });

    // Search for existing WAL's
    std::string path = engine.basePath() + '/';
    timestar::wal_log.debug("Scanning for WAL files in {} on shard {}", path, shardId);

    std::vector<std::string> discoveredWalPaths;

    // Wrap blocking std::filesystem calls in seastar::async to avoid
    // blocking the Seastar reactor thread (important for NFS / high I/O).
    discoveredWalPaths = co_await seastar::async([&path]() {
        std::vector<std::string> files;
        if (fs::exists(path)) {
            for (const auto& entry : fs::directory_iterator(path)) {
                if (endsWith(entry.path(), ".wal"))
                    files.push_back(entry.path());
            }
        }
        return files;
    });

    // Never ignore an artifact in the WAL namespace. It may be the only durable
    // copy of acknowledged data whose name was damaged or partially migrated.
    // Starting without it would expose a silently incomplete history.
    std::vector<std::pair<uint64_t, std::string>> walFiles;
    walFiles.reserve(discoveredWalPaths.size());
    for (auto& walPath : discoveredWalPaths) {
        auto seq = parseWalSeqNum(walPath);
        if (!seq.has_value()) {
            throw std::runtime_error("Refusing startup because WAL filename is not canonical: " + walPath);
        }
        walFiles.emplace_back(*seq, std::move(walPath));
    }

    // Sort WAL files by sequence number to ensure deterministic replay order.
    // directory_iterator returns entries in filesystem-dependent order; without
    // sorting, a DeleteRange in sequence 5 could replay before its Write in
    // sequence 4, causing data loss.
    std::sort(walFiles.begin(), walFiles.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

    if (!walFiles.empty()) {
        timestar::wal_log.info("Found {} existing WAL files on shard {} - converting to TSM", walFiles.size(), shardId);

        timestar::wal_log.info("Starting WAL recovery on shard {}", shardId);
    }

    // Convert them to TSM's if they exist and are closed
    for (const auto& [seqNum, walFilename] : walFiles) {

        if (!walSequenceInitialized_ || seqNum > currentWalSequenceNumber) {
            currentWalSequenceNumber = seqNum;
            walSequenceInitialized_ = true;
        }

        timestar::wal_log.debug("Creating recovery store for WAL sequence {} on shard {}", seqNum, shardId);
        seastar::shared_ptr store = seastar::make_shared<MemoryStore>(seqNum);
        timestar::wal_log.debug("Reading WAL file {} on shard {}", walFilename, shardId);
        co_await store->initFromWAL(walFilename);
        timestar::wal_log.debug("WAL recovery complete for sequence {} on shard {}", seqNum, shardId);

        // Write to TSM if there's data
        // NOTE: We always have to write the WAL to TSM since WAL's can't be resumed
        // due to make_file_output_stream
        bool conversionSucceeded = false;
        if (!store->isEmpty()) {
            timestar::wal_log.info("Writing memory store {} to TSM on shard {}", seqNum, shardId);
            try {
                co_await convertWalToTsm(store);
                conversionSucceeded = true;
            } catch (const std::bad_alloc& e) {
                timestar::wal_log.error("Failed to convert WAL {} to TSM on shard {} - bad_alloc", seqNum, shardId);
                timestar::wal_log.error("System may be low on memory - bad_alloc during WAL {} recovery on shard {}",
                                        seqNum, shardId);
                throw;
            } catch (const std::exception& e) {
                timestar::wal_log.error("Failed to convert WAL {} to TSM on shard {}: {}", seqNum, shardId, e.what());
                // Preserve the source WAL, but never finish startup without its
                // recovered contents in the live query path. Continuing here
                // would destroy `store`, create a fresh active WAL, and serve a
                // silently incomplete dataset until the next restart.
                timestar::wal_log.error(
                    "Preserving WAL file {} and refusing startup because its recovered data is not query-visible",
                    walFilename);
                throw;
            }
        } else {
            timestar::wal_log.info("WAL {} is empty, removing without creating TSM on shard {}", seqNum, shardId);
            conversionSucceeded = true;  // Empty WAL, safe to remove
        }

        // Recovery stores do not own a WAL object, so convertWalToTsm cannot
        // retire this source internally. Do not complete startup until unlink
        // and the containing-directory barrier both succeed.
        if (conversionSucceeded) {
            co_await removeRecoveredWal(walFilename);
            timestar::wal_log.info("Successfully converted and durably retired recovered WAL {} on shard {}", seqNum,
                                   shardId);
        }

        // Explicitly release the temporary memory store to free resources
        store = nullptr;
    }

    if (memoryStores.size() == 0) {
        // If WAL files were found during recovery, advance past the highest
        // sequence number.  Otherwise start at 0.
        if (walSequenceInitialized_) {
            if (currentWalSequenceNumber == std::numeric_limits<uint64_t>::max()) [[unlikely]] {
                throw std::overflow_error("WAL sequence number exhausted");
            }
            ++currentWalSequenceNumber;
        }
        // Either way, the sequence is now valid.
        walSequenceInitialized_ = true;

        seastar::shared_ptr store = seastar::make_shared<MemoryStore>(currentWalSequenceNumber);
        co_await store->initWAL(layout_, shardId);
        memoryStores.push_back(store);
    }

    timestar::wal_log.info("WAL file manager initialization complete for shard {}", shardId);

    if (!walFiles.empty()) {
        timestar::wal_log.info("WAL recovery complete on shard {}", shardId);
    }
}

template <class T>
seastar::future<> WALFileManager::insert(TimeStarInsert<T>& insertRequest) {
    LOG_INSERT_PATH(timestar::wal_log, debug, "[WAL] Insert called for series: '{}', {} values",
                    insertRequest.seriesKey(), insertRequest.values.size());

    // Ensure we have at least one memory store
    if (memoryStores.empty()) {
        throw std::runtime_error("No memory stores available for insert");
    }

    // First, estimate the size of this insert against the WAL segment limit
    if (memoryStores[0] && memoryStores[0]->getWAL()) {
        size_t estimatedSize = memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
        if (estimatedSize > MemoryStore::walSizeThreshold()) {
            // This single insert exceeds the entire WAL segment limit
            timestar::wal_log.error("Insert request of {} bytes exceeds maximum WAL size of {} bytes", estimatedSize,
                                    MemoryStore::walSizeThreshold());
            throw timestar::InsertTooLargeException("Insert batch too large - requested " +
                                                    std::to_string(estimatedSize) +
                                                    " bytes, exceeds the WAL segment limit. Please reduce batch size.");
        }
    }

    LOG_INSERT_PATH(timestar::wal_log, debug, "[WAL] Inserting into memory store for series: '{}'",
                    insertRequest.seriesKey());
    // Try to insert - returns true if rollover is needed
    bool needsRollover = co_await memoryStores[0]->insert(insertRequest);

    if (needsRollover) {
        LOG_INSERT_PATH(timestar::wal_log, debug, "[WAL] Memory store rollover needed for series: '{}'",
                        insertRequest.seriesKey());
        // Rollover the WAL
        co_await rolloverMemoryStore();

        // Now retry the insert with the new memory store
        bool retryResult = co_await memoryStores[0]->insert(insertRequest);
        if (retryResult) {
            // The insert still doesn't fit in a fresh WAL - it's too large
            size_t estimatedSize = memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
            timestar::wal_log.error("Insert batch of {} bytes too large for fresh {} byte WAL", estimatedSize,
                                    MemoryStore::walSizeThreshold());
            throw timestar::InsertTooLargeException("Insert batch too large - requested " +
                                                    std::to_string(estimatedSize) +
                                                    " bytes, exceeds the WAL segment limit. Please reduce batch size.");
        }
    }
}

template <class T>
seastar::future<> WALFileManager::insertBatch(std::vector<TimeStarInsert<T>>& insertRequests) {
    if (insertRequests.empty()) {
        co_return;  // No work to do
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto start_wal_batch = std::chrono::high_resolution_clock::now();
#endif
    LOG_INSERT_PATH(timestar::wal_log, info, "[PERF] [WAL] Batch insert started for {} requests",
                    insertRequests.size());

    // Ensure we have at least one memory store
    if (memoryStores.empty()) {
        throw std::runtime_error("No memory stores available for batch insert");
    }

    // First, estimate the total size of this batch to check if it exceeds
    // threshold. We compute this once and pass it through to MemoryStore::insertBatch
    // so it can skip its own re-estimation.
    size_t totalEstimatedSize = 0;
    if (memoryStores[0] && memoryStores[0]->getWAL()) {
        for (auto& insertRequest : insertRequests) {
            totalEstimatedSize += memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
        }

        if (totalEstimatedSize > MemoryStore::walSizeThreshold()) {
            // This batch exceeds the entire WAL limit
            timestar::wal_log.error(
                "Batch insert request of {} bytes exceeds maximum "
                "WAL size of {} bytes",
                totalEstimatedSize, MemoryStore::walSizeThreshold());
            throw timestar::InsertTooLargeException("Insert batch too large - requested " +
                                                    std::to_string(totalEstimatedSize) +
                                                    " bytes, exceeds the WAL segment limit. Please reduce batch size.");
        }
    }

    LOG_INSERT_PATH(timestar::wal_log, debug, "[WAL] Inserting batch into memory store for {} requests",
                    insertRequests.size());

#if TIMESTAR_LOG_INSERT_PATH
    auto start_memory_batch = std::chrono::high_resolution_clock::now();
#endif
    // Try to insert batch - returns true if rollover is needed.
    // Pass the pre-computed size so MemoryStore skips redundant estimation.
    bool needsRollover = co_await memoryStores[0]->insertBatch(insertRequests, totalEstimatedSize);
#if TIMESTAR_LOG_INSERT_PATH
    auto end_memory_batch = std::chrono::high_resolution_clock::now();
#endif

    if (needsRollover) {
        LOG_INSERT_PATH(timestar::wal_log, debug, "[WAL] Memory store rollover needed for batch of {} requests",
                        insertRequests.size());
        // Roll over and retry, LOOPING on concurrent fills. A single
        // rollover-then-retry mistook "the fresh store was already filled by
        // OTHER in-flight batches" for "this batch can never fit": with N
        // concurrent high-cardinality batches racing into one fresh store,
        // whichever retried third got a spurious 413 for a batch that fits an
        // empty store easily. The two cases are distinguishable -- a batch is
        // genuinely too large only if it fails against a store that was EMPTY
        // when it tried.
        constexpr int kMaxRolloverRetries = 8;
        for (int attempt = 0;; ++attempt) {
            co_await rolloverMemoryStore();
            const bool storeWasEmpty = memoryStores[0]->isEmpty();
            bool retryResult = co_await memoryStores[0]->insertBatch(insertRequests, totalEstimatedSize);
            if (!retryResult) {
                break;  // inserted (store may now be due another rollover; the next insert triggers it)
            }
            if (storeWasEmpty) {
                // Failed against a genuinely fresh store: the batch itself is
                // the problem. Re-use the already-computed totalEstimatedSize
                // (each per-insert size is cached in the TimeStarInsert).
                timestar::wal_log.error("Batch insert of {} bytes too large for fresh WAL", totalEstimatedSize);
                throw timestar::InsertTooLargeException(
                    "Insert batch too large - requested " + std::to_string(totalEstimatedSize) +
                    " bytes, exceeds the WAL segment limit. Please reduce batch size.");
            }
            if (attempt >= kMaxRolloverRetries) {
                // Persistent contention, not size: surface as retryable
                // backpressure (503 + Retry-After), never as a 413 the client
                // would respond to by shrinking a batch that is not too big.
                throw timestar::IngestBacklogException("Shard " + std::to_string(shardId) +
                                                       " rollover contention: fresh stores filled by concurrent "
                                                       "batches " +
                                                       std::to_string(attempt + 1) + " times in a row");
            }
        }
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto end_wal_batch = std::chrono::high_resolution_clock::now();
    auto memory_batch_duration =
        std::chrono::duration_cast<std::chrono::microseconds>(end_memory_batch - start_memory_batch);
    auto wal_batch_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_wal_batch - start_wal_batch);

    LOG_INSERT_PATH(timestar::wal_log, info, "[PERF] [WAL] Memory batch insert: {}μs", memory_batch_duration.count());
    LOG_INSERT_PATH(timestar::wal_log, info, "[PERF] [WAL] Total batch insert: {}μs", wal_batch_duration.count());
#endif
}

seastar::future<> WALFileManager::rolloverMemoryStore() {
    // Acquire the rollover semaphore to serialize concurrent rollover attempts.
    // Multiple insert coroutines may observe needsRollover=true and call this
    // method simultaneously; the semaphore ensures only one rollover executes.
    auto units = co_await seastar::get_units(compactionSemaphore, 1);

    // After acquiring the semaphore, check whether the current store still
    // needs rollover. Another coroutine may have already completed a rollover
    // while we waited for the semaphore, leaving a fresh empty store at
    // index 0. Skip the rollover to avoid creating unnecessary empty stores
    // and WAL files.
    //
    // We check isEmpty() rather than isFull() because of the gap between
    // wouldExceedThreshold() (which projects the NEXT insert) and isFull()
    // (which checks CURRENT state). A store can be "not full" by isFull()
    // but still trigger rollover when the next insert is added. Checking
    // isEmpty() is conservative: it only skips when the store is definitely
    // fresh (no data at all), which means another coroutine must have just
    // rolled over.
    if (!memoryStores.empty() && memoryStores[0]->isEmpty()) {
        timestar::wal_log.debug("Rollover already completed by another coroutine on shard {}, skipping", shardId);
        co_return;
    }

    auto previousStore = memoryStores[0];
    timestar::wal_log.info("Memory store {} full (WAL segment threshold reached), rolling over",
                           previousStore->sequenceNumber);

    // Create and init the new store FIRST, before closing the old one.
    // This ensures memoryStores[0] always points to an open store,
    // even if another insert coroutine runs during a co_await yield.
    // Backlog handling: absorb bursts, never block.
    //
    // This used to spin here until the backlog drained, which made ingest feel
    // every conversion delay. Combined with inline tier compaction holding the
    // single conversion slot for a whole deep merge, that produced multi-second
    // rollover stalls and client write timeouts. Compaction no longer runs on
    // the conversion fiber, so the backlog should now drain at TSM-write speed;
    // when it does not, the policy is to accumulate rather than stall.
    //
    // Blocking is NOT reinstated as a fallback. A blocked rollover holds
    // compactionSemaphore (capacity 1), so every other rollover on the shard
    // queues behind it -- one slow conversion became a shard-wide write stall.
    // Admission control belongs at the request edge, where it can reject a
    // single write cheaply, not deep in the rollover path where it blocks all
    // of them. Engine::insert consults isIngestBacklogged() and returns 503 +
    // Retry-After at the ceiling.
    if (memoryStores.size() >= kMaxUnconvertedMemoryStores) {
        timestar::wal_log.info(
            "Shard {}: {} memory stores awaiting conversion (including the active store); "
            "absorbing burst without throttling rollover",
            shardId, memoryStores.size());
    }

    if (currentWalSequenceNumber == std::numeric_limits<uint64_t>::max()) [[unlikely]] {
        throw std::overflow_error("WAL sequence number exhausted");
    }
    auto store = seastar::make_shared<MemoryStore>(++currentWalSequenceNumber);
    // Seed the series map's capacity from the retiring store: under steady
    // ingest the same fleet reports into every store, so the fresh map will
    // reach the same size again within seconds. Growing there from empty
    // rehashed the full ~330B-slot flat table ~7 times per store cycle --
    // every few seconds at high cardinality. The eventual footprint is
    // identical either way; this only moves the allocation to creation time.
    store->series.reserve(previousStore->series.size());
    co_await store->initWAL(layout_, shardId);
    memoryStores.insert(memoryStores.begin(), store);

    timestar::wal_log.info("New memory store {} created for shard {}", store->sequenceNumber, shardId);

    // Fix the retiring store's TSM sequence number NOW, while rollovers are
    // still serialized. Conversions run concurrently and complete out of
    // order; if the seq were assigned at write time (writeMemstore), a newer
    // store finishing first would take a lower seq and its data would lose
    // last-write-wins conflicts to older stores. Rollover order IS write
    // order, so this is the last point where the two can be bound together.
    if (tsmFileManager != nullptr && !previousStore->isEmpty()) {
        previousStore->reservedTsmSeq = tsmFileManager->reserveSequenceId();
    }

    // -----------------------------------------------------------------------
    // Release the rollover semaphore early. The critical section is complete:
    // memoryStores[0] now points to a fresh, empty store that can accept new
    // inserts. Any other coroutines waiting on the semaphore can proceed
    // immediately — they will either insert into the new store or (if it fills
    // up) trigger another rollover.
    //
    // The remaining work (background TSM conversion of the old store, or
    // cleanup of an empty store) does not need to block new inserts.
    // Background TSM conversions are separately serialized by
    // _conversionSemaphore and tracked by _backgroundGate.
    // -----------------------------------------------------------------------
    units.return_all();

    // Do NOT close the old store here — in-flight writes may still be
    // completing on its WAL (holding _io_gate, waiting on _io_sem).
    // The background conversion task closes the store after all pending
    // I/O drains via WAL::close() -> _io_gate.close().

    // Launch TSM conversion as a background task so writes are not blocked.
    // The gate tracks the in-flight conversion; close() drains it at shutdown.
    // The configured conversion semaphore bounds concurrent TSM encodes and
    // publications; tier compaction runs independently on its background loop.
    if (!previousStore->isEmpty()) {
        auto sid = shardId;
        auto seqNum = previousStore->sequenceNumber;
        // One gate holder spans the first attempt and every retry, eliminating
        // a shutdown race between a failed attempt and scheduling its successor.
        (void)seastar::try_with_gate(_backgroundGate, [this, store = previousStore] {
            return retryConversionUntilSuccess(store);
        }).handle_exception([sid, seqNum](auto ep) {
            try {
                std::rethrow_exception(ep);
            } catch (const seastar::gate_closed_exception&) {
                // Shutdown in progress, ignore
            } catch (const std::exception& e) {
                timestar::wal_log.error(
                    "[BG_CONVERT] Retry loop terminated unexpectedly for store {} on shard {}: {}. "
                    "Its data remains queryable and its retained store will be retried during shutdown.",
                    seqNum, sid, e.what());
            }
        });
    } else {
        // Empty store — close and remove the WAL file.
        // This path runs after the semaphore is released, so new inserts
        // are not blocked during the I/O operations.
        co_await previousStore->close();
        co_await previousStore->removeWAL();
        auto it = std::find(memoryStores.begin(), memoryStores.end(), previousStore);
        if (it != memoryStores.end()) {
            memoryStores.erase(it);
        }
    }

    timestar::wal_log.info("Rollover complete, new memory store {} created for shard {}", store->sequenceNumber,
                           shardId);
}

seastar::future<> WALFileManager::convertWalToTsm(seastar::shared_ptr<MemoryStore> store) {
    timestar::wal_log.info("[CONVERT_WAL_TO_TSM] Starting conversion of WAL {} to TSM on shard {}",
                           store->sequenceNumber, shardId);

    // Diagnostic stats for the conversion path. The full pass walks every
    // series (and every string byte for string series) plus unconditional
    // info-level logging — that is pure overhead inside the rollover path, so
    // it is compiled in only with TIMESTAR_LOG_INSERT_PATH=1.
    size_t totalSeries = store->series.size();
    [[maybe_unused]] size_t totalPoints = 0;
    [[maybe_unused]] size_t totalMemoryEstimate = 0;
    [[maybe_unused]] size_t largestSeriesPoints = 0;
    [[maybe_unused]] std::string largestSeriesKey;

#if TIMESTAR_LOG_INSERT_PATH
    for (const auto& [key, series] : store->series) {
        size_t seriesPoints = std::visit([](const auto& s) { return s.timestamps.size(); }, series);
        totalPoints += seriesPoints;

        if (seriesPoints > largestSeriesPoints) {
            largestSeriesPoints = seriesPoints;
            largestSeriesKey = key.toHex();
        }

        // Estimate memory usage for this series
        size_t seriesMemory = std::visit(
            [&seriesPoints](const auto& s) -> size_t {
                using T = typename std::decay_t<decltype(s.values)>::value_type;
                size_t mem = seriesPoints * sizeof(uint64_t);  // timestamps
                if constexpr (std::is_same_v<T, double>) {
                    mem += seriesPoints * sizeof(double);
                } else if constexpr (std::is_same_v<T, bool>) {
                    mem += seriesPoints * sizeof(bool);
                } else if constexpr (std::is_same_v<T, std::string>) {
                    // Estimate string memory
                    size_t stringMem = 0;
                    for (const auto& str : s.values) {
                        stringMem += str.size() + sizeof(std::string);
                    }
                    mem += stringMem;
                } else if constexpr (std::is_same_v<T, int64_t>) {
                    mem += seriesPoints * sizeof(int64_t);
                }
                return mem;
            },
            series);
        totalMemoryEstimate += seriesMemory;

        // Warn about large series
        if (seriesPoints > 100000) {
            timestar::wal_log.warn(
                "[LARGE_SERIES] Series '{}' with {} points (~{} MB) "
                "in memory store {}",
                key, seriesPoints, seriesMemory / (1024 * 1024), store->sequenceNumber);
        }
    }

    timestar::wal_log.info(
        "[MEMORY_STORE_STATS] Store {} on shard {}: {} series, {} "
        "total points, ~{} MB estimated memory",
        store->sequenceNumber, shardId, totalSeries, totalPoints, totalMemoryEstimate / (1024 * 1024));
    timestar::wal_log.info("[LARGEST_SERIES] Largest series: '{}' with {} points", largestSeriesKey,
                           largestSeriesPoints);
#endif  // TIMESTAR_LOG_INSERT_PATH

    uint64_t tsmBytesWritten = store->walConversionTsmBytes;
    if (!store->walConversionPublished) {
        try {
            timestar::wal_log.debug(
                "[TSM_WRITE_START] Calling tsmFileManager->writeMemstore for store {} "
                "on shard {}",
                store->sequenceNumber, shardId);
            tsmBytesWritten = co_await tsmFileManager->writeMemstore(store);
            // This marker is in-memory retry state, not the durability record:
            // writeMemstore returns only after the TSM is synced and registered.
            store->walConversionTsmBytes = tsmBytesWritten;
            store->walConversionPublished = true;
            timestar::wal_log.debug("[TSM_WRITE_SUCCESS] Successfully wrote TSM for store {} on shard {}",
                                    store->sequenceNumber, shardId);
        } catch (const std::bad_alloc& e) {
            timestar::wal_log.error(
                "[BAD_ALLOC] Memory allocation failed when writing TSM "
                "for store {} on shard {}",
                store->sequenceNumber, shardId);
#if TIMESTAR_LOG_INSERT_PATH
            timestar::wal_log.error(
                "[BAD_ALLOC] Stats: {} series, {} points, ~{} MB "
                "estimated, largest series: '{}' ({} points)",
                totalSeries, totalPoints, totalMemoryEstimate / (1024 * 1024), largestSeriesKey,
                largestSeriesPoints);
#else
            timestar::wal_log.error(
                "[BAD_ALLOC] Stats: {} series (detailed stats require TIMESTAR_LOG_INSERT_PATH=1)", totalSeries);
#endif

            timestar::wal_log.error(
                "[SYSTEM_MEMORY] System may be low on memory - bad_alloc during TSM write "
                "for store {} on shard {}",
                store->sequenceNumber, shardId);
            throw;
        } catch (const std::exception& e) {
            timestar::wal_log.error("[TSM_WRITE_ERROR] Failed to write TSM for store {} on shard {}: {}",
                                    store->sequenceNumber, shardId, e.what());
            throw;
        }
    } else {
        timestar::wal_log.info(
            "[CONVERT_WAL_TO_TSM] TSM for store {} on shard {} is already published; retrying WAL retirement only",
            store->sequenceNumber, shardId);
    }

    // Remove from memoryStores immediately after successful TSM write,
    // BEFORE the removeWAL yield point, to prevent duplicate query results.
    // A live store then moves to the non-query retirement list so its retained
    // memory remains admission-accounted until the WAL directory barrier
    // succeeds. Recovery stores do not own a live WAL and stay caller-owned.
    const bool ownsLiveWal = store->getWAL() != nullptr;
    auto it = std::find(memoryStores.begin(), memoryStores.end(), store);
    if (it != memoryStores.end()) {
        memoryStores.erase(it);
        if (ownsLiveWal &&
            std::find(walRetirementStores_.begin(), walRetirementStores_.end(), store) == walRetirementStores_.end()) {
            walRetirementStores_.push_back(store);
        }
    }

    // Capture the WAL's on-disk size BEFORE unlinking it. WAL->TSM is a format
    // change (row-oriented, per-entry framed -> columnar, ALP/Simple8b/zstd),
    // so it reclaims far more per byte processed than a tier merge, which only
    // re-packs data that is already in TSM form. That asymmetry is what
    // justifies prioritising conversion over merges, and this is the number
    // that demonstrates it.
    const uint64_t walBytes = store->walSizeOnDisk();
    co_await store->removeWAL();
    auto retirementIt = std::find(walRetirementStores_.begin(), walRetirementStores_.end(), store);
    if (retirementIt != walRetirementStores_.end()) {
        walRetirementStores_.erase(retirementIt);
    }
    const double reclaimedPct =
        walBytes > 0 ? 100.0 * (1.0 - static_cast<double>(tsmBytesWritten) / static_cast<double>(walBytes)) : 0.0;
    if (ownsLiveWal) {
        timestar::wal_log.info("Successfully converted WAL {} to TSM on shard {} | {} -> {} bytes ({:.1f}% reclaimed)",
                               store->sequenceNumber, shardId, walBytes, tsmBytesWritten, reclaimedPct);
    } else {
        // Recovery stores deliberately do not attach their source WAL. The
        // caller still owes removeRecoveredWal() and its directory barrier.
        timestar::wal_log.debug("Published recovered WAL {} as TSM on shard {}; source retirement still pending",
                                store->sequenceNumber, shardId);
    }
}

seastar::future<> WALFileManager::close() {
    timestar::wal_log.info("[WAL_CLOSE] Starting WAL file manager close on shard {}", shardId);

    // Wake any retained-store retry loops before draining their gate. A retry
    // currently inside conversion is allowed to finish; one sleeping for the
    // next attempt exits immediately so shutdown never waits out 30 seconds.
    if (conversionRetryAbort_.has_value() && !conversionRetryAbort_->abort_requested()) {
        conversionRetryAbort_->request_abort();
    }

    // Drain all in-flight background TSM conversions before closing.
    // Guard against double-close (e.g., seastar::sharded<Engine> calling stop() twice).
    if (!_backgroundGate.is_closed()) {
        timestar::wal_log.info("[WAL_CLOSE] Draining {} background TSM conversions on shard {}",
                               _backgroundGate.get_count(), shardId);
        co_await _backgroundGate.close();
        timestar::wal_log.info("[WAL_CLOSE] Background TSM conversions drained on shard {}", shardId);
    }

    // A store can already be query-visible from TSM while its WAL unlink is
    // waiting for a successful directory barrier. Retry those lightweight,
    // idempotent retirements once during shutdown; a failure leaves the WAL on
    // disk (or conservatively assumes it may reappear after a crash).
    auto retirementSnapshot = walRetirementStores_;
    for (auto& store : retirementSnapshot) {
        try {
            timestar::wal_log.info("[WAL_CLOSE] Retrying WAL retirement for published store {} on shard {}",
                                   store->sequenceNumber, shardId);
            co_await convertWalToTsm(store);
        } catch (const std::exception& e) {
            timestar::wal_log.error(
                "[WAL_CLOSE] Failed to retire WAL for published store {} on shard {}: {} "
                "(published TSM remains authoritative; WAL preserved for recovery)",
                store->sequenceNumber, shardId, e.what());
        }
    }

    // Inline conversion of remaining stores.  These run sequentially with
    // co_await (not via the background gate, which is already closed) so
    // they are safe: no concurrent background conversions can be in flight.
    // If a conversion fails, the WAL file is preserved for crash recovery.
    // convertWalToTsm() erases from memoryStores and calls removeWAL()
    // internally, so we iterate a copy to avoid iterator invalidation.
    auto snapshot = memoryStores;

    for (auto& store : snapshot) {
        if (!store)
            continue;

        if (!store->isEmpty()) {
            // Non-empty store: flush WAL to disk, then convert to TSM.
            try {
                timestar::wal_log.info("[WAL_CLOSE] Flushing memory store {} to TSM on shard {}", store->sequenceNumber,
                                       shardId);
                co_await store->close();          // flush WAL (idempotent)
                co_await convertWalToTsm(store);  // write TSM + erase from memoryStores + removeWAL
                timestar::wal_log.info("[WAL_CLOSE] Successfully flushed store {} to TSM on shard {}",
                                       store->sequenceNumber, shardId);
            } catch (const std::exception& e) {
                timestar::wal_log.error(
                    "[WAL_CLOSE] Failed to flush store {} to TSM on shard {}: {} "
                    "(WAL preserved for recovery on next startup)",
                    store->sequenceNumber, shardId, e.what());
                // WAL file stays on disk — startup recovery will handle it.
            }
        } else {
            // Empty store: just close and remove the WAL file.
            try {
                timestar::wal_log.info("[WAL_CLOSE] Closing empty memory store {} on shard {}", store->sequenceNumber,
                                       shardId);
                co_await store->close();
                co_await store->removeWAL();
            } catch (const std::exception& e) {
                timestar::wal_log.error("[WAL_CLOSE] Error closing empty store {} on shard {}: {}",
                                        store->sequenceNumber, shardId, e.what());
            }
        }
    }

    memoryStores.clear();
    walRetirementStores_.clear();
    timestar::wal_log.info("[WAL_CLOSE] WAL file manager closed on shard {}", shardId);
}

std::optional<TSMValueType> WALFileManager::getSeriesType(const std::string& seriesKey) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    return getSeriesType(seriesId);
}

std::optional<TSMValueType> WALFileManager::getSeriesType(const SeriesId128& seriesId) {
    std::optional<TSMValueType> seriesType;

    for (auto const& memoryStore : memoryStores) {
        seriesType = memoryStore->getSeriesType(seriesId);

        if (seriesType.has_value())
            return seriesType;
    }

    seriesType.reset();
    return seriesType;
}

seastar::future<> WALFileManager::deleteFromMemoryStores(const std::string& seriesKey, uint64_t startTime,
                                                         uint64_t endTime) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

    // Write deletion to WAL and apply to memory stores
    // We always write to WAL to ensure deletions are persisted
    if (!memoryStores.empty() && memoryStores[0]) {
        auto wal = memoryStores[0]->getWAL();
        if (wal) {
            co_await wal->deleteRange(seriesId, startTime, endTime);
            timestar::wal_log.debug("Wrote deleteRange to WAL: series={}, startTime={}, endTime={}", seriesKey,
                                    startTime, endTime);
        }
    }

    // Apply deletion to all memory stores
    // This ensures tombstones are applied even if data arrives later
    for (auto& memStore : memoryStores) {
        memStore->deleteRange(seriesId, startTime, endTime);
    }

    timestar::wal_log.debug("Applied deleteRange to {} memory stores", memoryStores.size());

    co_return;
}

seastar::future<size_t> WALFileManager::deleteVShardFromMemoryStores(uint16_t vshard) {
    if (vshard >= timestar::VIRTUAL_SHARD_COUNT)
        throw std::invalid_argument("WALFileManager::deleteVShardFromMemoryStores: invalid VShard");
    if (memoryStores.empty() || !memoryStores.front() || !memoryStores.front()->getWAL())
        throw std::runtime_error("No active WAL available for VShard generation replacement");

    // Durability precedes visibility: if this append fails, no in-memory state
    // has changed. The retained Raft suffix is applied only after snapshot
    // install returns, so this marker orders strictly between the discarded
    // generation and that suffix on WAL replay.
    co_await memoryStores.front()->getWAL()->deleteVShard(vshard);

    size_t removed = 0;
    for (auto& store : memoryStores) {
        if (store)
            removed += store->deleteVShard(vshard);
    }
    co_return removed;
}

template seastar::future<> WALFileManager::insert<bool>(TimeStarInsert<bool>& insertRequest);
template seastar::future<> WALFileManager::insert<double>(TimeStarInsert<double>& insertRequest);
template seastar::future<> WALFileManager::insert<std::string>(TimeStarInsert<std::string>& insertRequest);
template seastar::future<> WALFileManager::insert<int64_t>(TimeStarInsert<int64_t>& insertRequest);

template seastar::future<> WALFileManager::insertBatch<bool>(std::vector<TimeStarInsert<bool>>& insertRequests);
template seastar::future<> WALFileManager::insertBatch<double>(std::vector<TimeStarInsert<double>>& insertRequests);
template seastar::future<> WALFileManager::insertBatch<std::string>(
    std::vector<TimeStarInsert<std::string>>& insertRequests);
template seastar::future<> WALFileManager::insertBatch<int64_t>(std::vector<TimeStarInsert<int64_t>>& insertRequests);
