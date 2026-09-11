/*
 * Test-only seam into NativeIndex's private state.
 *
 * Lives in namespace timestar::index because that is where NativeIndex declares
 * `friend struct NativeIndexTestAccess`. It is a header (rather than a struct
 * defined in one test .cpp) so several test TUs can share ONE definition — two
 * TUs each defining their own version of this struct would be an ODR violation.
 *
 * Everything here plants a state that a normal write path cannot produce, so a
 * test can start from a disk image a buggy or crashed server would have left.
 */

#pragma once

#include "../../lib/index/key_encoding.hpp"
#include "../../lib/index/native/bloom_filter.hpp"
#include "../../lib/index/native/native_index.hpp"

#include <endian.h>

#include <cstring>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <string>
#include <vector>

namespace timestar::index {

struct NativeIndexTestAccess {
    // Exercise the completion/resumption gap: a caller may not consume a ready
    // future until after cache entries have moved or been evicted.
    static seastar::future<> visitBitmap(NativeIndex& index, const std::string& key, bool day,
                                        seastar::noncopyable_function<void(const roaring::Roaring*)> consume) {
        return day ? index.withDayBitmap(key, std::move(consume))
                   : index.withPostingsBitmap(key, std::move(consume));
    }

    static seastar::future<> updateBitmap(NativeIndex& index, std::string& key, bool day, uint32_t id) {
        auto update = [id](roaring::Roaring& bitmap) { bitmap.add(id); };
        return day ? index.withDayBitmapForInsert(key, update) : index.withBitmapForInsert(key, update);
    }

    static seastar::future<bool> addDayMembership(NativeIndex& index, std::string& key, uint32_t id) {
        return index.addDayMembership(key, id);
    }

    static void plantBitmap(NativeIndex& index, const std::string& key, bool day, uint32_t id) {
        auto& entry = day ? index.dayBitmapCache_[key] : index.bitmapCache_[key];
        entry.bitmap.add(id);
    }

    static void rehashBitmaps(NativeIndex& index) {
        index.bitmapCache_.reserve(index.bitmapCache_.bucket_count() * 4 + 100);
        index.dayBitmapCache_.reserve(index.dayBitmapCache_.bucket_count() * 4 + 100);
    }

    static void clearBitmaps(NativeIndex& index) {
        index.bitmapCache_.clear();
        index.dayBitmapCache_.clear();
        index.bitmapCacheDirtyKeys_.clear();
        index.dayBitmapCacheDirtyKeys_.clear();
    }

    static double cachedSketchEstimate(const NativeIndex& index, const std::string& key) {
        auto it = index.hllCache_.find(key);
        return it == index.hllCache_.end() ? 0.0 : it->second.estimate();
    }

    static void clearBlockCache(NativeIndex& index) { index.blockCache_ = BlockCache(index.blockCache_.maxBytes()); }

    static seastar::future<> setIndexWalReadOnly(NativeIndex& index, bool readOnly) {
        return setWalReadOnly(*index.wal_, readOnly);
    }

    // Leave less than one series-creation batch before the automatic WAL flush.
    static seastar::future<> fillIndexWalNearThreshold(NativeIndex& index) {
        IndexWriteBatch batch;
        batch.put("padding", std::string(1024 * 1024 - 64, 'p'));
        co_await index.wal_->append(batch);
    }

    static std::string nextSstablePath(NativeIndex& index) {
        return index.sstFilename(index.manifest_->currentFileNumber());
    }

    static seastar::future<> stagePostingsCheckpoint(NativeIndex& index) {
        auto key = keys::encodePostingsWatermarkKey();
        auto value = co_await index.kvGet(key);
        co_await index.kvPut(key, value.value_or(keys::encodeLocalId(0)));
    }

    static seastar::future<> triggerBackgroundFlush(NativeIndex& index) { return index.maybeFlushMemTable(); }

    static seastar::future<> waitForBackgroundResult(NativeIndex& index) {
        if (index.flushFuture_)
            co_await index.flushFuture_->get_future();
    }

    static uint32_t reserveUncommittedLocalId(NativeIndex& index, SeriesId128 id) {
        return index.localIdMap_.getOrAssign(id);
    }

    static bool dayPruningDisabled(const NativeIndex& index) {
        return index.dayBitmapCoverage_ && index.dayBitmapCoverage_->first > index.dayBitmapCoverage_->second;
    }

    // Used only after sync(): keep durable metadata, but discard all derived
    // RAM state as a crash would. Clearing just postings while leaving dirty
    // HLL/bloom caches would make close() perform an artificial new checkpoint.
    static void discardAllVolatileDerivedState(NativeIndex& index) {
        discardVolatilePostings(index);
        index.dayBitmapCache_.clear();
        index.dayBitmapCacheDirtyKeys_.clear();
        index.hllCache_.clear();
        index.hllCacheDirty_.clear();
        index.dirtyMeasurementBlooms_.clear();
    }

    // Model a legacy missing forward mapping while retaining recoverable metadata.
    static seastar::future<> dropLocalIdMapping(NativeIndex& index, SeriesId128 id) {
        const auto lost = *index.localIdMap_.getLocalId(id);
        LocalIdMap restored;
        restored.restoreBegin(index.localIdMap_.nextId(), index.localIdMap_.nextId());
        for (uint32_t n = 0; n < index.localIdMap_.nextId(); ++n) {
            if (n != lost && index.localIdMap_.isValid(n))
                (void)restored.restoreEntry(n, index.localIdMap_.getGlobalId(n));
        }
        index.localIdMap_ = std::move(restored);
        co_await index.kvDelete(keys::encodeLocalIdForwardKey(lost));
    }

    static seastar::future<seastar::semaphore_units<>> holdIndexFlush(NativeIndex& index) {
        return seastar::get_units(index.flushMutex_, 1);
    }

    static seastar::future<> flushWalBlocks(IndexWAL& wal) {
        auto units = co_await seastar::get_units(*wal.writeSem_, 1);
        co_await wal.flushBuffer();
    }

    static seastar::future<> setWalReadOnly(IndexWAL& wal, bool readOnly) {
        co_await wal.walFile_->close();
        wal.walFile_.emplace(co_await seastar::open_file_dma(
            wal.currentPath_, readOnly ? seastar::open_flags::ro : seastar::open_flags::rw));
    }

    static uint64_t walSequence(const NativeIndex& index) { return index.wal_->sequenceNumber(); }

    static seastar::future<> plantLegacyPostingsCheckpoint(NativeIndex& index) {
        co_await index.kvPut(keys::encodePostingsWatermarkKey(), keys::encodeLocalId(index.localIdMap_.nextId()));
        co_await index.kvDelete(std::string(1, static_cast<char>(POSTINGS_REPAIR_GENERATION)));
    }

    // Read independently recoverable state, excluding the live memtable and
    // all derived caches. Do not close or sync the index under test.
    static seastar::future<std::optional<std::string>> durableValue(NativeIndex& index, const std::string& key) {
        auto wal = co_await IndexWAL::open(index.indexPath_ + "/wal");
        MemTable replayed;
        co_await wal.replay(replayed);
        if (replayed.isTombstone(key))
            co_return std::nullopt;
        if (auto val = replayed.get(key))
            co_return std::string(*val);
        for (auto it = index.sstableReaders_.rbegin(); it != index.sstableReaders_.rend(); ++it) {
            auto val = co_await it->second->get(key);
            if (val) {
                if (*val == std::string(1, '\0'))
                    co_return std::nullopt;
                co_return val;
            }
        }
        co_return std::nullopt;
    }

    static void cancelTimers(NativeIndex& index) {
        index.walSyncTimer_.cancel();
        index.dayBitmapFlushTimer_.cancel();
    }

    // Model a suspended cold load plus a concurrent insert's placeholder add.
    static void holdPostingsLoad(NativeIndex& index, const std::string& measurement, const std::string& tagKey,
                                 const std::string& tagValue, SeriesId128 concurrentSeries) {
        std::string key;
        index.buildBitmapCacheKey(key, measurement, tagKey, tagValue);
        auto& entry = index.bitmapCache_[key];
        entry.bitmap = roaring::Roaring();
        entry.bitmap.add(*index.localIdMap_.getLocalId(concurrentSeries));
        entry.loading = true;
        entry.dirty = true;
        index.bitmapCacheDirtyKeys_.insert(key);
    }

    static void discardVolatilePostings(NativeIndex& index) {
        index.bitmapCache_.clear();
        index.bitmapCacheDirtyKeys_.clear();
        index.suppressCleanShutdownMarker();
    }

    // Flip twice to restore the original bytes after a real CRC read failure.
    static void flipSstableDataByte(NativeIndex& index) {
        auto path = index.sstFilename(index.sstableReaders_.begin()->first);
        std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
        file.exceptions(std::ios::failbit | std::ios::badbit);
        char byte = 0;
        file.get(byte);
        file.seekp(0);
        file.put(byte ^ 1);
        file.flush();
        index.blockCache_ = BlockCache(index.blockCache_.maxBytes());
    }

    // Plants a persisted measurement bloom that omits some postings keys — what a
    // <= 1.4.0 server could leave on disk — bypassing every normal write path.
    static seastar::future<> plantStaleBloom(NativeIndex& index, const std::string& measurement,
                                             const std::vector<std::string>& postingsKeysToKeep) {
        BloomFilter bloom(10);
        for (const auto& key : postingsKeysToKeep) {
            bloom.addKey(key);
        }
        bloom.build();
        std::string serialized;
        bloom.serializeTo(serialized);
        co_await index.kvPut(keys::encodeMeasurementBloomKey(measurement), serialized);
        index.measurementBloomCache_.erase(measurement);
    }

    // Models an UNCLEAN shutdown for day bitmaps: day membership recorded for
    // days >= fromDay never reached disk (it only ever lived in dayBitmapCache_,
    // which is persisted solely when the index memtable crosses
    // write_buffer_size), so it is gone. Series metadata and LocalIds are NOT
    // touched — those are persisted with each series-creation batch, which is
    // exactly why the two can disagree after a crash.
    // Drop day membership for [fromDay, toDay] WITHOUT touching the watermark —
    // the shape a crash leaves when the lost days are older than the newest day
    // recorded (a device uploading buffered points, say). The watermark keeps
    // claiming those days are durable, which is exactly what the repair must
    // not trust.
    static seastar::future<> dropDayBitmapsInRange(NativeIndex& index, const std::string& measurement, uint32_t fromDay,
                                                   uint32_t toDay) {
        namespace ke = timestar::index::keys;

        IndexWriteBatch batch;
        co_await index.kvPrefixScan(ke::encodeDayBitmapPrefix(measurement),
                                    [&](std::string_view key, std::string_view) {
                                        const uint32_t day = ke::decodeDayFromDayBitmapKey(key);
                                        if (day >= fromDay && day <= toDay) {
                                            batch.remove(std::string(key));
                                        }
                                        return true;
                                    });
        if (!batch.empty()) {
            co_await index.kvWriteBatch(batch);
        }

        std::string measPrefix = measurement;
        measPrefix.push_back('\0');
        std::vector<std::string> toEvict;
        for (const auto& [k, entry] : index.dayBitmapCache_) {
            if (k.size() != measPrefix.size() + 4 || k.compare(0, measPrefix.size(), measPrefix) != 0) {
                continue;
            }
            uint32_t dayBE;
            std::memcpy(&dayBE, k.data() + k.size() - 4, 4);
            const uint32_t day = be32toh(dayBE);
            if (day >= fromDay && day <= toDay) {
                toEvict.push_back(k);
            }
        }
        for (const auto& k : toEvict) {
            index.dayBitmapCache_.erase(k);
            index.dayBitmapCacheDirtyKeys_.erase(k);
        }
    }

    static seastar::future<> dropDayBitmapsFrom(NativeIndex& index, const std::string& measurement, uint32_t fromDay) {
        namespace ke = timestar::index::keys;

        IndexWriteBatch batch;
        co_await index.kvPrefixScan(ke::encodeDayBitmapPrefix(measurement),
                                    [&](std::string_view key, std::string_view) {
                                        if (ke::decodeDayFromDayBitmapKey(key) >= fromDay) {
                                            batch.remove(std::string(key));
                                        }
                                        return true;
                                    });
        if (!batch.empty()) {
            co_await index.kvWriteBatch(batch);
        }

        // Cache key format: "measurement\0day(4B big-endian)".
        std::string measPrefix = measurement;
        measPrefix.push_back('\0');
        std::vector<std::string> toEvict;
        for (const auto& [k, entry] : index.dayBitmapCache_) {
            if (k.size() != measPrefix.size() + 4 || k.compare(0, measPrefix.size(), measPrefix) != 0) {
                continue;
            }
            uint32_t dayBE;
            std::memcpy(&dayBE, k.data() + k.size() - 4, 4);
            if (be32toh(dayBE) >= fromDay) {
                toEvict.push_back(k);
            }
        }
        for (const auto& k : toEvict) {
            index.dayBitmapCache_.erase(k);
            index.dayBitmapCacheDirtyKeys_.erase(k);
        }

        // Roll the durability watermark back to just before the lost region, as
        // a crash would leave it: the last flush happened at fromDay-1, and
        // everything recorded after it died with the process. Without this the
        // watermark would still claim the dropped days are durable and the
        // startup repair would skip exactly the days it needs to rebuild.
        const uint32_t lastDurableDay = fromDay > 0 ? fromDay - 1 : 0;
        index.maxRecordedDay_ = lastDurableDay;
        index.dayBitmapWatermark_ = lastDurableDay;
        co_await index.kvPut(ke::encodeDayBitmapWatermarkKey(), ke::encodeDay(lastDurableDay));
    }

    // Make the coming close() look like a process that died: no clean-shutdown
    // marker, so the next open() repairs. A test cannot kill the process, and
    // without this every test that shuts down tidily would skip the repair it
    // means to exercise.
    static void simulateUncleanShutdown(NativeIndex& index) { index.suppressCleanShutdownMarker(); }

    // Leave behind a clean-shutdown marker as an OLDER build would have written
    // it. Pair with simulateUncleanShutdown() on the same index so close() does
    // not overwrite it with the current generation.
    static seastar::future<> plantCleanShutdownMarker(NativeIndex& index, const std::string& value) {
        co_await index.kvPut(keys::encodeCleanShutdownKey(), value);
    }

    // True when a day bitmap for (measurement, day) is durable — the state a
    // recovery pass has to restore. Deliberately reads the KV store only, not
    // the cache.
    static seastar::future<bool> hasPersistedDayBitmap(NativeIndex& index, const std::string& measurement,
                                                       uint32_t day) {
        namespace ke = timestar::index::keys;
        std::string key = ke::encodeDayBitmapPrefix(measurement);
        uint32_t dayBE = htobe32(day);
        key.append(reinterpret_cast<const char*>(&dayBE), 4);
        auto val = co_await index.kvGet(key);
        co_return val.has_value();
    }
};

}  // namespace timestar::index
