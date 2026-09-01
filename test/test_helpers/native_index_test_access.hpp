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

#include "../../lib/index/native/bloom_filter.hpp"
#include "../../lib/index/native/native_index.hpp"
#include "../../lib/index/key_encoding.hpp"

#include <endian.h>

#include <cstring>
#include <seastar/core/coroutine.hh>
#include <string>
#include <vector>

namespace timestar::index {

struct NativeIndexTestAccess {
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
    static seastar::future<> dropDayBitmapsFrom(NativeIndex& index, const std::string& measurement,
                                                uint32_t fromDay) {
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
