#ifndef LEVELDB_INDEX_H_INCLUDED
#define LEVELDB_INDEX_H_INCLUDED

// Compatibility header: LevelDBIndex is now an alias for NativeIndex.
// All callers continue to work unchanged via this type alias.

#include "native/native_index.hpp"

// CacheSizeEstimator specializations for LRU cache value types
// (used by NativeIndex's internal LRU caches)

namespace timestar {

template <>
struct CacheSizeEstimator<SeriesId128> {
    static size_t estimate(const SeriesId128&) { return sizeof(SeriesId128); }
};

template <>
struct CacheSizeEstimator<SeriesMetadata> {
    static size_t estimate(const SeriesMetadata& m) {
        size_t sz = sizeof(SeriesMetadata);
        sz += m.measurement.capacity();
        sz += m.field.capacity();
        for (const auto& [k, v] : m.tags) {
            sz += 48 + k.capacity() + v.capacity();
        }
        return sz;
    }
};

template <>
struct CacheSizeEstimator<std::shared_ptr<const std::vector<SeriesWithMetadata>>> {
    static size_t estimate(const std::shared_ptr<const std::vector<SeriesWithMetadata>>& ptr) {
        if (!ptr)
            return sizeof(std::shared_ptr<const std::vector<SeriesWithMetadata>>);
        size_t sz = 32 + sizeof(std::vector<SeriesWithMetadata>);
        sz += ptr->capacity() * sizeof(SeriesWithMetadata);
        for (const auto& swm : *ptr) {
            sz += swm.metadata.measurement.capacity();
            sz += swm.metadata.field.capacity();
            for (const auto& [k, v] : swm.metadata.tags) {
                sz += 48 + k.capacity() + v.capacity();
            }
        }
        return sz;
    }
};

}  // namespace timestar

// Type alias: LevelDBIndex -> NativeIndex
// This allows all existing code (Engine, HTTP handlers, query planner, tests)
// to continue using the LevelDBIndex name without modification.
using LevelDBIndex = timestar::index::NativeIndex;

#endif  // LEVELDB_INDEX_H_INCLUDED
