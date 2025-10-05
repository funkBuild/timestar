#ifndef __BATCH_POOL_H_INCLUDED__
#define __BATCH_POOL_H_INCLUDED__

#include <vector>
#include <queue>
#include <memory>
#include <cstdint>

// Phase 3.1: Batch pool for reusable timestamp/value buffers
// Reduces allocation overhead during compaction by pre-allocating and reusing buffers

template<typename T>
struct Batch {
    std::vector<uint64_t> timestamps;
    std::vector<T> values;

    Batch(size_t capacity = 10000) {
        timestamps.reserve(capacity);
        values.reserve(capacity);
    }

    void clear() {
        timestamps.clear();
        values.clear();
    }

    size_t capacity() const {
        return timestamps.capacity();
    }

    size_t size() const {
        return timestamps.size();
    }

    bool empty() const {
        return timestamps.empty();
    }
};

template<typename T>
class BatchPool {
private:
    std::queue<std::unique_ptr<Batch<T>>> availableBatches;
    size_t batchCapacity;
    size_t poolSize;
    size_t totalAllocated = 0;

public:
    BatchPool(size_t capacity = 10000, size_t size = 10)
        : batchCapacity(capacity), poolSize(size) {

        // Pre-allocate batches
        for (size_t i = 0; i < poolSize; ++i) {
            availableBatches.push(std::make_unique<Batch<T>>(batchCapacity));
            totalAllocated++;
        }
    }

    // Phase 3.1: Acquire a batch from the pool (or allocate new if empty)
    std::unique_ptr<Batch<T>> acquire() {
        if (!availableBatches.empty()) {
            auto batch = std::move(availableBatches.front());
            availableBatches.pop();
            batch->clear();  // Reset for reuse
            return batch;
        }

        // Pool exhausted, allocate new batch
        totalAllocated++;
        return std::make_unique<Batch<T>>(batchCapacity);
    }

    // Phase 3.1: Release a batch back to the pool
    void release(std::unique_ptr<Batch<T>> batch) {
        if (!batch) {
            return;
        }

        batch->clear();
        availableBatches.push(std::move(batch));
    }

    // Get pool statistics
    size_t available() const {
        return availableBatches.size();
    }

    size_t allocated() const {
        return totalAllocated;
    }

    size_t inUse() const {
        return totalAllocated - availableBatches.size();
    }
};

#endif // __BATCH_POOL_H_INCLUDED__
