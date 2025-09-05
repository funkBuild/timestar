#ifndef __TSM_READER_H_INCLUDED__
#define __TSM_READER_H_INCLUDED__

#include "tsm.hpp"
#include <seastar/core/shared_ptr.hh>

// RAII wrapper for TSM file access with automatic reference counting
// This ensures reads are not blocked during compaction
class TSMReader {
private:
    seastar::shared_ptr<TSM> tsm;
    
public:
    explicit TSMReader(seastar::shared_ptr<TSM> file) : tsm(file) {
        if (tsm) {
            tsm->addRef();
        }
    }
    
    ~TSMReader() {
        if (tsm) {
            tsm->releaseRef();
        }
    }
    
    // Delete copy constructor and assignment
    TSMReader(const TSMReader&) = delete;
    TSMReader& operator=(const TSMReader&) = delete;
    
    // Move constructor
    TSMReader(TSMReader&& other) noexcept : tsm(std::move(other.tsm)) {
        other.tsm = nullptr;
    }
    
    // Move assignment
    TSMReader& operator=(TSMReader&& other) noexcept {
        if (this != &other) {
            if (tsm) {
                tsm->releaseRef();
            }
            tsm = std::move(other.tsm);
            other.tsm = nullptr;
        }
        return *this;
    }
    
    // Access the underlying TSM file
    TSM* operator->() const { return tsm.get(); }
    TSM& operator*() const { return *tsm; }
    
    // Check if valid
    explicit operator bool() const { return tsm != nullptr; }
    
    // Get the underlying pointer
    TSM* get() const { return tsm.get(); }
    
    // Read series with automatic reference management
    template <class T>
    seastar::future<TSMResult<T>> readSeries(const std::string& seriesKey, 
                                             uint64_t startTime, 
                                             uint64_t endTime) {
        TSMResult<T> results(tsm->rankAsInteger());
        co_await tsm->readSeries<T>(seriesKey, startTime, endTime, results);
        co_return results;
    }
};

// Helper to create a reader
inline TSMReader makeTSMReader(seastar::shared_ptr<TSM> file) {
    return TSMReader(file);
}

#endif // __TSM_READER_H_INCLUDED__