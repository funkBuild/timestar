#include "compaction.hpp"

#include "merge_iterator.hpp"
#include "sstable.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <format>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

namespace timestar::index {

// Adapts a synchronous SSTableReader::Iterator to the async IteratorSource interface.
class SSTableIteratorSource : public IteratorSource {
public:
    SSTableIteratorSource(std::unique_ptr<SSTableReader> reader, std::unique_ptr<SSTableReader::Iterator> iter,
                          int priority)
        : reader_(std::move(reader)), iter_(std::move(iter)), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override {
        iter_->seek(target);
        return seastar::make_ready_future<>();
    }
    seastar::future<> seekToFirst() override {
        iter_->seekToFirst();
        return seastar::make_ready_future<>();
    }
    seastar::future<> next() override {
        iter_->next();
        return seastar::make_ready_future<>();
    }

    bool valid() const override { return iter_->valid(); }
    std::string_view key() const override { return iter_->key(); }
    std::string_view value() const override { return iter_->value(); }
    // Return false so tombstones pass through to the compaction writer.
    // Compaction handles tombstone GC directly via sentinel value check.
    bool isTombstone() const override { return false; }
    int priority() const override { return priority_; }

private:
    std::unique_ptr<SSTableReader> reader_;
    std::unique_ptr<SSTableReader::Iterator> iter_;
    int priority_;
};

CompactionEngine::CompactionEngine(std::string dataDir, Manifest& manifest, CompactionConfig config)
    : dataDir_(std::move(dataDir)), manifest_(manifest), config_(config) {}

std::string CompactionEngine::sstFilename(uint64_t fileNumber) {
    return std::format("{}/idx_{:06}.sst", dataDir_, fileNumber);
}

std::optional<CompactionEngine::CompactionJob> CompactionEngine::pickCompaction() {
    auto l0Files = manifest_.filesAtLevel(0);
    if (l0Files.size() >= config_.level0Threshold) {
        return CompactionJob{0, std::move(l0Files)};
    }
    return std::nullopt;
}

seastar::future<> CompactionEngine::maybeCompact() {
    auto job = pickCompaction();
    if (job) {
        co_await doCompaction(std::move(*job));
    }
}

seastar::future<> CompactionEngine::compactAll() {
    // Merge all files into a single L1 file
    auto allFiles = manifest_.files();
    if (allFiles.size() <= 1)
        co_return;

    CompactionJob job;
    job.inputLevel = 0;
    job.inputFiles = std::move(allFiles);
    co_await doCompaction(std::move(job));
}

seastar::future<> CompactionEngine::doCompaction(CompactionJob job) {
    if (job.inputFiles.empty())
        co_return;

    int outputLevel = job.inputLevel + 1;

    // Open all input SSTables and create iterator sources.
    // Lower priority = newer = wins. Assign decreasing priority so the LAST file
    // (newest, highest file number) gets priority 0 and wins duplicate key resolution.
    std::vector<std::unique_ptr<IteratorSource>> sources;
    int priority = static_cast<int>(job.inputFiles.size()) - 1;
    for (const auto& fileMeta : job.inputFiles) {
        auto reader = co_await SSTableReader::open(sstFilename(fileMeta.fileNumber));
        auto iter = reader->newIterator();
        sources.push_back(std::make_unique<SSTableIteratorSource>(std::move(reader), std::move(iter), priority--));
    }

    // Create merge iterator
    MergeIterator merger(std::move(sources));
    merger.seekToFirstSync();  // SSTable iterators are synchronous

    if (!merger.valid()) {
        // All inputs were empty — remove from manifest and delete physical files
        std::vector<uint64_t> toRemove;
        for (const auto& f : job.inputFiles)
            toRemove.push_back(f.fileNumber);
        co_await manifest_.removeFiles(toRemove);
        for (const auto& f : job.inputFiles) {
            auto path = sstFilename(f.fileNumber);
            if (co_await seastar::file_exists(path)) {
                co_await seastar::remove_file(path);
            }
        }
        co_return;
    }

    // Write merged output to a new SSTable.
    // Use higher zstd compression level for compacted output (L1+) — better ratio, acceptable speed.
    uint64_t outputFileNum = manifest_.nextFileNumber();
    auto outputPath = sstFilename(outputFileNum);
    int compressionLevel = (outputLevel >= 1) ? 3 : 1;
    auto writer =
        co_await SSTableWriter::create(outputPath, config_.blockSize, config_.bloomBitsPerKey, compressionLevel);

    // Tombstone GC: determine if tombstones from these input files can be dropped.
    // A tombstone (empty value) is safe to drop if ALL input SSTables were written
    // more than tombstoneGracePeriodMs ago — no newer data can shadow it.
    bool canDropTombstones = false;
    size_t tombstonesDropped = 0;
    if (config_.tombstoneGracePeriodMs > 0) {
        auto nowNs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        uint64_t cutoffNs = nowNs - config_.tombstoneGracePeriodMs * 1'000'000;
        canDropTombstones = true;
        for (const auto& f : job.inputFiles) {
            if (f.writeTimestamp == 0 || f.writeTimestamp > cutoffNs) {
                canDropTombstones = false;
                break;
            }
        }
    }

    size_t addCount = 0;
    size_t bytesWritten = 0;
    auto rateStart = std::chrono::steady_clock::now();
    const size_t rateLimitBytesPerSec = static_cast<size_t>(config_.rateLimitMBps) * 1024 * 1024;

    // Wrap the merge/write/manifest section so we can clean up the partial
    // output file if any step fails (no co_await in catch blocks in C++20).
    std::exception_ptr compactionError;
    try {
        while (merger.valid()) {
            auto mergedVal = merger.value();
            if (canDropTombstones && mergedVal.size() == 1 && mergedVal[0] == '\0') {
                ++tombstonesDropped;
                merger.nextSync();
                continue;
            }

            bytesWritten += merger.key().size() + merger.value().size();
            writer.add(merger.key(), merger.value());
            if (++addCount % 1024 == 0) {
                co_await writer.flushPending();

                if (rateLimitBytesPerSec > 0) {
                    auto elapsed = std::chrono::steady_clock::now() - rateStart;
                    auto elapsedSec = std::chrono::duration<double>(elapsed).count();
                    if (elapsedSec > 0) {
                        double currentRate = static_cast<double>(bytesWritten) / elapsedSec;
                        if (currentRate > static_cast<double>(rateLimitBytesPerSec)) {
                            double targetSec =
                                static_cast<double>(bytesWritten) / static_cast<double>(rateLimitBytesPerSec);
                            double sleepSec = targetSec - elapsedSec;
                            if (sleepSec > 0.001) {
                                co_await seastar::sleep(
                                    std::chrono::microseconds(static_cast<int64_t>(sleepSec * 1'000'000)));
                            }
                        }
                    }
                }
            }
            merger.nextSync();
        }
        co_await writer.flushPending();

        auto outputMeta = co_await writer.finish();
        outputMeta.fileNumber = outputFileNum;
        outputMeta.level = outputLevel;

        std::vector<uint64_t> toRemove;
        for (const auto& f : job.inputFiles) {
            toRemove.push_back(f.fileNumber);
        }
        co_await manifest_.atomicReplaceFiles(outputMeta, toRemove);

        for (const auto& f : job.inputFiles) {
            auto path = sstFilename(f.fileNumber);
            if (co_await seastar::file_exists(path)) {
                co_await seastar::remove_file(path);
            }
        }
    } catch (...) {
        compactionError = std::current_exception();
    }

    // Clean up partial output file on failure
    if (compactionError) {
        if (co_await seastar::file_exists(outputPath)) {
            co_await seastar::remove_file(outputPath);
        }
        std::rethrow_exception(compactionError);
    }
}

}  // namespace timestar::index
