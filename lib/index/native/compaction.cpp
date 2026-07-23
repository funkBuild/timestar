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
#include <seastar/util/log.hh>

namespace timestar::index {

static seastar::logger compaction_log("timestar.compaction");

// Adapts a synchronous SSTableReader::Iterator to the async IteratorSource interface.
// Borrows the reader (via its iterator) — doCompaction owns the readers so it
// can close() them after the merge finishes (they were previously leaked open).
class SSTableIteratorSource : public IteratorSource {
public:
    SSTableIteratorSource(std::unique_ptr<SSTableReader::Iterator> iter, int priority)
        : iter_(std::move(iter)), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override { co_await iter_->seek(target); }
    seastar::future<> seekToFirst() override { co_await iter_->seekToFirst(); }
    seastar::future<> next() override { co_await iter_->next(); }

    bool valid() const override { return iter_->valid(); }
    std::string_view key() const override { return iter_->key(); }
    std::string_view value() const override { return iter_->value(); }
    // Return false so tombstones pass through to the compaction writer.
    // Compaction handles tombstone GC directly via sentinel value check.
    bool isTombstone() const override { return false; }
    int priority() const override { return priority_; }

private:
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

    // Tiered policy for L1/L2: merge the entire level into one file at the
    // next level once it accumulates levelThreshold files. Without this the
    // SSTable count grows forever and every kvGet/kvPrefixScan walks all files.
    auto l1Files = manifest_.filesAtLevel(1);
    if (l1Files.size() >= config_.levelThreshold) {
        return CompactionJob{1, std::move(l1Files)};
    }

    auto l2Files = manifest_.filesAtLevel(2);
    if (l2Files.size() >= config_.levelThreshold) {
        // Fold existing L3 output into the job so L3 stays a single file.
        // When no L0/L1 files exist at this moment, the input set is every
        // live file — doCompaction detects that (isFullCompaction) and can
        // finally drop aged tombstones organically.
        auto l3Files = manifest_.filesAtLevel(3);
        for (auto& f : l3Files) {
            l2Files.push_back(std::move(f));
        }
        return CompactionJob{2, std::move(l2Files)};
    }

    return std::nullopt;
}

seastar::future<> CompactionEngine::maybeCompact() {
    // Loop until no compaction is picked: a flush can cascade L0→L1, which
    // pushes L1 over its threshold and triggers L1→L2, and so on. Rate
    // limiting applies inside doCompaction for each job.
    while (true) {
        auto job = pickCompaction();
        if (!job) {
            break;
        }
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

    // Sort inputs by file number (ascending = oldest first). File numbers are
    // allocated monotonically, so a higher number always contains newer
    // versions of a key — this holds even for mixed-level jobs (L2 + L3).
    std::sort(job.inputFiles.begin(), job.inputFiles.end(),
              [](const SSTableMetadata& a, const SSTableMetadata& b) { return a.fileNumber < b.fileNumber; });

    // Open all input SSTables and create iterator sources.
    // Lower priority = newer = wins. Assign decreasing priority so the LAST file
    // (newest, highest file number) gets priority 0 and wins duplicate key resolution.
    // Readers are owned here (not by the sources) so they can be close()d after
    // the merge — previously they were never closed, leaking one fd per input
    // file per compaction.
    std::vector<std::unique_ptr<SSTableReader>> inputReaders;
    std::vector<std::unique_ptr<IteratorSource>> sources;
    std::exception_ptr openError;
    try {
        int priority = static_cast<int>(job.inputFiles.size()) - 1;
        for (const auto& fileMeta : job.inputFiles) {
            auto reader = co_await SSTableReader::open(sstFilename(fileMeta.fileNumber));
            auto iter = reader->newIterator();
            sources.push_back(std::make_unique<SSTableIteratorSource>(std::move(iter), priority--));
            inputReaders.push_back(std::move(reader));
        }
    } catch (...) {
        openError = std::current_exception();
    }
    if (openError) {
        for (auto& reader : inputReaders) {
            co_await reader->close();
        }
        std::rethrow_exception(openError);
    }

    // Create merge iterator
    MergeIterator merger(std::move(sources));
    co_await merger.seekToFirst();  // SSTable iterators are async (DMA I/O)

    if (!merger.valid()) {
        // All inputs were empty — remove from manifest and delete physical files
        for (auto& reader : inputReaders) {
            co_await reader->close();
        }
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
    // A tombstone is ONLY safe to drop when:
    //   1. ALL SSTables are included in this compaction (no non-input files exist
    //      that could still contain the key the tombstone deletes), AND
    //   2. All input files were written more than tombstoneGracePeriodMs ago.
    // Without condition (1), dropping a tombstone during partial (L0-only) compaction
    // allows deleted data in L1+ files to "resurrect."
    bool canDropTombstones = false;
    size_t tombstonesDropped = 0;
    const bool isFullCompaction = (job.inputFiles.size() == manifest_.files().size());
    if (isFullCompaction && config_.tombstoneGracePeriodMs > 0) {
        auto nowNs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        uint64_t graceNs = static_cast<uint64_t>(config_.tombstoneGracePeriodMs) * 1'000'000ULL;
        uint64_t cutoffNs = (nowNs > graceNs) ? nowNs - graceNs : 0;
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
                co_await merger.next();
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
            co_await merger.next();
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

        compaction_log.info("Compaction L{} -> L{}: merged {} files, {} keys written, {} tombstones dropped, {} bytes",
                            job.inputLevel, outputLevel, job.inputFiles.size(), addCount, tombstonesDropped,
                            bytesWritten);
    } catch (...) {
        compactionError = std::current_exception();
    }

    // Close input readers on both success and failure paths (fd leak otherwise).
    // The merger's iterators are not touched after this point.
    for (auto& reader : inputReaders) {
        co_await reader->close();
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
