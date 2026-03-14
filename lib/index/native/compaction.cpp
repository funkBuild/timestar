#include "compaction.hpp"

#include "merge_iterator.hpp"
#include "sstable.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>

#include <algorithm>
#include <filesystem>
#include <memory>

namespace timestar::index {

// Adapts an SSTableReader::Iterator to the IteratorSource interface.
class SSTableIteratorSource : public IteratorSource {
public:
    SSTableIteratorSource(std::unique_ptr<SSTableReader> reader, std::unique_ptr<SSTableReader::Iterator> iter,
                          int priority)
        : reader_(std::move(reader)), iter_(std::move(iter)), priority_(priority) {}

    seastar::future<> seek(std::string_view target) override { co_await iter_->seek(target); }
    seastar::future<> seekToFirst() override { co_await iter_->seekToFirst(); }
    seastar::future<> next() override { co_await iter_->next(); }

    bool valid() const override { return iter_->valid(); }
    std::string_view key() const override { return iter_->key(); }
    std::string_view value() const override { return iter_->value(); }
    bool isTombstone() const override { return false; }  // SSTables don't store tombstones directly
    int priority() const override { return priority_; }

private:
    std::unique_ptr<SSTableReader> reader_;
    std::unique_ptr<SSTableReader::Iterator> iter_;
    int priority_;
};

CompactionEngine::CompactionEngine(std::string dataDir, Manifest& manifest, CompactionConfig config)
    : dataDir_(std::move(dataDir)), manifest_(manifest), config_(config) {}

std::string CompactionEngine::sstFilename(uint64_t fileNumber) {
    char buf[32];
    snprintf(buf, sizeof(buf), "idx_%06lu.sst", fileNumber);
    return dataDir_ + "/" + buf;
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
    if (allFiles.size() <= 1) co_return;

    CompactionJob job;
    job.inputLevel = 0;
    job.inputFiles = std::move(allFiles);
    co_await doCompaction(std::move(job));
}

seastar::future<> CompactionEngine::doCompaction(CompactionJob job) {
    if (job.inputFiles.empty()) co_return;

    int outputLevel = job.inputLevel + 1;

    // Open all input SSTables and create iterator sources
    std::vector<std::unique_ptr<IteratorSource>> sources;
    int priority = 0;
    for (const auto& fileMeta : job.inputFiles) {
        auto reader = co_await SSTableReader::open(sstFilename(fileMeta.fileNumber));
        auto iter = co_await reader->newIterator();
        sources.push_back(
            std::make_unique<SSTableIteratorSource>(std::move(reader), std::move(iter), priority++));
    }

    // Create merge iterator
    MergeIterator merger(std::move(sources));
    co_await merger.seekToFirst();

    if (!merger.valid()) {
        // All inputs were empty — just remove the files
        std::vector<uint64_t> toRemove;
        for (const auto& f : job.inputFiles) toRemove.push_back(f.fileNumber);
        co_await manifest_.removeFiles(toRemove);
        co_return;
    }

    // Write merged output to a new SSTable
    uint64_t outputFileNum = manifest_.nextFileNumber();
    auto outputPath = sstFilename(outputFileNum);
    auto writer = co_await SSTableWriter::create(outputPath, config_.blockSize, config_.bloomBitsPerKey);

    while (merger.valid()) {
        writer.add(merger.key(), merger.value());
        co_await merger.next();
    }

    auto outputMeta = co_await writer.finish();
    outputMeta.fileNumber = outputFileNum;
    outputMeta.level = outputLevel;

    // Update manifest: add output, remove inputs
    co_await manifest_.addFile(outputMeta);

    std::vector<uint64_t> toRemove;
    for (const auto& f : job.inputFiles) {
        toRemove.push_back(f.fileNumber);
    }
    co_await manifest_.removeFiles(toRemove);

    // Delete old SSTable files from disk
    for (const auto& f : job.inputFiles) {
        auto path = sstFilename(f.fileNumber);
        if (std::filesystem::exists(path)) {
            co_await seastar::remove_file(path);
        }
    }
}

}  // namespace timestar::index
