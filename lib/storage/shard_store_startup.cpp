#include "shard_store_startup.hpp"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <format>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace timestar {
namespace {

// State of the shard_count.meta control file. Malformed is kept distinct from
// Absent so a corrupted metadata file over real shard data can fail closed
// rather than be silently read as a fresh store.
enum class MetaState { Absent, Valid, Malformed };

struct MetaRead {
    MetaState state = MetaState::Absent;
    unsigned value = 0;
};

MetaRead readShardCountMeta(const fs::path& path) {
    std::ifstream ifs(path);
    if (!ifs)
        return {MetaState::Absent, 0};
    unsigned count = 0;
    if (!(ifs >> count))
        return {MetaState::Malformed, 0};
    return {MetaState::Valid, count};
}

struct RootScan {
    std::vector<unsigned> shardIndices;
    std::optional<std::string> stagingArtifact;  // shard_N_new / shard_N_old left by an interrupted rebalance
};

RootScan scanRoot(const StorageLayout& layout) {
    RootScan scan;
    DIR* dir = ::opendir(layout.root().c_str());
    if (dir == nullptr)
        return scan;  // A missing root is an empty (fresh) store.

    while (const dirent* entry = ::readdir(dir)) {
        const std::string name = entry->d_name;
        if (name == "." || name == "..")
            continue;

        if (const auto shard = layout.parseShardDirName(name)) {
            struct stat st{};
            if (::stat((layout.root() / name).c_str(), &st) == 0 && S_ISDIR(st.st_mode))
                scan.shardIndices.push_back(*shard);
        } else if (name.starts_with("shard_") && (name.ends_with("_new") || name.ends_with("_old"))) {
            scan.stagingArtifact = name;
        }
    }
    ::closedir(dir);
    return scan;
}

// A contiguous shard_0..shard_{k-1} set is a store of k shards. Any gap or
// duplicate means the shard directories cannot be trusted to describe a count.
std::optional<unsigned> inferContiguousShardCount(std::vector<unsigned> indices) {
    if (indices.empty())
        return 0u;
    std::sort(indices.begin(), indices.end());
    for (unsigned i = 0; i < indices.size(); ++i) {
        if (indices[i] != i)
            return std::nullopt;
    }
    return static_cast<unsigned>(indices.size());
}

void atomicallyWriteShardCount(const StorageLayout& layout, unsigned count) {
    const auto file = layout.shardCountMetadataFile();
    const auto tmp = layout.shardCountMetadataTemporaryFile();
    {
        std::ofstream ofs(tmp, std::ios::trunc);
        if (!ofs)
            throw std::runtime_error("Failed to write " + tmp.string());
        ofs << count << "\n";
        ofs.flush();
        if (!ofs)
            throw std::runtime_error("Failed to write " + tmp.string());
    }
    if (const int fd = ::open(tmp.c_str(), O_RDONLY); fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
    fs::rename(tmp, file);
    if (const auto parent = file.parent_path(); !parent.empty()) {
        if (const int dirFd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY); dirFd >= 0) {
            ::fsync(dirFd);
            ::close(dirFd);
        }
    }
}

}  // namespace

ShardStoreStartup::ShardStoreStartup(StorageLayout layout) : layout_(std::move(layout)) {}

ShardStoreInspection ShardStoreStartup::inspect(unsigned requestedShardCount) const {
    ShardStoreInspection result;
    result.requestedShardCount = requestedShardCount;

    // 1. An interrupted rebalance (state file or leftover staging directory) is
    //    never resumed automatically by normal startup.
    if (std::ifstream(layout_.rebalanceStateFile())) {
        result.status = ShardStoreStartupStatus::InterruptedLegacyRebalance;
        result.detail = "found " + layout_.rebalanceStateFile().string();
        return result;
    }

    const RootScan scan = scanRoot(layout_);
    if (scan.stagingArtifact) {
        result.status = ShardStoreStartupStatus::InterruptedLegacyRebalance;
        result.detail = "found leftover rebalance staging directory " + *scan.stagingArtifact;
        return result;
    }

    MetaRead meta = readShardCountMeta(layout_.shardCountMetadataFile());

    // A corrupt metadata file over real shard data is unusable; with no shard
    // data it is indistinguishable from a fresh store and treated as absent.
    if (meta.state == MetaState::Malformed) {
        if (!scan.shardIndices.empty()) {
            result.status = ShardStoreStartupStatus::InvalidMetadata;
            result.detail = "shard_count.meta is present but unreadable while shard data exists";
            return result;
        }
        meta.state = MetaState::Absent;
    }

    // 2. A recorded, non-zero previous count is authoritative -- but only if the
    //    shard directories on disk actually back it. A committed store always
    //    has exactly shard_0..shard_{N-1}; a meta that disagrees with the
    //    directory set (fewer/extra/gappy dirs, or none at all) is a corrupt or
    //    partially-created store, not a clean count change, and must fail closed
    //    rather than let Engine create fresh shards beside populated ones.
    if (meta.state == MetaState::Valid && meta.value != 0) {
        result.previousShardCount = meta.value;
        const auto inferred = inferContiguousShardCount(scan.shardIndices);
        if (!inferred || *inferred != meta.value) {
            result.status = ShardStoreStartupStatus::InvalidMetadata;
            result.detail = "shard_count.meta records " + std::to_string(meta.value) +
                            " shards but the shard directories on disk are not a matching shard_0..shard_N-1 set";
            return result;
        }
        result.status = (meta.value == requestedShardCount) ? ShardStoreStartupStatus::MatchingShardCount
                                                            : ShardStoreStartupStatus::UnsafeShardCountChange;
        return result;
    }

    // 3. No recorded count. A truly empty root is a fresh store.
    if (scan.shardIndices.empty()) {
        result.status = ShardStoreStartupStatus::FreshStore;
        return result;
    }

    // 4. Shard data with no recorded count: infer the count from the directory
    //    set so a mismatch still fails closed rather than being migrated blind.
    const auto inferred = inferContiguousShardCount(scan.shardIndices);
    if (!inferred || *inferred == 0) {
        result.status = ShardStoreStartupStatus::InvalidMetadata;
        result.detail = "shard directories are not a contiguous shard_0..shard_N-1 set";
        return result;
    }
    result.previousShardCount = *inferred;
    result.status = (*inferred == requestedShardCount) ? ShardStoreStartupStatus::MatchingShardCount
                                                       : ShardStoreStartupStatus::UnsafeShardCountChange;
    return result;
}

std::string ShardStoreStartup::failureMessage(const ShardStoreInspection& inspection) const {
    const std::string root = layout_.root().string();
    switch (inspection.status) {
        case ShardStoreStartupStatus::UnsafeShardCountChange:
            return std::format(
                "Refusing to start: data root '{}' holds a {}-shard store but the server was started with {} shards "
                "(--smp/core count). Changing the core count of an existing store would run the legacy rebalancer, "
                "which can make already-ingested series undiscoverable. Restart with {} shards to reopen this store "
                "unchanged, or move the data root aside to start fresh. A metadata-safe migration is not yet "
                "available (see docs/clustering-starting-point.md).",
                root, inspection.previousShardCount, inspection.requestedShardCount, inspection.previousShardCount);
        case ShardStoreStartupStatus::InterruptedLegacyRebalance:
            return std::format(
                "Refusing to start: data root '{}' contains an interrupted legacy rebalance ({}). Normal startup does "
                "not resume it. Recover with an explicit offline tool or restore the data root from backup.",
                root, inspection.detail);
        case ShardStoreStartupStatus::InvalidMetadata:
            return std::format(
                "Refusing to start: data root '{}' has unusable shard metadata ({}). Inspect the root and, once its "
                "state is understood, correct or remove the offending artifact before restarting. Requested shard "
                "count: {}.",
                root, inspection.detail, inspection.requestedShardCount);
        case ShardStoreStartupStatus::FreshStore:
        case ShardStoreStartupStatus::MatchingShardCount:
            return {};
    }
    return {};
}

void ShardStoreStartup::commitAfterInitialization(const ShardStoreInspection& inspection) const {
    if (!inspection.canStart())
        throw std::logic_error("commitAfterInitialization called for a refused startup inspection");
    // Always record the requested count: a no-op for a matching store, and the
    // authoritative record for a fresh one. Mirrors the pre-existing behaviour
    // of writing shard_count.meta on every successful startup.
    atomicallyWriteShardCount(layout_, inspection.requestedShardCount);
}

ShardStoreStartupSession::ShardStoreStartupSession(StorageLayout layout, unsigned requestedShardCount)
    : startup_(std::move(layout)), inspection_(startup_.inspect(requestedShardCount)) {}

std::string ShardStoreStartupSession::failureMessage() const {
    return startup_.failureMessage(inspection_);
}

void ShardStoreStartupSession::commitEngineInitialization() {
    if (committed_)
        return;
    startup_.commitAfterInitialization(inspection_);
    committed_ = true;
}

}  // namespace timestar
