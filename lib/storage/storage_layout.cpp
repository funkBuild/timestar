#include "storage_layout.hpp"

#include "../core/vshard.hpp"

#include <charconv>
#include <format>
#include <stdexcept>
#include <string>
#include <utility>

namespace fs = std::filesystem;

namespace timestar {
namespace {

constexpr std::string_view shardDirectoryPrefix = "shard_";
constexpr std::string_view effectiveVShardOwnershipRevisionPrefix = "owners.";
constexpr std::string_view effectiveVShardOwnershipRevisionSuffix = ".bin";

}  // namespace

StorageLayout::StorageLayout(fs::path root) : root_(normalizeRoot(std::move(root))) {}

StorageLayout StorageLayout::anchored() const {
    if (root_.is_absolute())
        return *this;
    return StorageLayout(fs::absolute(root_));
}

fs::path StorageLayout::normalizeRoot(fs::path root) {
    if (root.empty())
        throw std::invalid_argument("StorageLayout root must not be empty");
    if (root.native().find(fs::path::value_type{}) != fs::path::string_type::npos)
        throw std::invalid_argument("StorageLayout root must not contain NUL");

    auto normalized = root.lexically_normal();
    while (normalized != normalized.root_path() && normalized.filename().empty())
        normalized = normalized.parent_path();
    return normalized;
}

fs::path StorageLayout::underRoot(const fs::path& relative) const {
    if (root_ == fs::path{"."})
        return relative;
    return root_ / relative;
}

fs::path StorageLayout::withSuffix(const fs::path& path, const char* suffix) {
    auto result = path;
    result += suffix;
    return result;
}

fs::path StorageLayout::withExtension(const fs::path& path, const char* extension) {
    auto result = path;
    result.replace_extension(extension);
    return result;
}

fs::path StorageLayout::requireTsmFilename(const fs::path& filename) {
    if (filename.empty() || filename.native().find(fs::path::value_type{}) != fs::path::string_type::npos ||
        filename.has_parent_path() || filename == "." || filename == ".." || filename.extension() != ".tsm") {
        throw std::invalid_argument("staged TSM filename must be one .tsm basename");
    }
    return filename;
}

std::string StorageLayout::requireFilenameSegment(std::string_view segment, std::string_view field) {
    const auto value = fs::path(segment);
    if (segment.empty() || segment.find('\0') != std::string_view::npos || value.has_parent_path() || value == "." ||
        value == "..") {
        throw std::invalid_argument(std::string(field) + " must be one non-empty filename segment");
    }
    return std::string(segment);
}

fs::path StorageLayout::shardDir(unsigned shard) const {
    return underRoot(std::string(shardDirectoryPrefix) + std::to_string(shard));
}

std::optional<unsigned> StorageLayout::parseShardDirName(std::string_view name) const {
    if (!isShardNamespaceEntry(name))
        return std::nullopt;

    const auto numeric = name.substr(shardDirectoryPrefix.size());
    unsigned shard = 0;
    const auto result = std::from_chars(numeric.data(), numeric.data() + numeric.size(), shard);
    if (numeric.empty() || result.ec != std::errc{} || result.ptr != numeric.data() + numeric.size() ||
        name != shardDir(shard).filename().string()) {
        return std::nullopt;
    }
    return shard;
}

bool StorageLayout::isShardNamespaceEntry(std::string_view name) const noexcept {
    return name.starts_with(shardDirectoryPrefix);
}

fs::path StorageLayout::shardStagingDir(unsigned shard) const {
    return underRoot("shard_" + std::to_string(shard) + "_new");
}

fs::path StorageLayout::shardStagingTsmDir(unsigned shard) const {
    return shardStagingDir(shard) / "tsm";
}

fs::path StorageLayout::shardStagingNativeIndexDir(unsigned shard) const {
    return shardStagingDir(shard) / "native_index";
}

fs::path StorageLayout::shardRetiredDir(unsigned shard) const {
    return underRoot("shard_" + std::to_string(shard) + "_old");
}

fs::path StorageLayout::walFile(unsigned shard, uint64_t sequence) const {
    return shardDir(shard) / std::format("{:010}.wal", sequence);
}

fs::path StorageLayout::tsmDir(unsigned shard) const {
    return shardDir(shard) / "tsm";
}

fs::path StorageLayout::tsmFile(unsigned shard, const fs::path& filename) const {
    return tsmDir(shard) / requireTsmFilename(filename);
}

fs::path StorageLayout::tsmTombstoneFile(unsigned shard, const fs::path& tsmFilename) const {
    return withExtension(tsmFile(shard, tsmFilename), ".tombstone");
}

fs::path StorageLayout::tsmFile(unsigned shard, uint64_t tier, uint64_t sequence) const {
    return tsmDir(shard) / (std::to_string(tier) + "_" + std::to_string(sequence) + ".tsm");
}

fs::path StorageLayout::tsmTemporaryFile(unsigned shard, uint64_t tier, uint64_t sequence) const {
    return withSuffix(tsmFile(shard, tier, sequence), ".tmp");
}

fs::path StorageLayout::tsmTombstoneFile(unsigned shard, uint64_t tier, uint64_t sequence) const {
    return withExtension(tsmFile(shard, tier, sequence), ".tombstone");
}

fs::path StorageLayout::compactedTsmFile(unsigned shard, uint64_t tier, uint64_t sequence,
                                         uint64_t dataSequence) const {
    return tsmDir(shard) /
           (std::to_string(tier) + "_" + std::to_string(sequence) + "_d" + std::to_string(dataSequence) + ".tsm");
}

fs::path StorageLayout::compactedTsmTemporaryFile(unsigned shard, uint64_t tier, uint64_t sequence,
                                                  uint64_t dataSequence) const {
    return withSuffix(compactedTsmFile(shard, tier, sequence, dataSequence), ".tmp");
}

fs::path StorageLayout::compactedTsmTombstoneFile(unsigned shard, uint64_t tier, uint64_t sequence,
                                                  uint64_t dataSequence) const {
    return withExtension(compactedTsmFile(shard, tier, sequence, dataSequence), ".tombstone");
}

fs::path StorageLayout::shardStagingTsmFile(unsigned shard, const fs::path& filename) const {
    return shardStagingTsmDir(shard) / requireTsmFilename(filename);
}

fs::path StorageLayout::shardStagingTombstoneFile(unsigned shard, const fs::path& tsmFilename) const {
    return withExtension(shardStagingTsmFile(shard, tsmFilename), ".tombstone");
}

fs::path StorageLayout::rebalanceWalTsmFile(unsigned targetShard, unsigned sourceShard,
                                            std::string_view walStem) const {
    return shardStagingTsmDir(targetShard) /
           ("0_wal_" + std::to_string(sourceShard) + "_" + requireFilenameSegment(walStem, "WAL stem") + ".tsm");
}

fs::path StorageLayout::rebalanceCollisionTsmFile(unsigned targetShard, uint64_t sequence) const {
    return shardStagingTsmDir(targetShard) / ("0_rebal_" + std::to_string(sequence) + ".tsm");
}

fs::path StorageLayout::rebalanceSplitTsmFile(unsigned targetShard, uint64_t sequence) const {
    return shardStagingTsmDir(targetShard) / ("0_split_" + std::to_string(sequence) + ".tsm");
}

fs::path StorageLayout::nativeIndexDir(unsigned shard) const {
    return shardDir(shard) / "native_index";
}

fs::path StorageLayout::nativeManifestFile(unsigned shard) const {
    return nativeIndexDir(shard) / "MANIFEST";
}

fs::path StorageLayout::nativeManifestTemporaryFile(unsigned shard) const {
    return withSuffix(nativeManifestFile(shard), ".tmp");
}

fs::path StorageLayout::nativeWalDir(unsigned shard) const {
    return nativeIndexDir(shard) / "wal";
}

fs::path StorageLayout::nativeWalFile(unsigned shard, uint64_t generation) const {
    return nativeWalDir(shard) / std::format("idx_{:06}.wal", generation);
}

fs::path StorageLayout::nativeSstableFile(unsigned shard, uint64_t fileNumber) const {
    return nativeIndexDir(shard) / std::format("idx_{:06}.sst", fileNumber);
}

fs::path StorageLayout::placementFile() const {
    return underRoot("placement.json");
}

fs::path StorageLayout::workerRegistryFile() const {
    return underRoot("workers.json");
}

fs::path StorageLayout::workerRegistryTemporaryFile() const {
    return withSuffix(workerRegistryFile(), ".tmp");
}

fs::path StorageLayout::vshardDataDir() const {
    return underRoot("vshards");
}

fs::path StorageLayout::vshardDir(uint16_t vshard) const {
    if (vshard >= VIRTUAL_SHARD_COUNT)
        throw std::out_of_range("VShard ID is outside the fixed storage-layout range");
    return vshardDataDir() / std::format("{:04}", vshard);
}

std::optional<uint16_t> StorageLayout::parseVShardDirName(std::string_view name) const {
    uint32_t parsed = 0;
    const auto result = std::from_chars(name.data(), name.data() + name.size(), parsed);
    if (name.empty() || result.ec != std::errc{} || result.ptr != name.data() + name.size() ||
        parsed >= VIRTUAL_SHARD_COUNT || name != vshardDir(static_cast<uint16_t>(parsed)).filename().string()) {
        return std::nullopt;
    }
    return static_cast<uint16_t>(parsed);
}

fs::path StorageLayout::effectiveVShardOwnershipDir() const {
    return underRoot("vshard_ownership");
}

fs::path StorageLayout::effectiveVShardOwnershipRevisionFile(uint64_t revision) const {
    if (revision == 0)
        throw std::out_of_range("effective VShard ownership revision zero is reserved");
    return effectiveVShardOwnershipDir() / std::format("owners.{:020}.bin", revision);
}

fs::path StorageLayout::effectiveVShardOwnershipRevisionTemporaryFile(uint64_t revision) const {
    return withSuffix(effectiveVShardOwnershipRevisionFile(revision), ".tmp");
}

std::optional<uint64_t> StorageLayout::parseEffectiveVShardOwnershipRevisionFilename(std::string_view name) const {
    if (!name.starts_with(effectiveVShardOwnershipRevisionPrefix) ||
        !name.ends_with(effectiveVShardOwnershipRevisionSuffix)) {
        return std::nullopt;
    }

    const auto numeric = name.substr(
        effectiveVShardOwnershipRevisionPrefix.size(),
        name.size() - effectiveVShardOwnershipRevisionPrefix.size() - effectiveVShardOwnershipRevisionSuffix.size());
    uint64_t revision = 0;
    const auto result = std::from_chars(numeric.data(), numeric.data() + numeric.size(), revision);
    if (numeric.empty() || result.ec != std::errc{} || result.ptr != numeric.data() + numeric.size() || revision == 0 ||
        name != effectiveVShardOwnershipRevisionFile(revision).filename().string()) {
        return std::nullopt;
    }
    return revision;
}

fs::path StorageLayout::effectiveVShardOwnershipManifestFile() const {
    return underRoot("vshard_ownership.manifest");
}

fs::path StorageLayout::effectiveVShardOwnershipManifestTemporaryFile() const {
    return withSuffix(effectiveVShardOwnershipManifestFile(), ".tmp");
}

fs::path StorageLayout::effectiveVShardOwnershipInitializingMarkerFile() const {
    return underRoot("vshard_ownership.initializing");
}

fs::path StorageLayout::shardCountMetadataFile() const {
    return underRoot("shard_count.meta");
}

fs::path StorageLayout::shardCountMetadataTemporaryFile() const {
    return withSuffix(shardCountMetadataFile(), ".tmp");
}

fs::path StorageLayout::rebalanceStateFile() const {
    return underRoot("rebalance.state");
}

fs::path StorageLayout::rebalanceStateTemporaryFile() const {
    return withSuffix(rebalanceStateFile(), ".tmp");
}

}  // namespace timestar
