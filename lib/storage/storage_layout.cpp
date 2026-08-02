#include "storage_layout.hpp"

#include <charconv>
#include <format>
#include <stdexcept>
#include <string>
#include <utility>

namespace fs = std::filesystem;

namespace timestar {
namespace {

constexpr std::string_view shardDirectoryPrefix = "shard_";

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
    // A bare "//" is an implementation-defined POSIX root that may name a
    // different path than "/". The configured-root helper (dataRootPath)
    // collapses it to "/", so match that here to keep the two in agreement.
    if (normalized.native() == "//")
        normalized = fs::path("/");
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

fs::path StorageLayout::vshardDataDir() const {
    return underRoot("vshards");
}

fs::path StorageLayout::vshardDir(VShardId vshard) const {
    if (!vshard.valid())
        throw std::out_of_range("VShard ID is outside the fixed storage-layout range");
    return vshardDataDir() / std::format("{:04}", vshard.value());
}

std::optional<VShardId> StorageLayout::parseVShardDirName(std::string_view name) const {
    uint32_t parsed = 0;
    const auto result = std::from_chars(name.data(), name.data() + name.size(), parsed);
    // Canonical spelling only: exactly the four-digit zero-padded form vshardDir
    // produces, in range, no sign/whitespace/suffix/path separators.
    if (name.empty() || result.ec != std::errc{} || result.ptr != name.data() + name.size() ||
        parsed >= VIRTUAL_SHARD_COUNT ||
        name != vshardDir(VShardId{static_cast<uint16_t>(parsed)}).filename().string()) {
        return std::nullopt;
    }
    return VShardId{static_cast<uint16_t>(parsed)};
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
