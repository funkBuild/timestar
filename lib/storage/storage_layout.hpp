#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string_view>

namespace timestar {

// Immutable, lexical authority for every shard_N storage path.
//
// It performs no filesystem I/O and reads no global configuration: it is a pure
// function of the root it is constructed with. Storage components must obtain
// every persistent path from a StorageLayout injected by their owner rather
// than independently concatenating "shard_", "tsm", WAL filenames, or control
// filenames. This is the single seam the later VShard work extends; keeping it
// concrete for today's physical shards is deliberate.
//
// With the default root "." the produced directory and filename structure is
// byte-for-byte compatible with the existing shard_N layout, so injecting this
// authority is a path-ownership change, not a storage-format migration.
class StorageLayout final {
public:
    explicit StorageLayout(std::filesystem::path root);

    [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }
    // Resolve a relative root against the process CWD once at an ownership
    // boundary. The returned layout remains stable if the CWD later changes.
    [[nodiscard]] StorageLayout anchored() const;

    [[nodiscard]] std::filesystem::path shardDir(unsigned shard) const;
    [[nodiscard]] std::optional<unsigned> parseShardDirName(std::string_view name) const;
    [[nodiscard]] bool isShardNamespaceEntry(std::string_view name) const noexcept;
    [[nodiscard]] std::filesystem::path walFile(unsigned shard, uint64_t sequence) const;
    // Reserved publication temporary for a WAL segment. It never receives
    // entries: only a durable v1 header is written before the inode is linked
    // into its final `.wal` name.
    [[nodiscard]] std::filesystem::path walCreationTemporaryFile(unsigned shard, uint64_t sequence) const;
    [[nodiscard]] std::filesystem::path tsmDir(unsigned shard) const;
    [[nodiscard]] std::filesystem::path tsmFile(unsigned shard, const std::filesystem::path& filename) const;
    [[nodiscard]] std::filesystem::path tsmTombstoneFile(unsigned shard,
                                                         const std::filesystem::path& tsmFilename) const;
    [[nodiscard]] std::filesystem::path tsmFile(unsigned shard, uint64_t tier, uint64_t sequence) const;
    [[nodiscard]] std::filesystem::path tsmTemporaryFile(unsigned shard, uint64_t tier, uint64_t sequence) const;
    [[nodiscard]] std::filesystem::path tsmTombstoneFile(unsigned shard, uint64_t tier, uint64_t sequence) const;
    [[nodiscard]] std::filesystem::path compactedTsmFile(unsigned shard, uint64_t tier, uint64_t sequence,
                                                         uint64_t dataSequence) const;
    [[nodiscard]] std::filesystem::path compactedTsmTemporaryFile(unsigned shard, uint64_t tier, uint64_t sequence,
                                                                  uint64_t dataSequence) const;
    [[nodiscard]] std::filesystem::path compactedTsmTombstoneFile(unsigned shard, uint64_t tier, uint64_t sequence,
                                                                  uint64_t dataSequence) const;

    [[nodiscard]] std::filesystem::path nativeIndexDir(unsigned shard) const;
    [[nodiscard]] std::filesystem::path nativeManifestFile(unsigned shard) const;
    [[nodiscard]] std::filesystem::path nativeManifestTemporaryFile(unsigned shard) const;
    [[nodiscard]] std::filesystem::path nativeWalDir(unsigned shard) const;
    [[nodiscard]] std::filesystem::path nativeWalFile(unsigned shard, uint64_t generation) const;
    [[nodiscard]] std::filesystem::path nativeSstableFile(unsigned shard, uint64_t fileNumber) const;

    [[nodiscard]] std::filesystem::path placementFile() const;
    [[nodiscard]] std::filesystem::path shardCountMetadataFile() const;
    [[nodiscard]] std::filesystem::path shardCountMetadataTemporaryFile() const;
    [[nodiscard]] std::filesystem::path rebalanceStateFile() const;
    [[nodiscard]] std::filesystem::path rebalanceStateTemporaryFile() const;

    friend bool operator==(const StorageLayout& lhs, const StorageLayout& rhs) noexcept {
        return lhs.root_ == rhs.root_;
    }

private:
    [[nodiscard]] static std::filesystem::path normalizeRoot(std::filesystem::path root);
    [[nodiscard]] std::filesystem::path underRoot(const std::filesystem::path& relative) const;
    [[nodiscard]] static std::filesystem::path withSuffix(const std::filesystem::path& path, const char* suffix);
    [[nodiscard]] static std::filesystem::path withExtension(const std::filesystem::path& path, const char* extension);
    [[nodiscard]] static std::filesystem::path requireTsmFilename(const std::filesystem::path& filename);
    std::filesystem::path root_;
};

}  // namespace timestar
