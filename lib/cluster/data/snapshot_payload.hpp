#pragma once

#include "../../core/series_id.hpp"
#include "../../storage/vshard_snapshot_manifest.hpp"

#include <optional>
#include <filesystem>
#include <seastar/core/future.hh>
#include <string>
#include <utility>
#include <vector>

namespace timestar::data {

using timestar::VShardSnapshotManifest;

// One data file carried in a snapshot: its target name (as it will be installed into
// the receiver's tsm dir) and its raw bytes.
struct SnapshotFile {
    std::string name;
    std::string bytes;
};

// Disk-backed form used by the production snapshot path. Ownership is
// explicit: temporary producer/extraction objects are removed unless moved to
// another owner or released after a durable promotion.
struct SnapshotFilePath {
    std::string name;
    std::filesystem::path path;
    uint64_t size = 0;
    bool removeOnDestroy = true;

    SnapshotFilePath() = default;
    SnapshotFilePath(std::string n, std::filesystem::path p, uint64_t s)
        : name(std::move(n)), path(std::move(p)), size(s) {}
    SnapshotFilePath(const SnapshotFilePath&) = delete;
    SnapshotFilePath& operator=(const SnapshotFilePath&) = delete;
    SnapshotFilePath(SnapshotFilePath&& other) noexcept;
    SnapshotFilePath& operator=(SnapshotFilePath&& other) noexcept;
    ~SnapshotFilePath();
    void release() noexcept { removeOnDestroy = false; }
};

// A replicated delete operation already applied at `appliedIndex`. These are
// state-machine data, not storage-engine tombstones: retaining them across Raft
// log compaction is what makes a client retry harmless after restart or
// InstallSnapshot. `commandHash` detects operation-ID reuse for another target.
struct DeleteOperationReceipt {
    SeriesId128 operationId{};
    uint64_t appliedIndex = 0;
    uint64_t commandHash = 0;
    // Client-stable issuance time carried by the replicated command. Non-zero.
    uint64_t issuedAtMs = 0;

    auto operator<=>(const DeleteOperationReceipt&) const = default;
};

struct DeleteReceiptSnapshotState {
    // Modern retries at or before this floor are known-retired no-ops. The
    // index identifies the log entry whose deterministic apply advanced it.
    uint64_t retiredBeforeMs = 0;
    uint64_t retiredAtIndex = 0;
    std::vector<DeleteOperationReceipt> receipts;
};

// The latest globally ordered sweep applied by this VShard. Group 0 permits
// only one sweep at a time, so one sequence fence makes every old/exact retry
// recognizable without a per-measurement map in each of 4,096 state machines.
struct RetentionCutoffSnapshotState {
    uint64_t sweepId = 0;
    std::string measurement;
    uint64_t policyVersion = 0;
    uint64_t cutoffTime = 0;
    uint64_t appliedIndex = 0;

    friend bool operator==(const RetentionCutoffSnapshotState&, const RetentionCutoffSnapshotState&) = default;
};

struct DataStateMachineSnapshotState {
    DeleteReceiptSnapshotState deleteReceipts;
    std::optional<RetentionCutoffSnapshotState> retentionCutoff;
};

// The exact-v1 logical snapshot payload for one VShard: manifest, catalog,
// state-machine fences and exactly one data object per manifest extent. The
// inline representation remains for Group-0-sized and codec tests; the
// production VShard path uses SnapshotPayloadFile and never materializes the
// data objects in one string.
struct SnapshotPayload {
    VShardSnapshotManifest manifest;
    // Deterministic SeriesCatalog::snapshot() bytes, authenticated by catalogHash.
    std::string catalog;
    // Canonically sorted bounded receipts at or below the Raft snapshot boundary.
    uint64_t deleteReceiptsRetiredBeforeMs = 0;
    uint64_t deleteReceiptsRetiredAtIndex = 0;
    std::vector<DeleteOperationReceipt> deleteReceipts;
    std::optional<RetentionCutoffSnapshotState> retentionCutoff;
    std::vector<SnapshotFile> files;  // one per manifest.dataExtents entry, same order
};

struct SnapshotPayloadFile {
    VShardSnapshotManifest manifest;
    std::string catalog;
    uint64_t deleteReceiptsRetiredBeforeMs = 0;
    uint64_t deleteReceiptsRetiredAtIndex = 0;
    std::vector<DeleteOperationReceipt> deleteReceipts;
    std::optional<RetentionCutoffSnapshotState> retentionCutoff;
    std::vector<SnapshotFilePath> files;
};

struct EncodedSnapshotFile {
    std::filesystem::path path;
    uint64_t size = 0;
    uint64_t hash = 0;  // FNV-1a over the complete TSP1 file, including trailer
    bool removeOnDestroy = true;

    EncodedSnapshotFile() = default;
    EncodedSnapshotFile(const EncodedSnapshotFile&) = delete;
    EncodedSnapshotFile& operator=(const EncodedSnapshotFile&) = delete;
    EncodedSnapshotFile(EncodedSnapshotFile&& other) noexcept;
    EncodedSnapshotFile& operator=(EncodedSnapshotFile&& other) noexcept;
    ~EncodedSnapshotFile();
    void release() noexcept { removeOnDestroy = false; }
};

// Explicitly-versioned v1, FNV-trailer-checksummed, bounds-checked codec.
// Decode returns nullopt on any malformed, truncated, or checksum-mismatched
// frame, so a corrupt snapshot can never be installed as valid state.
std::string encodeSnapshotPayload(const SnapshotPayload& payload);
std::optional<SnapshotPayload> decodeSnapshotPayload(const std::string& bytes);

// Helpers for inline exact-v1 payloads. Recovery can restore
// state-machine metadata without decoding/copying data objects; each helper
// verifies the outer checksum and complete framing.
std::optional<std::vector<DeleteOperationReceipt>> decodeSnapshotDeleteReceipts(const std::string& bytes);
std::optional<DeleteReceiptSnapshotState> decodeSnapshotDeleteReceiptState(const std::string& bytes);
std::optional<DataStateMachineSnapshotState> decodeSnapshotStateMachineState(const std::string& bytes);

// Consuming overload: byte-identical to the const-reference
// encoder, with one exact reservation and released input object bodies.
std::string encodeSnapshotPayload(SnapshotPayload&& payload);

// Byte-for-byte TSP1 v1 encoding/decoding without materialising any data object
// in memory. Only catalog/receipt metadata remains resident; each object is
// copied through a fixed 1-MiB buffer. The encoder consumes and cleans its
// temporary source objects. The decoder writes verified objects below
// `extractionDirectory`; its result owns those files.
seastar::future<EncodedSnapshotFile> encodeSnapshotPayloadFile(SnapshotPayloadFile payload,
                                                               std::filesystem::path outputPath);
seastar::future<std::optional<SnapshotPayloadFile>> decodeSnapshotPayloadFile(
    const std::filesystem::path& encodedPath, const std::filesystem::path& extractionDirectory);
seastar::future<std::optional<DataStateMachineSnapshotState>> decodeSnapshotStateMachineStateFile(
    const std::filesystem::path& encodedPath);

}  // namespace timestar::data
