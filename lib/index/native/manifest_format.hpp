#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

inline constexpr uint32_t MANIFEST_MAGIC = 0x464D5354;  // "TSMF"
inline constexpr uint32_t MANIFEST_VERSION = 2;
inline constexpr size_t MANIFEST_HEADER_SIZE = 8;

enum class ManifestDiskFormat {
    LegacyV1,
    CrcV2,
};

enum class ManifestDecodeStatus {
    Complete,
    Empty,
    RecoverableTail,
    Fatal,
};

struct ManifestFileRecord {
    uint64_t fileNumber = 0;
    uint64_t entryCount = 0;
    uint64_t fileSize = 0;
    std::string minKey;
    std::string maxKey;
    int level = 0;
    uint64_t writeTimestamp = 0;
};

struct ManifestDecodeResult {
    ManifestDecodeStatus status = ManifestDecodeStatus::Fatal;
    ManifestDiskFormat format = ManifestDiskFormat::LegacyV1;
    uint64_t nextFileNumber = 1;
    std::vector<ManifestFileRecord> files;
    size_t validRecordCount = 0;
    size_t issueOffset = 0;
    std::string issue;

    [[nodiscard]] bool complete() const noexcept { return status == ManifestDecodeStatus::Complete; }
};

// Decode the complete NativeIndex manifest format without performing I/O.
// A torn frame or bad CRC is reported as RecoverableTail after preserving the
// last fully decoded state. Semantically invalid records and unsupported
// versions are Fatal so callers never rewrite checksummed garbage as valid.
[[nodiscard]] ManifestDecodeResult decodeManifest(std::string_view contents);

}  // namespace timestar::index
