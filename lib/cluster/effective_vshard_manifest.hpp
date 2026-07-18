#pragma once

#include "effective_vshard_layout.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace timestar::cluster {

inline constexpr uint32_t EFFECTIVE_VSHARD_MANIFEST_FORMAT_VERSION = 1;
inline constexpr size_t EFFECTIVE_VSHARD_MANIFEST_BINARY_BYTES = 44;

// A 128-bit XXH3 digest of the exact immutable ownership-revision bytes. It is
// a cross-file identity and rollback witness, not an authentication primitive.
struct EffectiveVShardLayoutDigest {
    uint64_t low64 = 0;
    uint64_t high64 = 0;

    bool operator==(const EffectiveVShardLayoutDigest&) const = default;
};

// Root-level high-water witness selecting one immutable ownership revision.
// The filename is derived canonically from layoutRevision by StorageLayout.
struct EffectiveVShardManifest {
    uint32_t formatVersion = EFFECTIVE_VSHARD_MANIFEST_FORMAT_VERSION;
    uint64_t layoutRevision = 0;
    uint32_t layoutBinaryBytes = 0;
    EffectiveVShardLayoutDigest layoutDigest;

    bool operator==(const EffectiveVShardManifest&) const = default;
};

[[nodiscard]] EffectiveVShardManifest createEffectiveVShardManifest(const EffectiveVShardLayout& layout);

// Validates that exactLayoutBytes are the canonical effective layout selected
// by the manifest, then returns the decoded layout. A matching CRC without a
// matching XXH3 digest is never accepted.
[[nodiscard]] EffectiveVShardLayout validateEffectiveVShardManifest(const EffectiveVShardManifest& manifest,
                                                                    std::string_view exactLayoutBytes);

// Format v1 is a fixed 44-byte canonical binary:
//   magic[8], format:u32, layout_revision:u64, layout_bytes:u32,
//   layout_digest_low:u64, layout_digest_high:u64, crc32:u32.
// Integers and the trailing CRC are little-endian. CRC-32/ISO-HDLC covers every
// byte except the CRC itself and detects corruption; freshness comes from the
// root-level manifest being durably advanced after each immutable revision.
[[nodiscard]] std::string encodeEffectiveVShardManifestBinary(const EffectiveVShardManifest& manifest);
[[nodiscard]] EffectiveVShardManifest decodeEffectiveVShardManifestBinary(std::string_view bytes);

}  // namespace timestar::cluster
