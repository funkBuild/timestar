#include "effective_vshard_manifest.hpp"

#include "../utils/crc32.hpp"

#include <xxhash.h>

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace timestar::cluster {
namespace {

constexpr std::array<char, 8> manifestMagic{'T', 'S', 'V', 'O', 'M', 'F', '1', '\0'};
constexpr size_t checksumBytes = sizeof(uint32_t);

template <typename Integer>
void appendLittleEndian(std::string& output, Integer value) {
    static_assert(std::is_unsigned_v<Integer>);
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        output.push_back(static_cast<char>(value & 0xffU));
        value >>= 8;
    }
}

template <typename Integer>
Integer readLittleEndian(std::string_view input, size_t& offset) {
    static_assert(std::is_unsigned_v<Integer>);
    if (offset > input.size() || input.size() - offset < sizeof(Integer))
        throw std::invalid_argument("effective VShard manifest binary is truncated");

    Integer value = 0;
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        value |= static_cast<Integer>(static_cast<unsigned char>(input[offset + byte])) << (byte * 8);
    }
    offset += sizeof(Integer);
    return value;
}

EffectiveVShardLayoutDigest digestLayoutBytes(std::string_view bytes) {
    const auto digest = XXH3_128bits(bytes.data(), bytes.size());
    return {.low64 = digest.low64, .high64 = digest.high64};
}

void validateStructure(const EffectiveVShardManifest& manifest) {
    if (manifest.formatVersion != EFFECTIVE_VSHARD_MANIFEST_FORMAT_VERSION)
        throw std::invalid_argument("unsupported effective VShard manifest format version");
    if (manifest.layoutRevision == 0)
        throw std::invalid_argument("effective VShard manifest revision zero is reserved");
    if (manifest.layoutBinaryBytes != EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES)
        throw std::invalid_argument("effective VShard manifest selects a noncanonical layout size");
}

}  // namespace

EffectiveVShardManifest createEffectiveVShardManifest(const EffectiveVShardLayout& layout) {
    const auto bytes = encodeEffectiveVShardLayoutBinary(layout);
    EffectiveVShardManifest manifest;
    manifest.layoutRevision = layout.revision;
    static_assert(EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES <= std::numeric_limits<uint32_t>::max());
    manifest.layoutBinaryBytes = static_cast<uint32_t>(bytes.size());
    manifest.layoutDigest = digestLayoutBytes(bytes);
    validateStructure(manifest);
    return manifest;
}

EffectiveVShardLayout validateEffectiveVShardManifest(const EffectiveVShardManifest& manifest,
                                                      std::string_view exactLayoutBytes) {
    validateStructure(manifest);
    if (exactLayoutBytes.size() != manifest.layoutBinaryBytes)
        throw std::invalid_argument("effective VShard manifest layout size mismatch");
    if (digestLayoutBytes(exactLayoutBytes) != manifest.layoutDigest)
        throw std::invalid_argument("effective VShard manifest layout digest mismatch");

    auto layout = decodeEffectiveVShardLayoutBinary(exactLayoutBytes);
    if (layout.revision != manifest.layoutRevision)
        throw std::invalid_argument("effective VShard manifest layout revision mismatch");
    return layout;
}

std::string encodeEffectiveVShardManifestBinary(const EffectiveVShardManifest& manifest) {
    validateStructure(manifest);

    std::string bytes;
    bytes.reserve(EFFECTIVE_VSHARD_MANIFEST_BINARY_BYTES);
    bytes.append(manifestMagic.data(), manifestMagic.size());
    appendLittleEndian(bytes, manifest.formatVersion);
    appendLittleEndian(bytes, manifest.layoutRevision);
    appendLittleEndian(bytes, manifest.layoutBinaryBytes);
    appendLittleEndian(bytes, manifest.layoutDigest.low64);
    appendLittleEndian(bytes, manifest.layoutDigest.high64);
    appendLittleEndian(bytes, CRC32::compute(bytes.data(), bytes.size()));
    if (bytes.size() != EFFECTIVE_VSHARD_MANIFEST_BINARY_BYTES)
        throw std::logic_error("effective VShard manifest encoder produced an unexpected size");
    return bytes;
}

EffectiveVShardManifest decodeEffectiveVShardManifestBinary(std::string_view bytes) {
    if (bytes.size() != EFFECTIVE_VSHARD_MANIFEST_BINARY_BYTES)
        throw std::invalid_argument("effective VShard manifest binary has a noncanonical size");

    size_t checksumOffset = bytes.size() - checksumBytes;
    const auto storedChecksum = readLittleEndian<uint32_t>(bytes, checksumOffset);
    if (storedChecksum != CRC32::compute(bytes.data(), bytes.size() - checksumBytes))
        throw std::invalid_argument("effective VShard manifest checksum mismatch");
    if (!std::equal(manifestMagic.begin(), manifestMagic.end(), bytes.begin()))
        throw std::invalid_argument("effective VShard manifest magic is invalid");

    size_t offset = manifestMagic.size();
    EffectiveVShardManifest manifest;
    manifest.formatVersion = readLittleEndian<uint32_t>(bytes, offset);
    manifest.layoutRevision = readLittleEndian<uint64_t>(bytes, offset);
    manifest.layoutBinaryBytes = readLittleEndian<uint32_t>(bytes, offset);
    manifest.layoutDigest.low64 = readLittleEndian<uint64_t>(bytes, offset);
    manifest.layoutDigest.high64 = readLittleEndian<uint64_t>(bytes, offset);
    if (offset != bytes.size() - checksumBytes)
        throw std::invalid_argument("effective VShard manifest binary has trailing fields");
    validateStructure(manifest);
    if (encodeEffectiveVShardManifestBinary(manifest) != bytes)
        throw std::invalid_argument("effective VShard manifest binary is not canonical");
    return manifest;
}

}  // namespace timestar::cluster
