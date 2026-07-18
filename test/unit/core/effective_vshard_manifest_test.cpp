#include "../../../lib/cluster/effective_vshard_manifest.hpp"

#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace {

using timestar::cluster::createEffectiveVShardManifest;
using timestar::cluster::createInitialEffectiveVShardLayout;
using timestar::cluster::createWorkerRegistry;
using timestar::cluster::decodeEffectiveVShardManifestBinary;
using timestar::cluster::EffectiveVShardManifest;
using timestar::cluster::EffectiveVShardTarget;
using timestar::cluster::encodeEffectiveVShardLayoutBinary;
using timestar::cluster::encodeEffectiveVShardManifestBinary;
using timestar::cluster::proposeEffectiveVShardOwnershipAdvance;
using timestar::cluster::reconcileWorkerRegistry;
using timestar::cluster::StorageWorkerId;
using timestar::cluster::validateEffectiveVShardManifest;

template <typename Integer>
Integer readLittleEndian(const std::string& bytes, size_t offset) {
    Integer value = 0;
    for (size_t byte = 0; byte < sizeof(Integer); ++byte)
        value |= static_cast<Integer>(static_cast<unsigned char>(bytes[offset + byte])) << (byte * 8);
    return value;
}

template <typename Integer>
void writeLittleEndian(std::string& bytes, size_t offset, Integer value) {
    for (size_t byte = 0; byte < sizeof(Integer); ++byte) {
        bytes[offset + byte] = static_cast<char>(value & 0xffU);
        value >>= 8;
    }
}

void repairChecksum(std::string& bytes) {
    const auto checksumOffset = bytes.size() - sizeof(uint32_t);
    writeLittleEndian(bytes, checksumOffset, CRC32::compute(bytes.data(), checksumOffset));
}

auto initialLayout() {
    const auto target = EffectiveVShardTarget::build(createWorkerRegistry(2));
    return createInitialEffectiveVShardLayout(target);
}

}  // namespace

TEST(EffectiveVShardManifestTest, ManifestSelectsExactCanonicalImmutableRevision) {
    const auto layout = initialLayout();
    const auto layoutBytes = encodeEffectiveVShardLayoutBinary(layout);
    const auto manifest = createEffectiveVShardManifest(layout);
    const auto encoded = encodeEffectiveVShardManifestBinary(manifest);

    EXPECT_EQ(manifest.layoutRevision, 1u);
    EXPECT_EQ(manifest.layoutBinaryBytes, layoutBytes.size());
    EXPECT_NE(manifest.layoutDigest.low64 | manifest.layoutDigest.high64, 0u);
    ASSERT_EQ(encoded.size(), timestar::cluster::EFFECTIVE_VSHARD_MANIFEST_BINARY_BYTES);
    EXPECT_EQ(encoded.size(), 44u);
    EXPECT_EQ(std::string_view(encoded.data(), 8), std::string_view("TSVOMF1\0", 8));
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 8), 1u);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 12), 1u);
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 20), 49212u);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 24), manifest.layoutDigest.low64);
    EXPECT_EQ(readLittleEndian<uint64_t>(encoded, 32), manifest.layoutDigest.high64);
    EXPECT_EQ(readLittleEndian<uint32_t>(encoded, 40), CRC32::compute(encoded.data(), 40));
    EXPECT_EQ(decodeEffectiveVShardManifestBinary(encoded), manifest);
    EXPECT_EQ(validateEffectiveVShardManifest(manifest, layoutBytes), layout);

    // Frozen golden values detect an accidental digest-algorithm or byte-order change.
    EXPECT_EQ(manifest.layoutDigest.low64, 3286474396794001057ULL);
    EXPECT_EQ(manifest.layoutDigest.high64, 11670442333263755712ULL);
}

TEST(EffectiveVShardManifestTest, DigestRejectsAValidSameRevisionFork) {
    const auto layout = initialLayout();
    const auto manifest = createEffectiveVShardManifest(layout);
    auto fork = layout;
    fork.ownership[0].owner = fork.ownership[0].owner == StorageWorkerId(1) ? StorageWorkerId(2) : StorageWorkerId(1);
    const auto forkBytes = encodeEffectiveVShardLayoutBinary(fork);

    EXPECT_EQ(fork.revision, manifest.layoutRevision);
    EXPECT_THROW((void)validateEffectiveVShardManifest(manifest, forkBytes), std::invalid_argument);
    EXPECT_NE(createEffectiveVShardManifest(fork).layoutDigest, manifest.layoutDigest);
}

TEST(EffectiveVShardManifestTest, ConsecutiveManifestsRejectDirectoryOnlyRevisionRollback) {
    const auto registryOne = createWorkerRegistry(2);
    const auto targetOne = EffectiveVShardTarget::build(registryOne);
    const auto revisionOne = createInitialEffectiveVShardLayout(targetOne);
    const auto targetTwo = EffectiveVShardTarget::build(reconcileWorkerRegistry(registryOne, 3));
    std::optional<uint16_t> moving;
    for (uint32_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard) {
        if (revisionOne.ownership[vshard].owner != targetTwo.desiredOwnerFor(static_cast<uint16_t>(vshard))) {
            moving = static_cast<uint16_t>(vshard);
            break;
        }
    }
    ASSERT_TRUE(moving.has_value());
    const auto revisionTwo = proposeEffectiveVShardOwnershipAdvance(revisionOne, targetTwo, *moving);
    const auto bytesOne = encodeEffectiveVShardLayoutBinary(revisionOne);
    const auto bytesTwo = encodeEffectiveVShardLayoutBinary(revisionTwo);
    const auto manifestOne = createEffectiveVShardManifest(revisionOne);
    const auto manifestTwo = createEffectiveVShardManifest(revisionTwo);

    ASSERT_EQ(manifestOne.layoutRevision, 1u);
    ASSERT_EQ(manifestTwo.layoutRevision, 2u);
    EXPECT_EQ(validateEffectiveVShardManifest(manifestOne, bytesOne), revisionOne);
    EXPECT_EQ(validateEffectiveVShardManifest(manifestTwo, bytesTwo), revisionTwo);
    EXPECT_THROW((void)validateEffectiveVShardManifest(manifestTwo, bytesOne), std::invalid_argument);
    EXPECT_THROW((void)validateEffectiveVShardManifest(manifestOne, bytesTwo), std::invalid_argument);
}

TEST(EffectiveVShardManifestTest, ValidationRejectsWrongSizeDigestAndRevision) {
    const auto layout = initialLayout();
    const auto bytes = encodeEffectiveVShardLayoutBinary(layout);
    const auto manifest = createEffectiveVShardManifest(layout);

    EXPECT_THROW((void)validateEffectiveVShardManifest(manifest, std::string_view(bytes).substr(1)),
                 std::invalid_argument);

    auto wrongDigest = manifest;
    ++wrongDigest.layoutDigest.low64;
    EXPECT_THROW((void)validateEffectiveVShardManifest(wrongDigest, bytes), std::invalid_argument);

    auto wrongRevision = manifest;
    ++wrongRevision.layoutRevision;
    EXPECT_THROW((void)validateEffectiveVShardManifest(wrongRevision, bytes), std::invalid_argument);
}

TEST(EffectiveVShardManifestTest, BinaryDecoderFailsClosedOnCorruptionAndNoncanonicalFields) {
    const auto canonical = encodeEffectiveVShardManifestBinary(createEffectiveVShardManifest(initialLayout()));
    const auto expectDecodeFailure = [](std::string_view bytes) {
        EXPECT_THROW(
            {
                const auto ignored = decodeEffectiveVShardManifestBinary(bytes);
                (void)ignored;
            },
            std::invalid_argument);
    };

    auto corrupt = canonical;
    corrupt[24] ^= 1;
    expectDecodeFailure(corrupt);
    expectDecodeFailure(std::string_view(canonical).substr(0, canonical.size() - 1));
    expectDecodeFailure(canonical + "x");

    auto badMagic = canonical;
    badMagic[0] ^= 1;
    repairChecksum(badMagic);
    expectDecodeFailure(badMagic);

    auto badVersion = canonical;
    writeLittleEndian<uint32_t>(badVersion, 8, 2);
    repairChecksum(badVersion);
    expectDecodeFailure(badVersion);

    auto zeroRevision = canonical;
    writeLittleEndian<uint64_t>(zeroRevision, 12, 0);
    repairChecksum(zeroRevision);
    expectDecodeFailure(zeroRevision);

    auto wrongSize = canonical;
    writeLittleEndian<uint32_t>(wrongSize, 20, 49211);
    repairChecksum(wrongSize);
    expectDecodeFailure(wrongSize);

    auto zeroDigest = canonical;
    writeLittleEndian<uint64_t>(zeroDigest, 24, 0);
    writeLittleEndian<uint64_t>(zeroDigest, 32, 0);
    repairChecksum(zeroDigest);
    const auto decodedZeroDigest = decodeEffectiveVShardManifestBinary(zeroDigest);
    EXPECT_THROW(
        (void)validateEffectiveVShardManifest(decodedZeroDigest, encodeEffectiveVShardLayoutBinary(initialLayout())),
        std::invalid_argument);
}

TEST(EffectiveVShardManifestTest, EncoderRejectsFabricatedInvalidManifestState) {
    EffectiveVShardManifest invalid;
    EXPECT_THROW((void)encodeEffectiveVShardManifestBinary(invalid), std::invalid_argument);

    invalid.layoutRevision = 1;
    invalid.layoutBinaryBytes = timestar::cluster::EFFECTIVE_VSHARD_LAYOUT_BINARY_BYTES;
    invalid.layoutDigest.low64 = 1;
    invalid.formatVersion = 2;
    EXPECT_THROW((void)encodeEffectiveVShardManifestBinary(invalid), std::invalid_argument);
}
