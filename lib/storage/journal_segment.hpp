#pragma once

#include "../utils/byte_codec.hpp"
#include "journal_record.hpp"

#include <array>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

namespace timestar {

// Fixed header at the start of every per-core journal segment file
// (ADR 0001 sec 2). Recovery validates it before trusting a segment's records,
// so a segment from a different cluster, format version, or (via boot id) a
// different process incarnation is detected rather than silently replayed.
struct JournalSegmentHeader {
    static constexpr uint32_t kMagic = 0x4A524E4C;  // 'JRNL'
    static constexpr uint32_t kFormatVersion = 1;
    // magic(4) + version(4) + clusterUuid(16) + coreNumber(4) + segmentNumber(8) + bootId(16)
    static constexpr size_t kEncodedBytes = 4 + 4 + 16 + 4 + 8 + 16;

    std::array<uint8_t, 16> clusterUuid{};
    uint32_t coreNumber = 0;
    uint64_t segmentNumber = 0;
    std::array<uint8_t, 16> bootId{};

    void encodeInto(std::string& out) const;
    [[nodiscard]] std::string encode() const;
    // Decode + validate. nullopt on a short read, wrong magic, or unknown format
    // version (a hard error -- not a recoverable torn tail).
    [[nodiscard]] static std::optional<JournalSegmentHeader> decode(codec::Reader& reader);

    friend bool operator==(const JournalSegmentHeader&, const JournalSegmentHeader&) = default;
};

// Result of scanning a segment for recovery (ADR 0001 sec 6).
struct JournalSegmentScan {
    JournalSegmentHeader header;
    std::vector<JournalRecord> records;  // every record up to the durable tail
    // Byte length of the header plus all cleanly-decoded records. Recovery
    // truncates the segment file to this length so a torn tail is dropped.
    size_t durableBytes = 0;
    // A partial or corrupt trailing record was found and discarded (expected
    // after a crash mid-append). Records earlier in the segment are intact.
    bool torn = false;
};

// Validate `data`'s segment header, then decode records until one fails to
// decode (a torn or corrupt tail) or the bytes are exhausted. Returns nullopt
// ONLY when the header itself is invalid -- a torn/corrupt tail is normal and is
// reported via JournalSegmentScan::torn with the intact durable prefix.
[[nodiscard]] std::optional<JournalSegmentScan> scanJournalSegment(std::span<const char> data);

}  // namespace timestar
