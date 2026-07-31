#pragma once

#include "../core/vshard.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace timestar {

// Kind of a per-core multiplexed journal record. Values are ON-DISK and must
// never be renumbered; extend only by appending before MaxKind. The decoder
// rejects any value >= MaxKind so an old binary fails closed on a record kind
// it does not understand rather than misinterpreting it.
enum class JournalRecordKind : uint8_t {
    Data = 0,           // point writes
    CatalogCreate = 1,  // series-catalog definition (first time a series is seen)
    Truncation = 2,     // logical Raft log-suffix truncation (appended, not in-place)
    HardState = 3,      // Raft term / vote
    Config = 4,         // membership / configuration generation
    Retention = 5,      // retention cutoff
    Snapshot = 6,       // Raft snapshot boundary (lastIncludedIndex/Term + config + data)
    MaxKind = 7,        // sentinel; not a real kind
};

// One record in a per-core multiplexed journal (ADR 0001 sec 1). Every record is
// self-describing and CRC-covered so a torn or bit-rotted tail is detectable.
//
// On-disk frame (little-endian):
//   [ body_len : u32 ][ crc32 : u32 ][ body ]
// where crc32 covers `body`, body_len is the length of `body`, and
//   body = [ vshard : u16 ][ vshard_seq : u64 ][ raft_term : u64 ]
//          [ raft_index : u64 ][ kind : u8 ][ payload : body_len - 27 bytes ]
//
// (vshard, vshard_seq) totally orders one VShard's records independent of their
// physical position in the shared stream; replay routes each record to its
// boot-derived owning core (assignCore) and applies it in vshard_seq order.
struct JournalRecord {
    VShardId vshard{0};
    uint64_t vshardSeq = 0;  // per-VShard monotonic sequence (also the pre-Raft point revision)
    uint64_t raftTerm = 0;
    uint64_t raftIndex = 0;
    JournalRecordKind kind = JournalRecordKind::Data;
    std::string payload;  // opaque to the codec

    // Fixed body header (everything before the payload): 2 + 8 + 8 + 8 + 1.
    static constexpr size_t kBodyHeaderBytes = 27;
    // Frame overhead before the body: body_len(4) + crc32(4).
    static constexpr size_t kFrameHeaderBytes = 8;

    void encodeInto(std::string& out) const;
    [[nodiscard]] std::string encode() const;

    // What encodeInto() will append, exactly, without encoding anything (debt D-32).
    // A caller that needs the SIZE before the bytes -- JournalWriter::append, which
    // decides whether to rotate the segment first -- can now stream the record straight
    // into its buffer instead of encoding to a temporary and copying it in.
    [[nodiscard]] size_t encodedBytes() const { return kFrameHeaderBytes + kBodyHeaderBytes + payload.size(); }

    // Decode one frame from the front of `data`. On success returns the record
    // and sets `consumed` to the total frame length so a caller can iterate a
    // buffer of concatenated records. Returns nullopt on: truncated input, a
    // body length below the fixed header, a body that exceeds the input, a CRC
    // mismatch, an out-of-range / reserved-high-bits VShard id, or an unknown
    // kind. Any nullopt is a hard stop on the durability path (fail closed).
    [[nodiscard]] static std::optional<JournalRecord> decode(std::span<const char> data, size_t& consumed);

    friend bool operator==(const JournalRecord&, const JournalRecord&) = default;
};

}  // namespace timestar
