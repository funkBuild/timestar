#include "journal_record.hpp"

#include "crc32.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <new>

// decode() computes frameLen = kFrameHeaderBytes + bodyLen with bodyLen a u32;
// this is overflow-safe only because size_t is wider than u32 on the target.
static_assert(sizeof(size_t) >= 8, "journal record decode assumes a 64-bit size_t");

namespace timestar {
namespace {

void putU16(std::string& out, uint16_t v) {
    out.push_back(static_cast<char>(v & 0xff));
    out.push_back(static_cast<char>((v >> 8) & 0xff));
}

void putU32(std::string& out, uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
}

void putU64(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xff));
}

uint16_t getU16(const char* p) {
    return static_cast<uint16_t>(static_cast<uint8_t>(p[0])) | static_cast<uint16_t>(static_cast<uint8_t>(p[1]) << 8);
}

uint32_t getU32(const char* p) {
    uint32_t r = 0;
    for (int i = 0; i < 4; ++i)
        r |= static_cast<uint32_t>(static_cast<uint8_t>(p[i])) << (i * 8);
    return r;
}

uint64_t getU64(const char* p) {
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i)
        r |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
    return r;
}

}  // namespace

void JournalRecord::encodeInto(std::string& out) const {
    // Write-side invariants (our own data): a record we produce must be one the
    // decoder would accept. Out-of-range ids and >4 GiB payloads are programming
    // errors, not untrusted input.
    assert(vshard.valid() && "encoding a JournalRecord with an out-of-range VShard id");
    assert(payload.size() <= static_cast<size_t>(UINT32_MAX) - kBodyHeaderBytes && "journal payload too large");

    // STRAIGHT INTO `out`, not via a `body` scratch string (debt D-32). The scratch cost a
    // full copy of the payload on every append -- invisible for a 1 KiB write batch, a
    // whole extra copy of a snapshot payload (up to kMaxVShardSnapshotBytes) for a Snapshot
    // record. The frame header has to be written before the body it describes and the CRC
    // is only known after, so the CRC field is reserved and PATCHED IN PLACE; the length is
    // known up front from encodedBytes()'s arithmetic.
    const size_t bodyLen = kBodyHeaderBytes + payload.size();

    // ALL OR NOTHING (review F4). Writing in place gave up the all-or-nothing that building
    // a scratch string and appending it had for free, and the consequence is worse than a
    // lost record: a throw between the length prefix and the payload leaves `out` -- which
    // for the one production caller IS `JournalWriter::tail_` -- holding a PARTIAL frame
    // with a valid-looking length. The next barrier writes it to disk, and recovery reads
    // the segment as torn FROM THAT POINT, discarding every later record too. So the write
    // is unwound to exactly where it started on any exception.
    const size_t mark = out.size();
    // GEOMETRIC, NOT EXACT (review F5) -- AND MEASURED, because the premise turned out not
    // to hold on this toolchain. The concern is real in principle: an exact reserve on a
    // buffer that is APPENDED TO repeatedly would defeat the amortization std::string
    // otherwise gives it, every record in a burst re-paying a reallocation for O(K^2)
    // copying over a window of K appends. On libstdc++ it does NOT happen, because
    // `_M_create` clamps any growth request up to at least twice the old capacity: an
    // exact-reserve loop of 1000 appends reallocates 11 times, not 1000 (measured). So
    // this is portability insurance and an explicit statement of intent, not a fix for a
    // live regression -- and the test below therefore pins the PROPERTY and cannot fail on
    // the exact-reserve shape here. Ask for double the capacity when growth is needed, take
    // the exact figure only when it is larger (a single huge snapshot record), and if the
    // doubled figure is the thing that cannot be allocated, fall back to the exact one
    // rather than failing a write that would have fit.
    //
    // RESERVING THE WHOLE FRAME UP FRONT IS ALSO WHAT MAKES THE WRITES BELOW UNABLE TO
    // THROW (review F4): with the capacity already in hand, every put/append is a memcpy
    // into it and nothing reallocates, so the half-written-frame window does not exist to
    // be hit. The unwind is kept as defence for a future edit that adds an allocating step.
    const size_t needed = mark + kFrameHeaderBytes + bodyLen;
    if (needed > out.capacity()) {
        try {
            out.reserve(std::max(needed, out.capacity() * 2));
        } catch (const std::bad_alloc&) {
            out.reserve(needed);
        }
    }
    try {
        putU32(out, static_cast<uint32_t>(bodyLen));
        const size_t crcOff = out.size();
        putU32(out, 0);  // patched below
        const size_t bodyOff = out.size();
        putU16(out, vshard.value());
        putU64(out, vshardSeq);
        putU64(out, raftTerm);
        putU64(out, raftIndex);
        out.push_back(static_cast<char>(kind));
        out.append(payload);
        assert(out.size() - bodyOff == bodyLen && "encodedBytes() and encodeInto() disagree");
        const uint32_t crc = CRC32::compute(out.data() + bodyOff, bodyLen);
        for (int i = 0; i < 4; ++i)
            out[crcOff + i] = static_cast<char>((crc >> (i * 8)) & 0xff);
    } catch (...) {
        out.resize(mark);  // never leave a half-written frame behind
        throw;
    }
}

std::string JournalRecord::encode() const {
    std::string out;
    encodeInto(out);
    return out;
}

std::optional<JournalRecord> JournalRecord::decode(std::span<const char> data, size_t& consumed) {
    if (data.size() < kFrameHeaderBytes)
        return std::nullopt;

    const uint32_t bodyLen = getU32(data.data());
    const uint32_t crc = getU32(data.data() + 4);
    if (bodyLen < kBodyHeaderBytes)
        return std::nullopt;  // a valid body always holds the fixed header

    const size_t frameLen = kFrameHeaderBytes + bodyLen;
    if (data.size() < frameLen)
        return std::nullopt;  // truncated tail

    const char* body = data.data() + kFrameHeaderBytes;
    if (CRC32::compute(body, bodyLen) != crc)
        return std::nullopt;  // corruption

    JournalRecord record;
    const uint16_t vs = getU16(body);
    // vs < VIRTUAL_SHARD_COUNT (4096) also enforces the reserved-zero top 4 bits
    // of the 16-bit on-disk VShard field (ADR 0002 sec 6).
    if (vs >= VIRTUAL_SHARD_COUNT)
        return std::nullopt;
    record.vshard = VShardId{vs};
    record.vshardSeq = getU64(body + 2);
    record.raftTerm = getU64(body + 10);
    record.raftIndex = getU64(body + 18);

    const uint8_t kindByte = static_cast<uint8_t>(body[26]);
    if (kindByte >= static_cast<uint8_t>(JournalRecordKind::MaxKind))
        return std::nullopt;  // unknown kind -> fail closed
    record.kind = static_cast<JournalRecordKind>(kindByte);

    record.payload.assign(body + kBodyHeaderBytes, bodyLen - kBodyHeaderBytes);
    consumed = frameLen;
    return record;
}

}  // namespace timestar
