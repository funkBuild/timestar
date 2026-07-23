#include "journal_record.hpp"

#include "crc32.hpp"

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
    std::string body;
    body.reserve(kBodyHeaderBytes + payload.size());
    putU16(body, vshard.value());
    putU64(body, vshardSeq);
    putU64(body, raftTerm);
    putU64(body, raftIndex);
    body.push_back(static_cast<char>(kind));
    body.append(payload);

    putU32(out, static_cast<uint32_t>(body.size()));
    putU32(out, CRC32::compute(body.data(), body.size()));
    out.append(body);
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
