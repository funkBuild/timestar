#include "../../../lib/storage/journal_record.hpp"

#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace {

using namespace std::string_literals;

using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::VShardId;

JournalRecord sample(uint16_t vshard, uint64_t seq, JournalRecordKind kind, std::string payload) {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
    r.raftTerm = 7;
    r.raftIndex = 9;
    r.kind = kind;
    r.payload = std::move(payload);
    return r;
}

// Build a raw frame directly (bypassing encode) so tests can inject values the
// public API would never produce, e.g. an out-of-range VShard or unknown kind.
std::string rawFrame(uint16_t vshard, uint64_t seq, uint8_t kind, const std::string& payload) {
    std::string body;
    auto putU16 = [&](uint16_t v) {
        body.push_back(char(v & 0xff));
        body.push_back(char((v >> 8) & 0xff));
    };
    auto putU64 = [&](uint64_t v) {
        for (int i = 0; i < 8; ++i)
            body.push_back(char((v >> (i * 8)) & 0xff));
    };
    putU16(vshard);
    putU64(seq);
    putU64(0);  // term
    putU64(0);  // index
    body.push_back(static_cast<char>(kind));
    body.append(payload);

    std::string frame;
    auto putU32 = [&](uint32_t v) {
        for (int i = 0; i < 4; ++i)
            frame.push_back(char((v >> (i * 8)) & 0xff));
    };
    putU32(static_cast<uint32_t>(body.size()));
    putU32(CRC32::compute(body.data(), body.size()));
    frame.append(body);
    return frame;
}

std::span<const char> asSpan(const std::string& s) {
    return std::span<const char>(s.data(), s.size());
}

TEST(JournalRecordTest, RoundTripsEveryFieldAndKind) {
    for (auto kind : {JournalRecordKind::Data, JournalRecordKind::CatalogCreate, JournalRecordKind::Truncation,
                      JournalRecordKind::HardState, JournalRecordKind::Config, JournalRecordKind::Retention}) {
        const auto original = sample(4095, 0xDEADBEEFCAFEULL, kind, "payload-bytes-\x00\x01\xff"s);
        const auto encoded = original.encode();

        size_t consumed = 0;
        const auto decoded = JournalRecord::decode(asSpan(encoded), consumed);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(consumed, encoded.size());
        EXPECT_EQ(*decoded, original);
    }
}

TEST(JournalRecordTest, EmptyPayloadRoundTrips) {
    const auto original = sample(0, 1, JournalRecordKind::Data, "");
    const auto encoded = original.encode();
    EXPECT_EQ(encoded.size(), JournalRecord::kFrameHeaderBytes + JournalRecord::kBodyHeaderBytes);

    size_t consumed = 0;
    const auto decoded = JournalRecord::decode(asSpan(encoded), consumed);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, original);
}

TEST(JournalRecordTest, ConcatenatedRecordsDecodeSequentially) {
    const std::vector<JournalRecord> records = {
        sample(0, 1, JournalRecordKind::CatalogCreate, "series-def"),
        sample(0, 2, JournalRecordKind::Data, "points"),
        sample(4095, 100, JournalRecordKind::Retention, ""),
    };
    std::string stream;
    for (const auto& r : records)
        r.encodeInto(stream);

    size_t offset = 0;
    for (const auto& expected : records) {
        size_t consumed = 0;
        const auto decoded =
            JournalRecord::decode(std::span<const char>(stream.data() + offset, stream.size() - offset), consumed);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(*decoded, expected);
        offset += consumed;
    }
    EXPECT_EQ(offset, stream.size());
}

TEST(JournalRecordTest, FormatIsStable) {
    // Pin the on-disk framing: body_len prefix, then crc, then body.
    const auto encoded = sample(1, 2, JournalRecordKind::Data, "hi").encode();
    ASSERT_GE(encoded.size(), JournalRecord::kFrameHeaderBytes);
    const uint32_t bodyLen = static_cast<uint8_t>(encoded[0]) | (static_cast<uint8_t>(encoded[1]) << 8) |
                             (static_cast<uint8_t>(encoded[2]) << 16) |
                             (static_cast<uint32_t>(static_cast<uint8_t>(encoded[3])) << 24);
    EXPECT_EQ(bodyLen, JournalRecord::kBodyHeaderBytes + 2);  // header + "hi"
    EXPECT_EQ(encoded.size(), JournalRecord::kFrameHeaderBytes + bodyLen);
}

TEST(JournalRecordTest, CrcMismatchIsRejected) {
    auto encoded = sample(3, 5, JournalRecordKind::Data, "corrupt-me").encode();
    // Flip a byte inside the body (past the 8-byte len+crc header).
    encoded[JournalRecord::kFrameHeaderBytes + 2] ^= 0x40;

    size_t consumed = 0;
    EXPECT_FALSE(JournalRecord::decode(asSpan(encoded), consumed).has_value());
}

TEST(JournalRecordTest, TruncatedInputIsRejected) {
    const auto encoded = sample(3, 5, JournalRecordKind::Data, "some-payload").encode();
    size_t consumed = 0;
    // One byte short of the full frame.
    EXPECT_FALSE(
        JournalRecord::decode(std::span<const char>(encoded.data(), encoded.size() - 1), consumed).has_value());
    // Shorter than even the frame header.
    EXPECT_FALSE(JournalRecord::decode(std::span<const char>(encoded.data(), 4), consumed).has_value());
}

TEST(JournalRecordTest, BodyLengthBelowHeaderIsRejected) {
    // Craft a frame claiming a body shorter than the fixed header.
    std::string frame;
    auto putU32 = [&](uint32_t v) {
        for (int i = 0; i < 4; ++i)
            frame.push_back(char((v >> (i * 8)) & 0xff));
    };
    putU32(10);  // body_len below kBodyHeaderBytes (27)
    putU32(0);   // crc (irrelevant; length check fails first)
    frame.append(10, '\0');

    size_t consumed = 0;
    EXPECT_FALSE(JournalRecord::decode(asSpan(frame), consumed).has_value());
}

TEST(JournalRecordTest, UnknownKindIsRejected) {
    const auto frame = rawFrame(0, 1, static_cast<uint8_t>(JournalRecordKind::MaxKind), "x");
    size_t consumed = 0;
    EXPECT_FALSE(JournalRecord::decode(asSpan(frame), consumed).has_value());

    const auto frame2 = rawFrame(0, 1, 200, "x");
    EXPECT_FALSE(JournalRecord::decode(asSpan(frame2), consumed).has_value());
}

TEST(JournalRecordTest, OutOfRangeOrReservedVShardIsRejected) {
    // 4096 sets bit 12 -> outside 0..4095 and into the reserved-zero top bits.
    const auto frame = rawFrame(4096, 1, static_cast<uint8_t>(JournalRecordKind::Data), "x");
    size_t consumed = 0;
    EXPECT_FALSE(JournalRecord::decode(asSpan(frame), consumed).has_value());

    // A reserved high bit set on an otherwise in-range low value.
    const auto frame2 = rawFrame(0x8000 | 5, 1, static_cast<uint8_t>(JournalRecordKind::Data), "x");
    EXPECT_FALSE(JournalRecord::decode(asSpan(frame2), consumed).has_value());
}

}  // namespace
