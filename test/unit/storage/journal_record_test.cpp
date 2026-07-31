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

// ---------------------------------------------------------------------------
// Streaming encode (debt D-32)
// ---------------------------------------------------------------------------

// encodeInto() used to build the body in a scratch string and append it, i.e. one whole
// extra copy of the payload per record -- nothing for a write batch, a second copy of a
// whole VShard snapshot for a Snapshot record. It now writes straight into `out` and
// PATCHES the CRC field in place, which is only correct if the patch lands on the right
// four bytes of a buffer that may ALREADY have records in it. That is what this pins.
TEST(JournalRecordTest, EncodeIntoANonEmptyBufferPatchesTheRightCrcAndStaysDecodable) {
    const JournalRecord a = sample(3, 1, JournalRecordKind::Data, "first record's payload"s);
    const JournalRecord b = sample(4, 2, JournalRecordKind::Snapshot, std::string(9000, '\x5a'));

    std::string buf = "PRE-EXISTING SEGMENT BYTES";
    const size_t prefix = buf.size();
    a.encodeInto(buf);
    b.encodeInto(buf);

    // Appending into a buffer is exactly concatenating the standalone encodings.
    EXPECT_EQ(buf, "PRE-EXISTING SEGMENT BYTES"s + a.encode() + b.encode());

    // ...and both decode, which is what proves the CRC was patched at the right offset in
    // each frame rather than at the head of the buffer.
    size_t consumed = 0;
    auto ra = JournalRecord::decode(std::span<const char>(buf.data() + prefix, buf.size() - prefix), consumed);
    ASSERT_TRUE(ra.has_value());
    EXPECT_EQ(ra->payload, "first record's payload");
    const size_t first = consumed;
    auto rb = JournalRecord::decode(std::span<const char>(buf.data() + prefix + first, buf.size() - prefix - first),
                                    consumed);
    ASSERT_TRUE(rb.has_value());
    EXPECT_EQ(rb->payload, std::string(9000, '\x5a'));
    EXPECT_EQ(rb->kind, JournalRecordKind::Snapshot);
}

// The size a caller can act on BEFORE the bytes exist -- JournalWriter::append decides
// whether to rotate the segment from this, then streams the record into its buffer. If it
// ever disagreed with the real encoding, a record would straddle a segment boundary.
TEST(JournalRecordTest, EncodedBytesMatchesTheRealEncodingForEveryPayloadShape) {
    for (const std::string& payload : {""s, "x"s, std::string(27, 'q'), std::string(65537, '\x01')}) {
        const JournalRecord r = sample(9, 5, JournalRecordKind::Data, payload);
        EXPECT_EQ(r.encodedBytes(), r.encode().size()) << "payload size " << payload.size();
    }
}

// NOT TESTED HERE, DELIBERATELY, AND THIS IS THE PLACE A READER WILL LOOK FOR IT (debt
// D-32, review F4): that `encodeInto` never leaves a PARTIAL frame behind. Writing the
// frame in place gave up the all-or-nothing a scratch string had for free, and the
// consequence would be worse than a lost record -- a throw between the length prefix and
// the payload leaves the writer's buffer holding a partial frame with a valid-looking
// length, the next barrier persists it, and recovery reads the segment as torn FROM THAT
// POINT, discarding every later record.
//
// Two things make it safe, and neither is externally observable, which is why there is no
// test rather than a weak one: (1) the whole frame's capacity is taken in ONE reservation
// before the first byte is written, so no write can reallocate and none can throw; (2) any
// exception unwinds the buffer to where it started. Observing (1) needs the reallocation
// COUNT during a single call, which nothing outside std::string can see -- final capacity
// is identical either way on libstdc++, since a trailing append lands on the same figure
// the reservation would have. Forcing (2) needs `operator new` to fail on demand, and
// seastar owns `operator new` process-wide in this binary; an override to test one function
// would change allocation for every other test in it.

// GEOMETRIC GROWTH (debt D-32, review F5). `JournalWriter::tail_` is appended to record
// after record, and an exact reserve per record would in principle re-pay a reallocation
// on every append -- O(K^2) copying over a burst of K records.
//
// READ THIS BEFORE TREATING IT AS A REGRESSION TEST: it is a PROPERTY test and its negative
// control does NOT fail. Reverting encodeInto to an exact reserve leaves it green, because
// libstdc++'s `_M_create` already clamps a growth request up to at least twice the old
// capacity (measured: an exact-reserve loop of 1000 appends reallocates 11 times, not
// 1000). What this pins is the property itself -- appending K records must not cost K
// reallocations -- which would catch a future edit that reintroduces the cost some other
// way, or a standard library that does not clamp.
TEST(JournalRecordTest, AppendingManyRecordsReallocatesLogarithmicallyNotEveryTime) {
    const JournalRecord r = sample(1, 1, JournalRecordKind::Data, std::string(512, 'z'));

    std::string tail;
    const char* last = tail.data();
    size_t reallocs = 0;
    constexpr int kRecords = 1000;
    for (int i = 0; i < kRecords; ++i) {
        r.encodeInto(tail);
        if (tail.data() != last) {
            ++reallocs;
            last = tail.data();
        }
    }

    EXPECT_EQ(tail.size(), static_cast<size_t>(kRecords) * r.encodedBytes());
    // Doubling from empty reaches ~520 KB in ~20 steps.
    EXPECT_LE(reallocs, 40u) << "reallocated " << reallocs << " times over " << kRecords
                             << " appends -- something has made the tail buffer grow linearly";
}
