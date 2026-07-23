#include "../../../lib/storage/journal_segment.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace {

using timestar::JournalRecord;
using timestar::JournalRecordKind;
using timestar::JournalSegmentHeader;
using timestar::scanJournalSegment;
using timestar::VShardId;

JournalSegmentHeader header() {
    JournalSegmentHeader h;
    h.clusterUuid.fill(0xAB);
    h.coreNumber = 3;
    h.segmentNumber = 42;
    h.bootId.fill(0xCD);
    return h;
}

JournalRecord rec(uint16_t vshard, uint64_t seq, std::string payload) {
    JournalRecord r;
    r.vshard = VShardId{vshard};
    r.vshardSeq = seq;
    r.kind = JournalRecordKind::Data;
    r.payload = std::move(payload);
    return r;
}

std::span<const char> asSpan(const std::string& s) {
    return std::span<const char>(s.data(), s.size());
}

TEST(JournalSegmentTest, HeaderRoundTrips) {
    const auto h = header();
    const auto encoded = h.encode();
    EXPECT_EQ(encoded.size(), JournalSegmentHeader::kEncodedBytes);

    timestar::codec::Reader r(asSpan(encoded));
    const auto decoded = JournalSegmentHeader::decode(r);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(r.exhausted());
    EXPECT_EQ(*decoded, h);
}

TEST(JournalSegmentTest, HeaderRejectsBadMagicVersionAndTruncation) {
    auto encoded = header().encode();
    // Bad magic.
    auto badMagic = encoded;
    badMagic[0] ^= 0xff;
    timestar::codec::Reader r1(asSpan(badMagic));
    EXPECT_FALSE(JournalSegmentHeader::decode(r1).has_value());
    // Bad version.
    auto badVer = encoded;
    badVer[4] = 0x7f;
    timestar::codec::Reader r2(asSpan(badVer));
    EXPECT_FALSE(JournalSegmentHeader::decode(r2).has_value());
    // Truncated.
    timestar::codec::Reader r3(std::span<const char>(encoded.data(), encoded.size() - 1));
    EXPECT_FALSE(JournalSegmentHeader::decode(r3).has_value());
}

TEST(JournalSegmentTest, ScanReturnsHeaderOnlyWhenNoRecords) {
    const auto segment = header().encode();
    const auto scan = scanJournalSegment(asSpan(segment));
    ASSERT_TRUE(scan.has_value());
    EXPECT_EQ(scan->header, header());
    EXPECT_TRUE(scan->records.empty());
    EXPECT_FALSE(scan->torn);
    EXPECT_EQ(scan->durableBytes, segment.size());
}

TEST(JournalSegmentTest, ScanDecodesAllCleanRecords) {
    std::string segment = header().encode();
    const std::vector<JournalRecord> records = {rec(0, 1, "a"), rec(0, 2, "bb"), rec(5, 1, "")};
    for (const auto& r : records)
        r.encodeInto(segment);

    const auto scan = scanJournalSegment(asSpan(segment));
    ASSERT_TRUE(scan.has_value());
    EXPECT_FALSE(scan->torn);
    EXPECT_EQ(scan->durableBytes, segment.size());
    ASSERT_EQ(scan->records.size(), records.size());
    for (size_t i = 0; i < records.size(); ++i)
        EXPECT_EQ(scan->records[i], records[i]);
}

TEST(JournalSegmentTest, ScanDiscardsATornTailAndReportsDurableBytes) {
    std::string segment = header().encode();
    rec(0, 1, "first").encodeInto(segment);
    const size_t durableAfterFirst = segment.size();
    rec(0, 2, "second").encodeInto(segment);
    const size_t durableAfterSecond = segment.size();
    // Append a partial third record (a crash mid-append).
    std::string third = rec(0, 3, "third-partial").encode();
    segment.append(third.substr(0, third.size() / 2));

    const auto scan = scanJournalSegment(asSpan(segment));
    ASSERT_TRUE(scan.has_value());
    EXPECT_TRUE(scan->torn);
    ASSERT_EQ(scan->records.size(), 2u);
    EXPECT_EQ(scan->records[0].payload, "first");
    EXPECT_EQ(scan->records[1].payload, "second");
    EXPECT_EQ(scan->durableBytes, durableAfterSecond);
    EXPECT_NE(durableAfterFirst, durableAfterSecond);  // sanity
}

TEST(JournalSegmentTest, ScanStopsAtACorruptMiddleRecord) {
    std::string segment = header().encode();
    rec(0, 1, "good").encodeInto(segment);
    const size_t durableAfterGood = segment.size();
    std::string bad = rec(0, 2, "corrupt").encode();
    bad[JournalRecord::kFrameHeaderBytes + 1] ^= 0x55;  // flip a body byte -> CRC fails
    segment.append(bad);
    rec(0, 3, "after").encodeInto(segment);  // a valid record after the corrupt one

    const auto scan = scanJournalSegment(asSpan(segment));
    ASSERT_TRUE(scan.has_value());
    EXPECT_TRUE(scan->torn);
    // Recovery stops at the first bad record; the later valid one is NOT
    // resurrected (the durable tail ends at the last contiguous good record).
    ASSERT_EQ(scan->records.size(), 1u);
    EXPECT_EQ(scan->records[0].payload, "good");
    EXPECT_EQ(scan->durableBytes, durableAfterGood);
}

TEST(JournalSegmentTest, ScanRejectsAnInvalidHeader) {
    std::string segment = header().encode();
    rec(0, 1, "x").encodeInto(segment);
    segment[0] ^= 0xff;  // corrupt the magic
    EXPECT_FALSE(scanJournalSegment(asSpan(segment)).has_value());
}

}  // namespace
