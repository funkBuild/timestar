// Integration F.1: the lossless inter-node write command codec. Round-trips all
// four value types (incl. strings + int64 above 2^53 + revisions) and rejects any
// truncated / checksum-broken / type-inconsistent frame.
#include "../../../lib/cluster/data/write_record.hpp"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <limits>

using namespace timestar::data;  // brings in TSMValueType (re-exported here)

namespace {
// The ORIGINAL (pre-write-scaleout-2c) v1 writer, preserved verbatim as the
// backward-compatibility oracle: the Raft journal stores encoded commands, so a
// cluster restarting on a new binary replays entries these exact bytes produced.
// Every byte is a separate push_back, exactly as it was.
struct LegacyWriter {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u32(uint32_t v) {
        for (int i = 0; i < 4; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void dbl(double v) {
        uint64_t b;
        std::memcpy(&b, &v, 8);
        u64(b);
    }
    void str(const std::string& s) {
        u32(static_cast<uint32_t>(s.size()));
        out.append(s);
    }
};

uint64_t legacyFnv1a(const char* p, size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; ++i) {
        h ^= static_cast<uint8_t>(p[i]);
        h *= 1099511628211ull;
    }
    return h;
}

std::string legacyEncodeWriteBatch(const WriteBatch& batch) {
    LegacyWriter w;
    w.u64(batch.schemaVersion);
    w.u32(static_cast<uint32_t>(batch.series.size()));
    for (const auto& s : batch.series) {
        w.u8(static_cast<uint8_t>(s.type));
        w.str(s.seriesKey);
        const uint32_t count = static_cast<uint32_t>(s.timestamps.size());
        w.u32(count);
        for (uint64_t ts : s.timestamps)
            w.u64(ts);
        switch (s.type) {
            case TSMValueType::Float:
                for (double v : std::get<0>(s.values))
                    w.dbl(v);
                break;
            case TSMValueType::Integer:
                for (int64_t v : std::get<1>(s.values))
                    w.u64(static_cast<uint64_t>(v));
                break;
            case TSMValueType::Boolean:
                for (bool v : std::get<2>(s.values))
                    w.u8(v ? 1 : 0);
                break;
            case TSMValueType::String:
                for (const std::string& v : std::get<3>(s.values))
                    w.str(v);
                break;
        }
        w.u32(static_cast<uint32_t>(s.revisions.size()));
        for (uint64_t r : s.revisions)
            w.u64(r);
    }
    w.u64(legacyFnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

WriteSeries floatSeries() {
    WriteSeries s;
    s.seriesKey = "cpu,host=h1 load";
    s.type = TSMValueType::Float;
    s.timestamps = {10, 20, 30};
    s.values = std::vector<double>{1.5, -2.25, 3.0};
    return s;
}
}  // namespace

TEST(WriteRecordCodec, RoundTripsAllFourTypes) {
    WriteBatch b;
    b.schemaVersion = 7;
    b.series.push_back(floatSeries());
    // int64 above 2^53 (would lose precision through double) with revisions.
    WriteSeries i;
    i.seriesKey = "m,host=h2 bytes";
    i.type = TSMValueType::Integer;
    i.timestamps = {100, 200};
    i.values = std::vector<int64_t>{9007199254740993LL, -9007199254740995LL};
    i.revisions = {5, 6};
    b.series.push_back(i);
    // strings (unrepresentable in DataPoint).
    WriteSeries s;
    s.seriesKey = "log,host=h3 msg";
    s.type = TSMValueType::String;
    s.timestamps = {1};
    s.values = std::vector<std::string>{"a UTF-8 string, with commas and spaces"};
    b.series.push_back(s);
    // booleans.
    WriteSeries bo;
    bo.seriesKey = "up,host=h4 state";
    bo.type = TSMValueType::Boolean;
    bo.timestamps = {1, 2, 3};
    bo.values = std::vector<bool>{true, false, true};
    b.series.push_back(bo);

    auto back = decodeWriteBatch(encodeWriteBatch(b));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->schemaVersion, 7u);
    ASSERT_EQ(back->series.size(), 4u);

    EXPECT_EQ(back->series[0].seriesKey, "cpu,host=h1 load");
    EXPECT_EQ(std::get<std::vector<double>>(back->series[0].values)[1], -2.25);

    EXPECT_EQ(back->series[1].type, TSMValueType::Integer);
    EXPECT_EQ(std::get<std::vector<int64_t>>(back->series[1].values)[0], 9007199254740993LL);
    EXPECT_EQ(back->series[1].revisions, (std::vector<uint64_t>{5, 6}));

    EXPECT_EQ(std::get<std::vector<std::string>>(back->series[2].values)[0],
              "a UTF-8 string, with commas and spaces");

    auto& bv = std::get<std::vector<bool>>(back->series[3].values);
    EXPECT_EQ(bv.size(), 3u);
    EXPECT_TRUE(bv[0]);
    EXPECT_FALSE(bv[1]);
}

TEST(WriteRecordCodec, EmptyBatchRoundTrips) {
    WriteBatch b;
    auto back = decodeWriteBatch(encodeWriteBatch(b));
    ASSERT_TRUE(back.has_value());
    EXPECT_TRUE(back->series.empty());
}

TEST(WriteRecordCodec, TruncationAndCorruptionRejected) {
    WriteBatch b;
    b.series.push_back(floatSeries());
    std::string full = encodeWriteBatch(b);
    ASSERT_TRUE(decodeWriteBatch(full).has_value());
    // Every truncated prefix is rejected (checksum/structure).
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeWriteBatch(full.substr(0, n)).has_value()) << "prefix " << n;
    // A flipped body byte fails the checksum.
    std::string flipped = full;
    flipped[0] ^= 0xff;
    EXPECT_FALSE(decodeWriteBatch(flipped).has_value());
    // A flipped checksum byte fails.
    std::string badsum = full;
    badsum[badsum.size() - 1] ^= 0xff;
    EXPECT_FALSE(decodeWriteBatch(badsum).has_value());
    // Bogus series count with no bytes behind it.
    std::string bogus(12, '\0');
    bogus[8] = static_cast<char>(0xff);  // numSeries high
    bogus[9] = static_cast<char>(0xff);
    EXPECT_FALSE(decodeWriteBatch(bogus).has_value());
}

TEST(WriteRecordCodec, ConsistencyEnforced) {
    // A value column whose size disagrees with timestamps is inconsistent and must
    // not be produced by the decoder (checked here directly).
    WriteSeries s = floatSeries();
    s.timestamps = {1, 2};                        // 2 timestamps
    s.values = std::vector<double>{1.0, 2.0, 3.0};  // 3 values
    EXPECT_FALSE(s.consistent());
    // Revisions of the wrong length are inconsistent.
    WriteSeries r = floatSeries();
    r.revisions = {1};  // 1 revision for 3 points
    EXPECT_FALSE(r.consistent());
    // A well-formed one is consistent.
    EXPECT_TRUE(floatSeries().consistent());
}

// ---------------------------------------------------------------------------
// write-scaleout 2c: bulk codec + the v2 (delta-varint timestamp) wire format.
// ---------------------------------------------------------------------------

namespace {
// One batch touching every value type, every special float, non-monotone and
// huge-gap timestamps, empty series, and revisions.
WriteBatch kitchenSink() {
    WriteBatch b;
    b.schemaVersion = 0xDEADBEEFCAFEull;

    WriteSeries f;
    f.seriesKey = "cpu,host=h1 load";
    f.type = TSMValueType::Float;
    // Monotone with a large first value (the varint-delta case), then a BACKWARDS
    // step (zigzag), then a repeat (delta 0).
    f.timestamps = {1700000000000000000ull, 1700000000000000001ull, 1700000000000000501ull,
                    1700000000000000400ull, 1700000000000000400ull};
    f.values = std::vector<double>{std::numeric_limits<double>::quiet_NaN(),
                                  std::numeric_limits<double>::infinity(),
                                  -std::numeric_limits<double>::infinity(),
                                  -0.0,
                                  3.141592653589793};
    f.revisions = {1, 2, 3, 4, 5};
    b.series.push_back(f);

    WriteSeries i;
    i.seriesKey = "m,host=h2 bytes";
    i.type = TSMValueType::Integer;
    i.timestamps = {0, 1, 2};
    i.values = std::vector<int64_t>{std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max(),
                                   9007199254740993LL};
    b.series.push_back(i);

    WriteSeries s;
    s.seriesKey = "log,host=h3 msg";
    s.type = TSMValueType::String;
    s.timestamps = {5, 6, 7};
    s.values = std::vector<std::string>{"", "a UTF-8 \xE2\x9C\x93 string, with commas", std::string(1000, 'x')};
    b.series.push_back(s);

    WriteSeries bo;
    bo.seriesKey = "up,host=h4 state";
    bo.type = TSMValueType::Boolean;
    bo.timestamps = {1, 2, 3, 4};
    bo.values = std::vector<bool>{true, false, false, true};
    b.series.push_back(bo);

    WriteSeries empty;  // a series with no points at all
    empty.seriesKey = "empty,host=h5 v";
    empty.type = TSMValueType::Float;
    empty.values = std::vector<double>{};
    b.series.push_back(empty);
    return b;
}

// Bit-exact comparison: -0.0 == 0.0 and NaN != NaN under ==, so compare the bits.
void expectSameBits(double a, double d) {
    uint64_t ba, bd;
    std::memcpy(&ba, &a, 8);
    std::memcpy(&bd, &d, 8);
    EXPECT_EQ(ba, bd);
}

void expectSameBatch(const WriteBatch& a, const WriteBatch& b) {
    ASSERT_EQ(a.schemaVersion, b.schemaVersion);
    ASSERT_EQ(a.series.size(), b.series.size());
    for (size_t i = 0; i < a.series.size(); ++i) {
        const auto& x = a.series[i];
        const auto& y = b.series[i];
        EXPECT_EQ(x.seriesKey, y.seriesKey);
        EXPECT_EQ(static_cast<int>(x.type), static_cast<int>(y.type));
        EXPECT_EQ(x.timestamps, y.timestamps);
        EXPECT_EQ(x.revisions, y.revisions);
        ASSERT_EQ(x.values.index(), y.values.index());
        if (x.type == TSMValueType::Float) {
            const auto& xv = std::get<0>(x.values);
            const auto& yv = std::get<0>(y.values);
            ASSERT_EQ(xv.size(), yv.size());
            for (size_t k = 0; k < xv.size(); ++k)
                expectSameBits(xv[k], yv[k]);
        } else {
            EXPECT_TRUE(x.values == y.values);
        }
    }
}
}  // namespace

// Every value type round-trips through BOTH emitted formats, bit-exactly --
// NaN / +-Inf / -0.0 included, int64 at the extremes, strings with embedded
// UTF-8, and timestamps that go backwards or repeat.
TEST(WriteRecordCodec, RoundTripsEveryTypeInBothWireVersions) {
    const WriteBatch b = kitchenSink();
    for (uint32_t version : {kWriteBatchFormatV1, kWriteBatchFormatV2}) {
        auto back = decodeWriteBatch(encodeWriteBatch(b, version));
        ASSERT_TRUE(back.has_value()) << "version " << version;
        expectSameBatch(b, *back);
    }
}

// The v1 encoder is BYTE-IDENTICAL to the pre-2c per-byte writer. This is the
// contract that lets the bulk rewrite touch the journal path at all: existing
// journals hold exactly these bytes.
TEST(WriteRecordCodec, V1EncodingIsByteIdenticalToTheLegacyWriter) {
    const WriteBatch b = kitchenSink();
    EXPECT_EQ(encodeWriteBatch(b), legacyEncodeWriteBatch(b));
    EXPECT_EQ(encodeWriteBatch(b, kWriteBatchFormatV1), legacyEncodeWriteBatch(b));
    WriteBatch empty;
    EXPECT_EQ(encodeWriteBatch(empty), legacyEncodeWriteBatch(empty));
}

// Bytes written by the OLD writer still decode on the NEW reader (the journal
// back-compat contract, pinned in-process).
TEST(WriteRecordCodec, LegacyBytesDecodeOnTheNewReader) {
    const WriteBatch b = kitchenSink();
    auto back = decodeWriteBatch(legacyEncodeWriteBatch(b));
    ASSERT_TRUE(back.has_value());
    expectSameBatch(b, *back);
}

// v2 is self-identifying and smaller; v1 has no version field at all, so the
// decoder must not need to be told which it is holding.
TEST(WriteRecordCodec, V2IsTaggedSelfDescribingAndSmaller) {
    WriteBatch b;
    WriteSeries s;
    s.seriesKey = "cpu,host=h1 load";
    s.type = TSMValueType::Float;
    for (uint64_t i = 0; i < 1000; ++i) {
        s.timestamps.push_back(1700000000000000000ull + i * 1000000ull);  // 1ms apart
    }
    s.values = std::vector<double>(1000, 1.5);
    b.series.push_back(s);

    const std::string v1 = encodeWriteBatch(b, kWriteBatchFormatV1);
    const std::string v2 = encodeWriteBatch(b, kWriteBatchFormatV2);
    EXPECT_EQ(v2.compare(0, 4, "TSW2"), 0);
    EXPECT_NE(v1.compare(0, 4, "TSW2"), 0);
    // 8 bytes/timestamp -> a 4-byte varint delta for a 1ms step.
    EXPECT_LT(v2.size(), v1.size());
    EXPECT_TRUE(decodeWriteBatch(v1).has_value());
    EXPECT_TRUE(decodeWriteBatch(v2).has_value());
    // An unknown FUTURE version degrades to the newest format we can write, never
    // to an unreadable frame.
    EXPECT_EQ(encodeWriteBatch(b, 99u), v2);
}

// Truncation/corruption of a v2 frame is rejected exactly like v1 -- including the
// case where the magic survives but the body does not.
TEST(WriteRecordCodec, V2TruncationAndCorruptionRejected) {
    WriteBatch b;
    b.series.push_back(floatSeries());
    const std::string full = encodeWriteBatch(b, kWriteBatchFormatV2);
    ASSERT_TRUE(decodeWriteBatch(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeWriteBatch(full.substr(0, n)).has_value()) << "prefix " << n;
    std::string flipped = full;
    flipped[full.size() - 12] ^= 0xff;  // inside the body, past the magic
    EXPECT_FALSE(decodeWriteBatch(flipped).has_value());
    std::string badsum = full;
    badsum[badsum.size() - 1] ^= 0xff;
    EXPECT_FALSE(decodeWriteBatch(badsum).has_value());
}

// splitByVShard / vshardOf (write-scaleout 2a/2b): the routing hint is derived
// from the series KEY and never carried on the wire, so a decoded series is
// unrouted and re-derives the same VShard the sender computed.
TEST(WriteRecordCodec, VShardHintIsDerivedFromTheKeyAndNeverOnTheWire) {
    WriteBatch b;
    b.series.push_back(floatSeries());
    const uint16_t expected = timestar::virtualShard(SeriesId128::fromSeriesKey(b.series[0].seriesKey));

    WriteSeries fresh = floatSeries();
    EXPECT_EQ(fresh.vshard, WriteSeries::kUnroutedVShard);
    EXPECT_EQ(vshardOf(fresh), expected);

    // A LIE about the vshard does not survive encoding: the field is not on the
    // wire, so the receiver re-derives it from the canonical key.
    b.series[0].vshard = 7;
    auto back = decodeWriteBatch(encodeWriteBatch(b, kWriteBatchFormatV2));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->series[0].vshard, WriteSeries::kUnroutedVShard);
    EXPECT_EQ(vshardOf(back->series[0]), expected);
}

TEST(WriteRecordCodec, SplitByVShardMovesEachSeriesOnce) {
    WriteBatch b;
    b.schemaVersion = 42;
    for (int i = 0; i < 50; ++i) {
        WriteSeries s = floatSeries();
        s.seriesKey = "cpu,host=h" + std::to_string(i) + " load";
        b.series.push_back(std::move(s));
    }
    auto groups = splitByVShard(b);
    size_t total = 0;
    for (auto& [vs, group] : groups) {
        EXPECT_EQ(group.schemaVersion, 42u);
        for (auto& s : group.series) {
            EXPECT_EQ(vshardOf(s), vs);  // every series is in ITS OWN VShard's group
            ++total;
        }
    }
    EXPECT_EQ(total, 50u);
    // Merging back preserves the batch (order within a group preserved).
    auto merged = mergeVShardBatches(std::move(groups));
    EXPECT_EQ(merged.series.size(), 50u);
    EXPECT_EQ(merged.schemaVersion, 42u);
}

namespace {
// Peak resident set (VmHWM), in KiB. Monotone, so a delta across a decode is exactly
// "how much memory did that decode force us to touch".
size_t peakRssKb() {
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line))
        if (line.rfind("VmHWM:", 0) == 0)
            return static_cast<size_t>(std::strtoul(line.c_str() + 6, nullptr, 10));
    return 0;
}

// A checksum-VALID v2 frame whose single series declares `declaredCount` timestamps
// while the body is just `bodyBytes` of filler. The count passes the bytes-remaining
// bound (a delta is >= 1 wire byte) but is a lie about how much there is to decode.
std::string inflatedCountV2Frame(size_t bodyBytes, uint32_t declaredCount) {
    auto u32 = [](std::string& o, uint32_t v) {
        for (int i = 0; i < 4; ++i)
            o.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    };
    auto u64 = [](std::string& o, uint64_t v) {
        for (int i = 0; i < 8; ++i)
            o.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    };
    std::string s;
    s.append("TSW2", 4);
    u64(s, 0);  // schemaVersion
    u32(s, 1);  // one series
    s.push_back(static_cast<char>(TSMValueType::Float));
    const std::string key = "cpu,host=h1 v";
    u32(s, static_cast<uint32_t>(key.size()));
    s.append(key);
    u32(s, declaredCount);
    s.append(bodyBytes, '\x01');  // filler deltas; decoding runs out long before the count
    uint64_t h = 1469598103934665603ull;
    for (char c : s) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ull;
    }
    u64(s, h);
    return s;
}
}  // namespace

// A declared count is bound-checked against the bytes remaining, which stops an
// over-READ but not an over-ALLOCATION: a v2 delta timestamp is >= 1 wire byte but 8
// bytes resident. A 16 MiB frame declaring ~16.7M timestamps used to reserve ~134 MB
// (8x the frame) before decoding a single delta -- and an inbound RPC frame is not
// size-limited today. The reserve is now capped and grown as bytes are really
// consumed, so a lying frame is rejected having allocated ~32 KB.
TEST(WriteRecordCodec, InflatedCountDoesNotAmplifyMemory) {
    constexpr size_t kFrameBytes = 16u << 20;
    const std::string frame = inflatedCountV2Frame(kFrameBytes, kFrameBytes - 25);

    const size_t before = peakRssKb();
    ASSERT_GT(before, 0u) << "no /proc/self/status VmHWM";
    auto decoded = decodeWriteBatch(frame);
    const size_t after = peakRssKb();

    EXPECT_FALSE(decoded.has_value()) << "a frame that lies about its count must be rejected";
    // Generous bound: the pre-fix reserve alone was ~134 MB (8x the frame). Anything
    // near the frame's own size is fine; an 8x blow-up is not.
    EXPECT_LT(after - before, 32u << 10) << "decoding a 16 MiB frame grew peak RSS by " << (after - before)
                                         << " KiB -- the declared count is being trusted for allocation again";
}

// write-scaleout 3b: encoding a BORROWED view of groups is byte-for-byte what encoding
// the merged batch produced. The remote propose path stopped merging (it must not
// consume groups it may need to re-dispatch), so this is the equality that says the
// wire did not change when the allocation went away.
TEST(WriteRecordCodec, ViewEncodingEqualsTheMergedEncoding) {
    for (uint32_t version : {kWriteBatchFormatV1, kWriteBatchFormatV2}) {
        VShardBatches groups = splitByVShard(kitchenSink());
        ASSERT_GE(groups.size(), 2u) << "the fixture must span several VShards to be a real test";
        const std::string viaView = encodeWriteBatch(viewOf(groups), version);
        const std::string viaMerge = encodeWriteBatch(mergeVShardBatches(std::move(groups)), version);
        EXPECT_EQ(viaView, viaMerge) << "version " << version;
        // ... and it still decodes to the same batch.
        auto a = decodeWriteBatch(viaView);
        ASSERT_TRUE(a.has_value());
        EXPECT_EQ(a->series.size(), kitchenSink().series.size());
    }
}

// A subset view encodes only that subset -- which is what makes "retry the failed slices
// only" possible without rebuilding a batch.
TEST(WriteRecordCodec, ViewEncodingCarriesOnlyTheSelectedGroups) {
    VShardBatches groups = splitByVShard(kitchenSink());
    ASSERT_GE(groups.size(), 2u);
    VShardBatchView one{&groups[0]};
    auto decoded = decodeWriteBatch(encodeWriteBatch(one, kWriteBatchFormatV2));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->series.size(), groups[0].second.series.size());
}
