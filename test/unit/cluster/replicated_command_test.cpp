// Integration M3 (foundation): the ENRICHED replicated command
// variant<WriteBatch, DeleteRangeKey, RetentionCutoffCmd> that replaces the lossy
// DataPoint-based DataCommand -- re-opens the Phase 5 codec gate with WriteBatch.
#include "../../../lib/cluster/data/replicated_command.hpp"

#include "../../../lib/cluster/data/journal_format.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;
using timestar::buildSeriesKey;

namespace {
WriteSeries floatSeries() {
    WriteSeries s;
    s.seriesKey = buildSeriesKey("cpu", {{"host", "h1"}}, "v");
    s.type = TSMValueType::Float;
    s.timestamps = {10, 20};
    s.values = std::vector<double>{1.5, 2.5};
    s.revisions = {};  // stamped at apply time
    return s;
}
}  // namespace

TEST(ReplicatedCommandCodec, WriteBatchArmRoundTrips) {
    WriteBatch b;
    b.series = {floatSeries()};
    b.schemaVersion = 7;
    ReplicatedCommand cmd = b;

    auto back = decodeReplicatedCommand(encodeReplicatedCommand(cmd));
    ASSERT_TRUE(back.has_value());
    ASSERT_TRUE(std::holds_alternative<WriteBatch>(*back));
    const auto& wb = std::get<WriteBatch>(*back);
    ASSERT_EQ(wb.series.size(), 1u);
    EXPECT_EQ(wb.series[0].seriesKey, buildSeriesKey("cpu", {{"host", "h1"}}, "v"));
    EXPECT_EQ(std::get<std::vector<double>>(wb.series[0].values), (std::vector<double>{1.5, 2.5}));
    EXPECT_EQ(wb.schemaVersion, 7u);
}

TEST(ReplicatedCommandCodec, DeleteAndRetentionArmsRoundTrip) {
    DeleteRangeKey d{buildSeriesKey("cpu", {{"host", "h1"}}, "v"), 100, 200};
    auto db = decodeReplicatedCommand(encodeReplicatedCommand(ReplicatedCommand{d}));
    ASSERT_TRUE(db.has_value());
    ASSERT_TRUE(std::holds_alternative<DeleteRangeKey>(*db));
    EXPECT_EQ(std::get<DeleteRangeKey>(*db).seriesKey, d.seriesKey);
    EXPECT_EQ(std::get<DeleteRangeKey>(*db).endTime, 200u);

    RetentionCutoffCmd rc{123456};
    auto rb = decodeReplicatedCommand(encodeReplicatedCommand(ReplicatedCommand{rc}));
    ASSERT_TRUE(rb.has_value());
    ASSERT_TRUE(std::holds_alternative<RetentionCutoffCmd>(*rb));
    EXPECT_EQ(std::get<RetentionCutoffCmd>(*rb).cutoffTime, 123456u);
}

TEST(ReplicatedCommandCodec, TruncationAndCorruptionRejected) {
    WriteBatch b;
    b.series = {floatSeries()};
    std::string full = encodeReplicatedCommand(ReplicatedCommand{b});
    ASSERT_TRUE(decodeReplicatedCommand(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeReplicatedCommand(full.substr(0, n)).has_value()) << "prefix " << n;
    std::string bad = full;
    bad[bad.size() - 1] ^= 0xff;  // flip checksum
    EXPECT_FALSE(decodeReplicatedCommand(bad).has_value());
    // Unknown kind tag.
    std::string unk = encodeReplicatedCommand(ReplicatedCommand{RetentionCutoffCmd{1}});
    unk[0] = 0x7f;  // invalid kind (checksum now wrong too -> rejected either way)
    EXPECT_FALSE(decodeReplicatedCommand(unk).has_value());
}

// write-scaleout 2c FORMAT PIN, now GATE-AWARE (debt D-7).
//
// THIS REPLACES `WriteBatchArmIsPinnedToTheV1JournalFormat`, which asserted the sub-blob
// was v1 UNCONDITIONALLY. That was the right test while nothing could raise the format;
// D-7 wires group-0's committed activation to the codec, so the property to pin changed
// shape rather than going away. Deleting it outright would have dropped the byte anchor
// that stops a change to `encodeWriteBatch`'s DEFAULT from silently promoting every
// journal entry -- which is the whole reason the pin existed.
//
// What is pinned now, in both directions:
//   * gate OFF (the fail-closed default, and what every node emits until the cluster has
//     COMMITTED an activation) => v1 bytes, byte-for-byte what the explicitly-v1 encoder
//     produces;
//   * gate ON (simulating an activation) => v2 bytes, byte-for-byte what the
//     explicitly-v2 encoder produces;
//   * the command decodes correctly EITHER WAY -- the decoder is unconditionally
//     bidirectional and old journals must stay readable forever.
//
// See data/journal_format.hpp for why "an old binary reads a v2 journal" is UNREACHABLE by
// activation ordering rather than untested.
namespace {
// Layout: u8 arm tag | u32 subBlobLen | subBlob | u64 fnv trailer.
constexpr size_t kArmTagBytes = 1;
constexpr size_t kLenPrefixBytes = 4;
constexpr size_t kSubBlobOffset = kArmTagBytes + kLenPrefixBytes;
constexpr size_t kMagicBytes = 4;

// Many points, so v2's delta-varint timestamps are a large and obvious size win -- i.e.
// exactly the batch a future "just use the newest format" change would target.
WriteBatch bigBatch() {
    WriteBatch b;
    b.series = {floatSeries()};
    b.series[0].timestamps.clear();
    std::vector<double> vals;
    for (uint64_t i = 0; i < 500; ++i) {
        b.series[0].timestamps.push_back(1700000000000000000ull + i * 1000000ull);
        vals.push_back(static_cast<double>(i));
    }
    b.series[0].values = vals;
    return b;
}

// A model of a decoder that predates v2: it reads the v1 layout and knows nothing of the
// magic prefix. This is the only way to assert what the gate PROTECTS, since the real old
// decoder is not in this tree -- and it is the reason emission must be cluster-gated rather
// than per-peer negotiated.
bool preV2DecoderAccepts(const std::string& subBlob) {
    return subBlob.compare(0, kMagicBytes, "TSW2") != 0;
}

// RAII so a failing assertion cannot leave the gate raised for every later test in the
// process (it is thread_local per shard, and the suite shares a reactor).
struct ScopedJournalFormat {
    explicit ScopedJournalFormat(uint32_t version) { JournalFormatGate::activate(version); }
    ~ScopedJournalFormat() { JournalFormatGate::resetForTesting(); }
};
}  // namespace

TEST(ReplicatedCommandCodec, WriteBatchArmIsV1UntilTheClusterCommitsTheV2Activation) {
    JournalFormatGate::resetForTesting();
    ASSERT_EQ(JournalFormatGate::activeVersion(), 1u) << "the gate must FAIL CLOSED by default";
    ASSERT_EQ(JournalFormatGate::writeBatchFormat(), kWriteBatchFormatV1);

    const WriteBatch b = bigBatch();
    const std::string enc = encodeReplicatedCommand(ReplicatedCommand{b});
    const std::string v1 = encodeWriteBatch(b, kWriteBatchFormatV1);
    ASSERT_GT(enc.size(), kSubBlobOffset + kMagicBytes);

    // BYTE-ANCHORED, not merely "not v2": the sub-blob must be exactly what the
    // explicitly-v1 encoder emits, so a change to encodeWriteBatch's DEFAULT cannot slip
    // a different format into every journal entry.
    EXPECT_EQ(enc.compare(kSubBlobOffset, v1.size(), v1), 0)
        << "with the gate off the journal must carry a v1 WriteBatch -- a v2 blob here would be written to journals "
           "no un-upgraded voter (and no older binary replaying them) can read";
    EXPECT_TRUE(preV2DecoderAccepts(enc.substr(kSubBlobOffset, kMagicBytes)));

    // And it is still the real, decodable command.
    auto back = decodeReplicatedCommand(enc);
    ASSERT_TRUE(back.has_value());
    ASSERT_TRUE(std::holds_alternative<WriteBatch>(*back));
    EXPECT_EQ(std::get<WriteBatch>(*back).series[0].timestamps.size(), 500u);

    // `encodeWriteCommand` -- the production propose path, which does not build a
    // ReplicatedCommand variant -- must agree with it exactly.
    EXPECT_EQ(encodeWriteCommand(b), enc);
}

TEST(ReplicatedCommandCodec, WriteBatchArmBecomesV2OnceTheClusterActivatesIt) {
    const WriteBatch b = bigBatch();
    const std::string encV1 = encodeReplicatedCommand(ReplicatedCommand{b});
    {
        ScopedJournalFormat activated(kJournalV2ActivationVersion);
        ASSERT_EQ(JournalFormatGate::writeBatchFormat(), kWriteBatchFormatV2);

        const std::string enc = encodeReplicatedCommand(ReplicatedCommand{b});
        const std::string v2 = encodeWriteBatch(b, kWriteBatchFormatV2);
        ASSERT_GT(enc.size(), kSubBlobOffset + kMagicBytes);
        // Byte-anchored in this direction too.
        EXPECT_EQ(enc.compare(kSubBlobOffset, kMagicBytes, "TSW2"), 0);
        EXPECT_EQ(enc.compare(kSubBlobOffset, v2.size(), v2), 0);
        EXPECT_LT(enc.size(), encV1.size()) << "v2 must actually be smaller, or the activation buys nothing";

        // THE THING THE GATE PROTECTS: a pre-v2 decoder cannot read these bytes. That is
        // why emission is gated on a CLUSTER-WIDE committed activation rather than on a
        // per-peer handshake -- a log entry goes to voters nobody handshook with, and the
        // journal outlives the process that wrote it.
        EXPECT_FALSE(preV2DecoderAccepts(enc.substr(kSubBlobOffset, kMagicBytes)));

        // The CURRENT decoder reads it (and still reads v1 -- see the next test).
        auto back = decodeReplicatedCommand(enc);
        ASSERT_TRUE(back.has_value());
        ASSERT_TRUE(std::holds_alternative<WriteBatch>(*back));
        EXPECT_EQ(std::get<WriteBatch>(*back).series[0].timestamps, b.series[0].timestamps);
        EXPECT_EQ(encodeWriteCommand(b), enc);
    }
    // The gate is monotonic in production; resetForTesting is test-only and exists so one
    // test cannot raise the format for the rest of the process.
    EXPECT_EQ(JournalFormatGate::writeBatchFormat(), kWriteBatchFormatV1);
}

TEST(ReplicatedCommandCodec, AnOldJournalStaysDecodableWhateverTheGateSays) {
    // The decoder is UNCONDITIONALLY bidirectional, and must stay that way: a journal
    // written before the activation is replayed by a binary running after it, on every
    // restart, forever. The gate governs EMISSION only.
    const WriteBatch b = bigBatch();
    JournalFormatGate::resetForTesting();
    const std::string oldEntry = encodeReplicatedCommand(ReplicatedCommand{b});  // v1 bytes
    {
        ScopedJournalFormat activated(kJournalV2ActivationVersion);
        auto back = decodeReplicatedCommand(oldEntry);
        ASSERT_TRUE(back.has_value()) << "a v1 journal entry must decode with the gate RAISED";
        ASSERT_TRUE(std::holds_alternative<WriteBatch>(*back));
        EXPECT_EQ(std::get<WriteBatch>(*back).series[0].timestamps, b.series[0].timestamps);

        const std::string newEntry = encodeReplicatedCommand(ReplicatedCommand{b});
        ASSERT_NE(newEntry, oldEntry);
        auto back2 = decodeReplicatedCommand(newEntry);
        ASSERT_TRUE(back2.has_value());
        EXPECT_EQ(std::get<WriteBatch>(*back2).series[0].timestamps, b.series[0].timestamps);
    }
    // ... and the v2 entry stays decodable after the gate is lowered, which is the shape a
    // ROLLED-BACK-BUT-STILL-v2-CAPABLE binary sees.
    const WriteBatch b2 = bigBatch();
    std::string v2Entry;
    {
        ScopedJournalFormat activated(kJournalV2ActivationVersion);
        v2Entry = encodeReplicatedCommand(ReplicatedCommand{b2});
    }
    auto back3 = decodeReplicatedCommand(v2Entry);
    ASSERT_TRUE(back3.has_value());
    EXPECT_EQ(std::get<WriteBatch>(*back3).series[0].timestamps, b2.series[0].timestamps);
}

TEST(ReplicatedCommandCodec, TheJournalGateOnlyEverRISES) {
    // Not tidiness. Group-0 state is rebuilt by REPLAYING its log, so a mid-replay observer
    // can transiently see an older activeFormatVersion than the one already committed.
    // Lowering the gate on such an observation would emit v1 into a log whose readers had
    // all moved on and then raise it again -- a flapping format. An activation is a
    // decision, not a setting.
    JournalFormatGate::resetForTesting();
    JournalFormatGate::activate(5);
    EXPECT_EQ(JournalFormatGate::activeVersion(), 5u);
    JournalFormatGate::activate(2);
    EXPECT_EQ(JournalFormatGate::activeVersion(), 5u) << "a stale/replayed lower version must not regress the gate";
    JournalFormatGate::activate(1);
    EXPECT_EQ(JournalFormatGate::activeVersion(), 5u);
    // And a version BELOW the activation threshold leaves the codec on v1.
    JournalFormatGate::resetForTesting();
    JournalFormatGate::activate(kJournalV2ActivationVersion - 1);
    EXPECT_EQ(JournalFormatGate::writeBatchFormat(), kWriteBatchFormatV1);
    JournalFormatGate::resetForTesting();
}
