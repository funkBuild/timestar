// Tests for the encodeInto() variants of FloatEncoder, IntegerEncoder, and
// StringEncoder.  BoolEncoder::encodeInto() is already covered by
// BoolEncoderTest.EncodeIntoEdgeSizes in bool_encoder_test.cpp.
//
// encodeInto() is the WAL write-path variant that writes directly into a
// caller-supplied AlignedBuffer instead of allocating and returning a new
// buffer.  It must produce identical data to encode() and the result must
// survive a round-trip through the corresponding decoder.

#include "aligned_buffer.hpp"
#include "compressed_buffer.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "slice_buffer.hpp"
#include "string_encoder.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

// ---------------------------------------------------------------------------
// Helper: build a CompressedSlice from an AlignedBuffer that was written by
// FloatEncoder::encodeInto().  The float encoder writes packed uint64_t words;
// the byte count must be a multiple of 8.
// ---------------------------------------------------------------------------
static CompressedSlice makeFloatSlice(const AlignedBuffer& buf, size_t byteOffset = 0) {
    const uint8_t* ptr = buf.data.data() + byteOffset;
    size_t byteLen = buf.size() - byteOffset;
    // Zero-copy path requires multiple of 8.  ALP always writes 8-byte words.
    if (byteLen % 8 == 0) {
        return CompressedSlice(CompressedSlice::ZeroCopy{}, ptr, byteLen);
    }
    return CompressedSlice(ptr, byteLen);
}

// ---------------------------------------------------------------------------
// FloatEncoder::encodeInto tests
// ---------------------------------------------------------------------------

TEST(FloatEncodeIntoTest, EmptyInputWritesZeroBytes) {
    AlignedBuffer buf;
    size_t written = FloatEncoder::encodeInto({}, buf);
    EXPECT_EQ(written, 0u);
    EXPECT_EQ(buf.size(), 0u);
}

TEST(FloatEncodeIntoTest, SingleValueRoundTrip) {
    std::vector<double> values = {42.0};
    AlignedBuffer buf;
    size_t written = FloatEncoder::encodeInto(values, buf);
    EXPECT_GT(written, 0u);
    EXPECT_EQ(buf.size(), written);

    CompressedSlice slice = makeFloatSlice(buf);
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    EXPECT_DOUBLE_EQ(decoded[0], values[0]);
}

TEST(FloatEncodeIntoTest, SmallInputRoundTrip) {
    std::vector<double> values = {1.1, 2.2, 3.3, 4.4, 5.5};
    AlignedBuffer buf;
    size_t written = FloatEncoder::encodeInto(values, buf);
    EXPECT_GT(written, 0u);

    CompressedSlice slice = makeFloatSlice(buf);
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_DOUBLE_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(FloatEncodeIntoTest, LargerInputRoundTrip) {
    std::vector<double> values;
    values.reserve(1000);
    for (int i = 0; i < 1000; i++) {
        values.push_back(std::sin(i * 0.01) * 100.0);
    }

    AlignedBuffer buf;
    FloatEncoder::encodeInto(values, buf);

    CompressedSlice slice = makeFloatSlice(buf);
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_DOUBLE_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(FloatEncodeIntoTest, NaNRoundTrip) {
    std::vector<double> values = {1.0, std::numeric_limits<double>::quiet_NaN(), 3.0};
    AlignedBuffer buf;
    FloatEncoder::encodeInto(values, buf);

    CompressedSlice slice = makeFloatSlice(buf);
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    EXPECT_DOUBLE_EQ(decoded[0], 1.0);
    EXPECT_TRUE(std::isnan(decoded[1]));
    EXPECT_DOUBLE_EQ(decoded[2], 3.0);
}

TEST(FloatEncodeIntoTest, InfRoundTrip) {
    std::vector<double> values = {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
                                  0.0};
    AlignedBuffer buf;
    FloatEncoder::encodeInto(values, buf);

    CompressedSlice slice = makeFloatSlice(buf);
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    EXPECT_EQ(decoded[0], std::numeric_limits<double>::infinity());
    EXPECT_EQ(decoded[1], -std::numeric_limits<double>::infinity());
    EXPECT_DOUBLE_EQ(decoded[2], 0.0);
}

TEST(FloatEncodeIntoTest, MatchesEncodeOutput) {
    // encodeInto must produce the same bytes as encode() followed by
    // extracting the raw words from the CompressedBuffer.
    std::vector<double> values;
    for (int i = 0; i < 200; i++) {
        values.push_back(i * 0.5 + 0.1);
    }

    // Via encodeInto
    AlignedBuffer buf;
    FloatEncoder::encodeInto(values, buf);

    // Via encode
    CompressedBuffer cb = FloatEncoder::encode(values);
    const size_t cbBytes = cb.data.size() * sizeof(uint64_t);

    ASSERT_EQ(buf.size(), cbBytes) << "encodeInto byte count should match encode() CompressedBuffer size";
    EXPECT_EQ(0, std::memcmp(buf.data.data(), reinterpret_cast<const uint8_t*>(cb.data.data()), cbBytes))
        << "encodeInto content should be identical to encode()";
}

TEST(FloatEncodeIntoTest, AppendsToExistingBuffer) {
    // Verify encodeInto appends after existing content rather than
    // overwriting from the start.
    std::vector<double> values = {7.0, 8.0, 9.0};

    AlignedBuffer buf;
    // Pre-fill with 16 sentinel bytes.
    const size_t sentinelSize = 16;
    for (size_t i = 0; i < sentinelSize; i++) {
        buf.write(static_cast<uint8_t>(0xAB));
    }
    ASSERT_EQ(buf.size(), sentinelSize);

    size_t written = FloatEncoder::encodeInto(values, buf);
    EXPECT_EQ(buf.size(), sentinelSize + written);

    // Sentinel bytes must be undisturbed.
    for (size_t i = 0; i < sentinelSize; i++) {
        EXPECT_EQ(buf.data[i], 0xAB) << "Sentinel byte at " << i << " was overwritten";
    }

    // Encoded data must still decode correctly.
    CompressedSlice slice = makeFloatSlice(buf, sentinelSize);
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, values.size(), decoded);

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_DOUBLE_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

// ---------------------------------------------------------------------------
// IntegerEncoder::encodeInto tests
// ---------------------------------------------------------------------------

// Helper: encode via encodeInto, then decode and verify
static void verifyIntRoundTripViaEncodeInto(const std::vector<uint64_t>& input) {
    AlignedBuffer buf;
    size_t written = IntegerEncoder::encodeInto(input, buf);
    EXPECT_GT(written, 0u) << "encodeInto wrote 0 bytes for non-empty input";
    EXPECT_EQ(buf.size(), written);

    Slice slice(buf.data.data(), buf.size());
    std::vector<uint64_t> decoded;
    auto [skipped, added] =
        IntegerEncoder::decode(slice, static_cast<unsigned int>(input.size()), decoded, 0, UINT64_MAX);

    ASSERT_EQ(added, input.size());
    ASSERT_EQ(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i;
    }
}

TEST(IntegerEncodeIntoTest, EmptyInput) {
    AlignedBuffer buf;
    size_t written = IntegerEncoder::encodeInto({}, buf);
    // An empty span may produce a small header or zero bytes; either is fine.
    // The important property is that the function returns without crashing.
    (void)written;
}

TEST(IntegerEncodeIntoTest, SingleValue) {
    verifyIntRoundTripViaEncodeInto({12345678901234ULL});
}

TEST(IntegerEncodeIntoTest, SmallMonotonicInput) {
    verifyIntRoundTripViaEncodeInto({100, 200, 300, 400, 500});
}

TEST(IntegerEncodeIntoTest, LargerInput) {
    std::vector<uint64_t> values;
    values.reserve(500);
    uint64_t ts = 1'000'000'000ULL;
    for (int i = 0; i < 500; i++) {
        values.push_back(ts);
        ts += 1'000'000ULL;  // 1 ms increments in nanoseconds
    }
    verifyIntRoundTripViaEncodeInto(values);
}

TEST(IntegerEncodeIntoTest, MaxMinValues) {
    verifyIntRoundTripViaEncodeInto({0, UINT64_MAX / 2, UINT64_MAX});
}

TEST(IntegerEncodeIntoTest, MatchesEncodeOutput) {
    std::vector<uint64_t> values;
    for (uint64_t i = 0; i < 100; i++) {
        values.push_back(i * 1000000ULL);
    }

    // Via encodeInto
    AlignedBuffer bufInto;
    IntegerEncoder::encodeInto(values, bufInto);

    // Via encode
    AlignedBuffer bufEncode = IntegerEncoder::encode(values);

    ASSERT_EQ(bufInto.size(), bufEncode.size()) << "encodeInto and encode should produce the same byte count";
    EXPECT_EQ(0, std::memcmp(bufInto.data.data(), bufEncode.data.data(), bufInto.size()))
        << "encodeInto and encode should produce identical bytes";
}

TEST(IntegerEncodeIntoTest, AppendsToExistingBuffer) {
    std::vector<uint64_t> values = {10, 20, 30};

    AlignedBuffer buf;
    // Pre-fill with 8 sentinel bytes.
    const size_t sentinelSize = 8;
    for (size_t i = 0; i < sentinelSize; i++) {
        buf.write(static_cast<uint8_t>(0xCD));
    }

    size_t written = IntegerEncoder::encodeInto(values, buf);
    EXPECT_EQ(buf.size(), sentinelSize + written);

    // Sentinel must be undisturbed.
    for (size_t i = 0; i < sentinelSize; i++) {
        EXPECT_EQ(buf.data[i], 0xCD) << "Sentinel byte at " << i << " was overwritten";
    }

    // Encoded payload must decode correctly.
    Slice slice(buf.data.data() + sentinelSize, written);
    std::vector<uint64_t> decoded;
    auto [skipped, added] =
        IntegerEncoder::decode(slice, static_cast<unsigned int>(values.size()), decoded, 0, UINT64_MAX);
    ASSERT_EQ(added, values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

// ---------------------------------------------------------------------------
// StringEncoder::encodeInto tests
// ---------------------------------------------------------------------------

// Helper: encode via encodeInto into an AlignedBuffer, then decode and verify
static void verifyStringRoundTripViaEncodeInto(const std::vector<std::string>& input) {
    AlignedBuffer buf;
    size_t written = StringEncoder::encodeInto(input, buf);
    EXPECT_GT(written, 0u) << "encodeInto wrote 0 bytes";
    EXPECT_EQ(buf.size(), written);

    std::vector<std::string> decoded;
    StringEncoder::decode(buf, input.size(), decoded);

    ASSERT_EQ(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i;
    }
}

TEST(StringEncodeIntoTest, EmptyVector) {
    AlignedBuffer buf;
    size_t written = StringEncoder::encodeInto({}, buf);
    // Must write at least the 16-byte header.
    EXPECT_GE(written, 16u);
    EXPECT_EQ(buf.size(), written);

    // The encodeInto() empty fast path writes compressedSize=0 in the header
    // (no actual Snappy stream) whereas encode() still runs Snappy on an empty
    // input and writes a minimal non-zero Snappy stream.  These two paths
    // produce different on-wire bytes for the empty case.  Verify that
    // encodeInto's header is structurally valid by checking the magic number.
    uint32_t magic;
    std::memcpy(&magic, buf.data.data(), 4);
    EXPECT_EQ(magic, 0x53545247u);  // "STRG"

    uint32_t count;
    std::memcpy(&count, buf.data.data() + 12, 4);
    EXPECT_EQ(count, 0u);
}

TEST(StringEncodeIntoTest, SingleString) {
    verifyStringRoundTripViaEncodeInto({"hello world"});
}

TEST(StringEncodeIntoTest, MultipleStrings) {
    verifyStringRoundTripViaEncodeInto({"first", "second", "third string with spaces", "fourth"});
}

TEST(StringEncodeIntoTest, EmptyStringsInVector) {
    // Strings that are themselves empty (zero-length).
    verifyStringRoundTripViaEncodeInto({"", "non-empty", "", ""});
}

TEST(StringEncodeIntoTest, LargeStrings) {
    std::vector<std::string> input;
    for (int i = 0; i < 5; i++) {
        input.emplace_back(2000 + i * 500, 'a' + i);
    }
    verifyStringRoundTripViaEncodeInto(input);
}

TEST(StringEncodeIntoTest, SpecialCharacters) {
    verifyStringRoundTripViaEncodeInto({"Tab:\there", "Newline:\nhere",
                                        "UTF-8: \xc3\xa9\xc3\xbc\xe4\xb8\xad\xe6\x96\x87", "Symbols: !@#$%^&*()",
                                        "JSON: {\"key\": \"value\"}"});
}

TEST(StringEncodeIntoTest, ManyStrings) {
    std::vector<std::string> input;
    input.reserve(500);
    for (int i = 0; i < 500; i++) {
        input.push_back("sensor_reading_" + std::to_string(i));
    }
    verifyStringRoundTripViaEncodeInto(input);
}

TEST(StringEncodeIntoTest, MatchesEncodeOutput) {
    // encodeInto must produce the same bytes as encode().
    std::vector<std::string> input = {"alpha", "beta", "gamma", "delta", "epsilon"};

    // Via encodeInto
    AlignedBuffer bufInto;
    StringEncoder::encodeInto(input, bufInto);

    // Via encode
    AlignedBuffer bufEncode = StringEncoder::encode(input);

    ASSERT_EQ(bufInto.size(), bufEncode.size()) << "encodeInto and encode should produce the same byte count";
    EXPECT_EQ(0, std::memcmp(bufInto.data.data(), bufEncode.data.data(), bufInto.size()))
        << "encodeInto and encode should produce identical bytes";
}

TEST(StringEncodeIntoTest, AppendsToExistingBuffer) {
    std::vector<std::string> input = {"one", "two", "three"};

    AlignedBuffer buf;
    // Write 8 sentinel bytes before the encoded data.
    const size_t sentinelSize = 8;
    for (size_t i = 0; i < sentinelSize; i++) {
        buf.write(static_cast<uint8_t>(0xEF));
    }

    size_t written = StringEncoder::encodeInto(input, buf);
    EXPECT_EQ(buf.size(), sentinelSize + written);

    // Sentinel bytes must be undisturbed.
    for (size_t i = 0; i < sentinelSize; i++) {
        EXPECT_EQ(buf.data[i], 0xEF) << "Sentinel byte at " << i << " was overwritten";
    }

    // Encoded payload must decode correctly.
    // Build an AlignedBuffer view over the payload slice.
    // decode(AlignedBuffer&) reads from offset 0, so we need to create a
    // sub-buffer containing only the encoded payload.
    AlignedBuffer payloadBuf(written);
    std::memcpy(payloadBuf.data.data(), buf.data.data() + sentinelSize, written);

    std::vector<std::string> decoded;
    StringEncoder::decode(payloadBuf, input.size(), decoded);
    ASSERT_EQ(decoded.size(), input.size());
    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i;
    }
}

TEST(StringEncodeIntoTest, VarIntBoundaryStrings) {
    // Exercise varint encoding for strings near byte-boundary lengths.
    std::vector<std::string> input;
    input.emplace_back(127, 'x');  // max 1-byte varint length
    input.emplace_back(128, 'y');  // min 2-byte varint length
    input.emplace_back(255, 'z');
    input.emplace_back(256, 'a');
    verifyStringRoundTripViaEncodeInto(input);
}
