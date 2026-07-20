// Mutation fuzzing for the block decoders.
//
// WHY THIS EXISTS
//
// Every memory bug found in this codebase so far has the same shape: a COUNT or
// LENGTH read from an untrusted source — an on-disk header, or a client-supplied
// protobuf field — used to size an allocation before anything validates it.
// Concrete instances, all since fixed:
//
//   - TSM index blockCount drove reserve() of up to 378 GB
//   - TSM block timestampSize drove reserve() of up to ~34 GB
//   - StringEncoder count (raw client uint32) drove reserve() of up to ~137 GB
//   - LocalIdMap restore counter drove resize() of up to 275 GB, on the STARTUP
//     path, so the shard could not recover by restarting
//   - the FFOR decode reserved its caller-supplied UPPER BOUND rather than the
//     real count, and that inflated capacity was then retained per series
//
// Hand-written tests keep missing these because they exercise well-formed input.
// This drives the decoders with deliberately malformed input instead: valid
// encodings with mutated bytes, truncations, and adversarial count arguments.
//
// WHAT IS ASSERTED
//
// A decoder given garbage may legitimately throw, or return fewer values, or
// return nothing. What it may NOT do is:
//   1. crash / read out of bounds (ASAN catches this when built with it), or
//   2. produce more output than the input could possibly encode.
//
// (2) is the one that matters here: it is the observable form of "sized an
// allocation from an unvalidated number". Every bug listed above would fail it.
//
// Deterministic by construction — a fixed seed per case, and the failure message
// prints the seed and the mutated bytes so any failure is reproducible.

#include "../../../lib/encoding/bool_encoder_rle.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/encoding/string_encoder.hpp"
#include "../../../lib/storage/aligned_buffer.hpp"
#include "../../../lib/storage/slice_buffer.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

namespace {

// A decoded value can never be smaller than one byte of input, whatever the
// encoding — so output count is bounded by input bytes times the densest ratio
// any encoder here achieves. FFOR's constant-delta fast path is the densest at
// 1024 values per 16-byte block = 64 values/byte; allow generous slack above it
// so this asserts "not absurd" rather than re-deriving each encoder's exact
// bound (which would just duplicate the code under test).
constexpr size_t kMaxPlausibleValuesPerByte = 256;

size_t plausibleOutputLimit(size_t inputBytes) {
    return (inputBytes + 1) * kMaxPlausibleValuesPerByte;
}

std::string bytesToHex(const std::vector<uint8_t>& v, size_t limit = 48) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    for (size_t i = 0; i < v.size() && i < limit; ++i) {
        out += kHex[v[i] >> 4];
        out += kHex[v[i] & 0xF];
    }
    if (v.size() > limit)
        out += "...";
    return out;
}

// Build a well-formed encoding, then corrupt it.
std::vector<uint8_t> encodeTimestamps(const std::vector<uint64_t>& values) {
    AlignedBuffer buf;
    IntegerEncoder::encodeInto(values, buf);
    return std::vector<uint8_t>(buf.data.data(), buf.data.data() + buf.size());
}

enum class Mutation { FlipBits, Truncate, ZeroFill, MaxFill, Splice };

void mutate(std::vector<uint8_t>& data, std::mt19937_64& rng, Mutation kind) {
    if (data.empty())
        return;
    std::uniform_int_distribution<size_t> pos(0, data.size() - 1);
    switch (kind) {
        case Mutation::FlipBits: {
            const size_t n = 1 + (rng() % 8);
            for (size_t i = 0; i < n; ++i) {
                data[pos(rng)] ^= static_cast<uint8_t>(1u << (rng() % 8));
            }
            break;
        }
        case Mutation::Truncate:
            data.resize(rng() % data.size());
            break;
        case Mutation::ZeroFill: {
            const size_t start = pos(rng);
            const size_t len = std::min(data.size() - start, static_cast<size_t>(1 + rng() % 16));
            std::fill(data.begin() + start, data.begin() + start + len, uint8_t{0});
            break;
        }
        case Mutation::MaxFill: {
            // Drives length/count fields to their maximum — the exact shape of
            // every allocation bug listed at the top of this file.
            const size_t start = pos(rng);
            const size_t len = std::min(data.size() - start, static_cast<size_t>(1 + rng() % 8));
            std::fill(data.begin() + start, data.begin() + start + len, uint8_t{0xFF});
            break;
        }
        case Mutation::Splice: {
            const size_t a = pos(rng), b = pos(rng);
            std::swap(data[a], data[b]);
            break;
        }
    }
}

const Mutation kAllMutations[] = {Mutation::FlipBits, Mutation::Truncate, Mutation::ZeroFill, Mutation::MaxFill,
                                  Mutation::Splice};

}  // namespace

class DecoderMutationFuzzTest : public ::testing::Test {};

// Timestamps: mutated payloads, with the count argument left honest.
TEST_F(DecoderMutationFuzzTest, IntegerDecoderSurvivesMutatedPayloads) {
    constexpr int kIterations = 4000;

    for (int iter = 0; iter < kIterations; ++iter) {
        std::mt19937_64 rng(0xC0FFEE0000ULL + static_cast<uint64_t>(iter));

        // Vary the source so mutations land on different encoder paths:
        // constant delta (the 16-byte fast path), irregular, and single-block.
        const size_t count = 1 + (rng() % 3000);
        std::vector<uint64_t> values;
        values.reserve(count);
        uint64_t t = 1700000000000000000ULL;
        const int shape = static_cast<int>(rng() % 3);
        for (size_t i = 0; i < count; ++i) {
            if (shape == 0)
                t += 1000000;  // constant delta
            else if (shape == 1)
                t += 1 + (rng() % 1000000);  // irregular
            else
                t += (i % 2) ? 1 : 1000000000;  // alternating
            values.push_back(t);
        }

        auto encoded = encodeTimestamps(values);
        const auto original = encoded;
        mutate(encoded, rng, kAllMutations[rng() % std::size(kAllMutations)]);
        if (encoded.empty())
            continue;

        std::vector<uint64_t> out;
        Slice slice(encoded.data(), encoded.size());
        try {
            IntegerEncoder::decode(slice, static_cast<unsigned int>(count), out);
        } catch (const std::exception&) {
            continue;  // rejecting corrupt input is the correct outcome
        }

        EXPECT_LE(out.size(), plausibleOutputLimit(encoded.size()))
            << "decoder produced " << out.size() << " values from " << encoded.size() << " bytes (iter=" << iter
            << ", bytes=" << bytesToHex(encoded)
            << "); an output that large means a size field was trusted without validation";
    }
}

// The bug class directly: an honest payload with a LIED-ABOUT count. This is
// what a corrupt on-disk header or a hostile protobuf field looks like.
TEST_F(DecoderMutationFuzzTest, IntegerDecoderRejectsAdversarialCounts) {
    const std::vector<uint64_t> values = []() {
        std::vector<uint64_t> v;
        uint64_t t = 1700000000000000000ULL;
        for (int i = 0; i < 500; ++i) {
            t += 1000000;
            v.push_back(t);
        }
        return v;
    }();

    const auto encoded = encodeTimestamps(values);

    // Counts far beyond what the payload can contain. Before the validation
    // fixes, the largest of these reserved tens of GB.
    const unsigned int kLiedCounts[] = {1000, 100000, 10000000, 100000000, 0xFFFFFFFFu};

    for (unsigned int lied : kLiedCounts) {
        std::vector<uint64_t> out;
        Slice slice(encoded.data(), encoded.size());
        try {
            IntegerEncoder::decode(slice, lied, out);
        } catch (const std::exception&) {
            continue;
        }
        EXPECT_LE(out.size(), plausibleOutputLimit(encoded.size()))
            << "claimed count " << lied << " over a " << encoded.size() << "-byte payload yielded " << out.size()
            << " values";
        // The real check: the decoder must not have ALLOCATED for the lie.
        EXPECT_LE(out.capacity(), plausibleOutputLimit(encoded.size()))
            << "decoder reserved capacity " << out.capacity() << " for a claimed count of " << lied << " over only "
            << encoded.size() << " bytes — a size field was trusted without validation";
    }
}

// Strings carry a separate client-supplied count, which is where the ~137 GB
// reserve lived.
TEST_F(DecoderMutationFuzzTest, StringDecoderRejectsAdversarialCounts) {
    std::vector<std::string> values;
    for (int i = 0; i < 200; ++i) {
        values.push_back("value-" + std::to_string(i % 17));
    }

    AlignedBuffer encodedBuf;
    StringEncoder::encodeInto(values, encodedBuf);
    std::vector<uint8_t> encoded(encodedBuf.data.data(), encodedBuf.data.data() + encodedBuf.size());
    ASSERT_FALSE(encoded.empty());

    const size_t kLiedCounts[] = {values.size(), 100000, 10000000, 1000000000, static_cast<size_t>(0xFFFFFFFFu)};

    for (size_t lied : kLiedCounts) {
        std::vector<std::string> out;
        Slice slice(encoded.data(), encoded.size());
        try {
            StringEncoder::decode(slice, lied, out);
        } catch (const std::exception&) {
            continue;
        }
        EXPECT_LE(out.size(), plausibleOutputLimit(encoded.size()))
            << "claimed string count " << lied << " over " << encoded.size() << " bytes yielded " << out.size();
        EXPECT_LE(out.capacity(), plausibleOutputLimit(encoded.size()))
            << "string decoder reserved " << out.capacity() << " slots (32 bytes each) for a claimed count of " << lied
            << " over only " << encoded.size() << " bytes";
    }
}

// Strings again, this time with the payload corrupted rather than the count.
TEST_F(DecoderMutationFuzzTest, StringDecoderSurvivesMutatedPayloads) {
    constexpr int kIterations = 2000;

    for (int iter = 0; iter < kIterations; ++iter) {
        std::mt19937_64 rng(0x5717A9ULL + static_cast<uint64_t>(iter));

        std::vector<std::string> values;
        const size_t count = 1 + (rng() % 200);
        for (size_t i = 0; i < count; ++i) {
            // Mix repetitive (dictionary-eligible) and unique values.
            values.push_back((rng() % 2) ? "repeat" : ("uniq-" + std::to_string(rng())));
        }

        AlignedBuffer encodedBuf;
        StringEncoder::encodeInto(values, encodedBuf);
        std::vector<uint8_t> encoded(encodedBuf.data.data(), encodedBuf.data.data() + encodedBuf.size());
        if (encoded.empty())
            continue;

        mutate(encoded, rng, kAllMutations[rng() % std::size(kAllMutations)]);
        if (encoded.empty())
            continue;

        std::vector<std::string> out;
        Slice slice(encoded.data(), encoded.size());
        try {
            StringEncoder::decode(slice, count, out);
        } catch (const std::exception&) {
            continue;
        }

        EXPECT_LE(out.size(), plausibleOutputLimit(encoded.size()))
            << "string decoder produced " << out.size() << " values from " << encoded.size() << " bytes (iter=" << iter
            << ", bytes=" << bytesToHex(encoded) << ")";

        // Total decoded payload must also stay proportionate to the input.
        size_t totalChars = 0;
        for (const auto& s : out) {
            totalChars += s.size();
        }
        EXPECT_LE(totalChars, encoded.size() * 4096u)
            << "decoded string payload (" << totalChars << " bytes) is implausible for a " << encoded.size()
            << "-byte input (iter=" << iter << ")";
    }
}

// Booleans: RLE run lengths are varints, so a corrupt run length is the
// equivalent unbounded-count hazard.
TEST_F(DecoderMutationFuzzTest, BoolDecoderSurvivesMutatedPayloads) {
    constexpr int kIterations = 2000;

    for (int iter = 0; iter < kIterations; ++iter) {
        std::mt19937_64 rng(0xB001ULL + static_cast<uint64_t>(iter));

        const size_t count = 1 + (rng() % 2000);
        std::vector<bool> values;
        values.reserve(count);
        bool cur = rng() % 2;
        for (size_t i = 0; i < count; ++i) {
            if (rng() % 8 == 0)
                cur = !cur;  // long runs, which is what RLE is for
            values.push_back(cur);
        }

        AlignedBuffer buf;
        BoolEncoderRLE::encodeInto(values, buf);
        std::vector<uint8_t> encoded(buf.data.data(), buf.data.data() + buf.size());
        if (encoded.empty())
            continue;

        mutate(encoded, rng, kAllMutations[rng() % std::size(kAllMutations)]);
        if (encoded.empty())
            continue;

        std::vector<bool> out;
        Slice slice(encoded.data(), encoded.size());
        try {
            BoolEncoderRLE::decode(slice, 0, count, out);
        } catch (const std::exception&) {
            continue;
        }

        // Bools pack 8 per byte, so allow a wider ratio than the shared helper.
        EXPECT_LE(out.size(), count) << "bool decoder produced " << out.size() << " values when only " << count
                                     << " were requested (iter=" << iter << ", bytes=" << bytesToHex(encoded) << ")";
    }
}
