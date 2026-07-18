#include "../../../lib/index/native/hyperloglog.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>
#include <vector>

// ============================================================================
// Property tests for the Highway-dispatched HyperLogLog SIMD kernels
// (lib/index/native/hyperloglog_simd.cpp) plus HLL-level properties that ride
// on them (deserialize clamping, register-wise-max merge semantics).
//
// Pattern follows test/unit/encoding/alp_simd_correctness_test.cpp: fixed
// seed, lane-boundary size sweep (uint8 lanes can be up to 64 on AVX-512, so
// the sweep covers 0/1, 63/64/65, 127/128/129, and large non-multiples), and
// guard regions to catch out-of-bounds writes.
// ============================================================================

using timestar::index::HyperLogLog;
namespace hllsimd = timestar::index::simd;

namespace {

// Counts covering scalar-tail-only, exact-lane, lane+1 for 16/32/64-byte
// vectors, and the real register-array size (16384).
const std::vector<size_t> kCounts = {0,  1,   2,   3,   7,   8,    15,   16,   17,   31,   32,  33,
                                     63, 64,  65,  127, 128, 129,  255,  256,  257,  1000, 4097,
                                     HyperLogLog::NUM_REGISTERS,  HyperLogLog::NUM_REGISTERS + 1};

constexpr uint8_t kGuardByte = 0xAB;
constexpr size_t kGuardLen = 128;  // > max uint8 lane count on any target

std::vector<uint8_t> randomRegisters(size_t count, std::mt19937_64& rng, int maxValue) {
    std::uniform_int_distribution<int> dist(0, maxValue);
    std::vector<uint8_t> regs(count);
    for (auto& r : regs)
        r = static_cast<uint8_t>(dist(rng));
    return regs;
}

// Scalar reference for hllEstimateSum.
void refEstimateSum(const uint8_t* regs, size_t count, double* sum_out, int* zero_count) {
    double sum = 0.0;
    int zeros = 0;
    for (size_t i = 0; i < count; ++i) {
        sum += std::ldexp(1.0, -static_cast<int>(regs[i]));  // 2^-r
        zeros += (regs[i] == 0);
    }
    *sum_out = sum;
    *zero_count = zeros;
}

// Element-wise max of two serialized register arrays == the union sketch.
std::string registerMax(const std::string& a, const std::string& b) {
    EXPECT_EQ(a.size(), b.size());
    std::string out(a.size(), '\0');
    for (size_t i = 0; i < a.size(); ++i)
        out[i] = static_cast<char>(std::max(static_cast<uint8_t>(a[i]), static_cast<uint8_t>(b[i])));
    return out;
}

}  // namespace

class HyperLogLogSimdTest : public ::testing::Test {};

// ==================== hllClampRegisters ====================

TEST_F(HyperLogLogSimdTest, ClampMatchesScalarSweepWithGuard) {
    std::mt19937_64 rng(20260716);
    const uint8_t maxVals[] = {0, 1, 51, 63, 254};

    for (size_t count : kCounts) {
        for (uint8_t maxVal : maxVals) {
            auto original = randomRegisters(count, rng, 255);

            std::vector<uint8_t> buf(count + kGuardLen, kGuardByte);
            std::copy(original.begin(), original.end(), buf.begin());

            hllsimd::hllClampRegisters(buf.data(), count, maxVal);

            for (size_t i = 0; i < count; ++i) {
                uint8_t want = std::min(original[i], maxVal);
                ASSERT_EQ(buf[i], want) << "clamp mismatch count=" << count << " maxVal=" << int(maxVal)
                                        << " i=" << i << " in=" << int(original[i]);
            }
            for (size_t i = count; i < buf.size(); ++i) {
                ASSERT_EQ(buf[i], kGuardByte)
                    << "clamp wrote past count=" << count << " at offset " << (i - count);
            }
        }
    }
}

TEST_F(HyperLogLogSimdTest, ClampIsIdempotent) {
    std::mt19937_64 rng(20260716);
    auto regs = randomRegisters(1000, rng, 255);
    hllsimd::hllClampRegisters(regs.data(), regs.size(), 51);
    auto once = regs;
    hllsimd::hllClampRegisters(regs.data(), regs.size(), 51);
    EXPECT_EQ(regs, once);
    EXPECT_LE(*std::max_element(regs.begin(), regs.end()), 51);
}

// ==================== hllEstimateSum ====================

TEST_F(HyperLogLogSimdTest, EstimateSumMatchesScalarSweep) {
    std::mt19937_64 rng(20260716);
    // maxValue 63 exercises the full LUT range (real registers are <= 51).
    const int maxValues[] = {0, 3, 51, 63};

    for (size_t count : kCounts) {
        for (int maxValue : maxValues) {
            auto regs = randomRegisters(count, rng, maxValue);

            double simdSum = -1.0;
            int simdZeros = -1;
            hllsimd::hllEstimateSum(regs.data(), count, &simdSum, &simdZeros);

            double refSum = -1.0;
            int refZeros = -1;
            refEstimateSum(regs.data(), count, &refSum, &refZeros);

            ASSERT_EQ(simdZeros, refZeros) << "zero count mismatch count=" << count << " maxValue=" << maxValue;
            // SIMD lane accumulation reassociates the sum; allow tiny relative slack.
            ASSERT_NEAR(simdSum, refSum, std::max(1e-12, refSum * 1e-12))
                << "harmonic sum mismatch count=" << count << " maxValue=" << maxValue;
        }
    }
}

TEST_F(HyperLogLogSimdTest, EstimateSumAllZeroRegisters) {
    // 2^0 = 1.0 per register: integer arithmetic, must be exact in any
    // accumulation order.
    for (size_t count : {size_t(1), size_t(64), size_t(1000), HyperLogLog::NUM_REGISTERS}) {
        std::vector<uint8_t> regs(count, 0);
        double sum = -1.0;
        int zeros = -1;
        hllsimd::hllEstimateSum(regs.data(), count, &sum, &zeros);
        EXPECT_DOUBLE_EQ(sum, static_cast<double>(count)) << "count=" << count;
        EXPECT_EQ(zeros, static_cast<int>(count)) << "count=" << count;
    }
}

TEST_F(HyperLogLogSimdTest, EstimateSumMaxRankRegisters) {
    // All registers at the clamp limit (51): each term is exactly 2^-51 and
    // the sum of <= 16384 equal powers of two is exact.
    constexpr size_t kCount = HyperLogLog::NUM_REGISTERS;
    std::vector<uint8_t> regs(kCount, 51);
    double sum = -1.0;
    int zeros = -1;
    hllsimd::hllEstimateSum(regs.data(), kCount, &sum, &zeros);
    EXPECT_DOUBLE_EQ(sum, static_cast<double>(kCount) * std::ldexp(1.0, -51));
    EXPECT_EQ(zeros, 0);
}

TEST_F(HyperLogLogSimdTest, EstimateSumEmptyInput) {
    double sum = -1.0;
    int zeros = -1;
    hllsimd::hllEstimateSum(nullptr, 0, &sum, &zeros);
    EXPECT_EQ(sum, 0.0);
    EXPECT_EQ(zeros, 0);
}

// ==================== HLL-level properties on top of the kernels ====================

TEST_F(HyperLogLogSimdTest, DeserializeClampsOversizedRegisters) {
    // Adversarial input: every register 0xFF. Without the clamp this shifts
    // 1ULL << 255 (UB); with it every register becomes 51 and the estimate is
    // finite and huge.
    std::string data(HyperLogLog::SERIALIZED_SIZE, static_cast<char>(0xFF));
    auto hll = HyperLogLog::deserialize(data);
    EXPECT_FALSE(hll.empty());

    double est = hll.estimate();
    EXPECT_TRUE(std::isfinite(est));
    EXPECT_GT(est, static_cast<double>(HyperLogLog::NUM_REGISTERS));

    std::string out;
    hll.serialize(out);
    ASSERT_EQ(out.size(), HyperLogLog::SERIALIZED_SIZE);
    for (size_t i = 0; i < out.size(); ++i) {
        ASSERT_LE(static_cast<uint8_t>(out[i]), 51u) << "register " << i << " not clamped";
    }
}

TEST_F(HyperLogLogSimdTest, DeserializeTruncatedDataYieldsEmptySketch) {
    std::string tooShort(HyperLogLog::SERIALIZED_SIZE - 1, static_cast<char>(0x07));
    auto hll = HyperLogLog::deserialize(tooShort);
    EXPECT_TRUE(hll.empty());
    EXPECT_DOUBLE_EQ(hll.estimate(), 0.0);

    auto fromEmpty = HyperLogLog::deserialize(std::string_view{});
    EXPECT_TRUE(fromEmpty.empty());
}

TEST_F(HyperLogLogSimdTest, SketchIsDeterministic) {
    HyperLogLog a, b;
    for (uint32_t i = 0; i < 5000; ++i) {
        a.add(i);
        b.add(i);
    }
    std::string sa, sb;
    a.serialize(sa);
    b.serialize(sb);
    EXPECT_EQ(sa, sb) << "identical inserts must produce identical sketches";

    // Re-adding the same set must not change the sketch (add is idempotent
    // per value: registers only take max).
    for (uint32_t i = 0; i < 5000; ++i)
        a.add(i);
    std::string sa2;
    a.serialize(sa2);
    EXPECT_EQ(sa, sa2);
}

TEST_F(HyperLogLogSimdTest, RegisterMaxMergeCommutativeIdempotentAccurate) {
    HyperLogLog a, b;
    for (uint32_t i = 0; i < 6000; ++i)
        a.add(i);
    for (uint32_t i = 3000; i < 9000; ++i)
        b.add(i);
    // True union cardinality: 0..8999 = 9000.

    std::string sa, sb;
    a.serialize(sa);
    b.serialize(sb);

    auto ab = registerMax(sa, sb);
    auto ba = registerMax(sb, sa);
    EXPECT_EQ(ab, ba) << "register-wise max merge must be commutative";
    EXPECT_EQ(registerMax(sa, sa), sa) << "register-wise max merge must be idempotent";

    auto merged = HyperLogLog::deserialize(ab);
    double estU = merged.estimate();
    double estA = a.estimate();
    double estB = b.estimate();

    // Union estimate can never be below either operand's estimate (registers
    // dominate pointwise; both sketches are in the linear-counting regime).
    EXPECT_GE(estU, estA - 1e-9);
    EXPECT_GE(estU, estB - 1e-9);

    double err = std::abs(estU - 9000.0) / 9000.0;
    EXPECT_LT(err, 0.05) << "union estimate " << estU << " for true cardinality 9000";
}

TEST_F(HyperLogLogSimdTest, LinearCountingSmallCardinality) {
    // Small cardinalities go through the zeros-based linear-counting branch,
    // which depends on the SIMD zero_count being exact.
    HyperLogLog hll;
    constexpr int kN = 100;
    for (uint32_t i = 0; i < kN; ++i)
        hll.add(i);
    double est = hll.estimate();
    double err = std::abs(est - kN) / kN;
    EXPECT_LT(err, 0.05) << "estimated " << est << " for " << kN;
}

TEST_F(HyperLogLogSimdTest, MixedKeyTypesAccuracy) {
    // string_view keys hash through the same addHash path.
    HyperLogLog hll;
    constexpr int kN = 20000;
    for (int i = 0; i < kN; ++i) {
        std::string key = "series/" + std::to_string(i) + "/field";
        hll.add(std::string_view(key));
    }
    double est = hll.estimate();
    double err = std::abs(est - kN) / kN;
    EXPECT_LT(err, 0.05) << "estimated " << est << " for " << kN << " string keys";
}
