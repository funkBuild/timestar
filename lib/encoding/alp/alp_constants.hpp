#ifndef ALP_CONSTANTS_HPP_INCLUDED
#define ALP_CONSTANTS_HPP_INCLUDED

#include <array>
#include <cstddef>
#include <cstdint>

namespace alp {

// ALP magic number: "ALP\x01" as uint32
static constexpr uint32_t ALP_MAGIC = 0x414C5001;

// Vector size for ALP block processing
static constexpr size_t ALP_VECTOR_SIZE = 1024;

// Maximum exception rate before falling back to ALP_RD
static constexpr double ALP_RD_EXCEPTION_THRESHOLD = 0.50;

// Sample size for (exp, fac) pair selection
static constexpr size_t ALP_SAMPLE_SIZE = 256;

// Encoding scheme identifiers
static constexpr uint8_t SCHEME_ALP = 0;
static constexpr uint8_t SCHEME_ALP_RD = 1;
static constexpr uint8_t SCHEME_ALP_DELTA = 2;  // ALP with delta encoding

// Maximum bit width for FFOR packing
static constexpr uint8_t MAX_BIT_WIDTH = 64;

// ALP_RD: maximum dictionary entries for left-part encoding
static constexpr size_t ALP_RD_MAX_DICT_SIZE = 8;

// Powers of 10 for factoring: FACT_ARR[i] = 10^i (for encoding: value * 10^exp)
static constexpr std::array<double, 19> FACT_ARR = {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8, 1e9,
                                                    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};

// Powers of 10 for fraction: FRAC_ARR[i] = 10^i (for decoding: encoded / 10^fac)
static constexpr std::array<double, 19> FRAC_ARR = {1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8, 1e9,
                                                    1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18};

// Maximum safe integer representable in double (2^53)
static constexpr int64_t MAX_SAFE_INT = (1LL << 53);
static constexpr int64_t MIN_SAFE_INT = -(1LL << 53);

// Number of (exp, fac) combinations
static constexpr size_t EXP_COUNT = 19;
static constexpr size_t FAC_COUNT = 19;

}  // namespace alp

#endif  // ALP_CONSTANTS_HPP_INCLUDED
