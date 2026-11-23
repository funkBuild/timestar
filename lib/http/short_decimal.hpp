#pragma once

#include <glaze/util/itoa.hpp>

#include <cmath>
#include <cstdint>
#include <cstring>

// Text for doubles that are short decimals.
//
// The general double-to-text algorithm (glaze's zmij: shortest text that
// parses back to the same double) costs 12-15 ns a value and is about a third
// of the CPU of a large JSON query response. But most metric values are not
// arbitrary doubles: they are integers, or readings with a few decimal places
// — the nearest doubles to short decimal numbers. For those the answer is
// known without the general algorithm: if v is the double nearest to m / 10^6
// for an integer m below 10^15, then that decimal (trailing zeros dropped) IS
// the shortest text that round-trips. It has at most 15 significant digits, no
// two distinct decimals that short parse to the same double, so nothing
// shorter can also parse to v. Integer arithmetic produces it in 4-7 ns.
//
// The output is byte-for-byte what glz::to_chars writes for the same value, so
// a response does not depend on which writer produced a number;
// response_formatter_bounds_test.cpp holds the two together. Values outside
// the cases below return nullptr and go to the general writer:
//   - magnitude below 1e-4 (scientific notation there), zero and -0;
//   - magnitude of 1e9 or more that is not an integer below 1e15;
//   - anything needing a seventh decimal place, NaN and infinities.
//
// Writes at most 23 characters (sign, 15 digits; or sign, 9 digits, point, 6
// digits — whichever applies) and nothing beyond what it returns except within
// those 23.

namespace timestar::http {

namespace short_decimal_detail {

inline constexpr char kDigitPairs[201] =
    "00010203040506070809101112131415161718192021222324252627282930313233343536373839"
    "40414243444546474849505152535455565758596061626364656667686970717273747576777879"
    "8081828384858687888990919293949596979899";

// Trailing zeros of a two-digit number: 2 for 00, 1 for x0, else 0.
inline constexpr uint8_t kTrailingZeros[100] = {
    2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0};

}  // namespace short_decimal_detail

// Writes `v` at `p` if it is a short decimal and returns the end of the text;
// returns nullptr (having written nothing that matters) otherwise.
inline char* writeShortDecimal(char* p, double v) noexcept {
    using namespace short_decimal_detail;
    const double magnitude = std::fabs(v);

    if (magnitude < 1e9) {
        // Is v exactly the double nearest to m / 10^6? Both m and 10^6 are
        // exact in binary and IEEE division is correctly rounded, so the
        // quotient below is that nearest double.
        const int64_t m = static_cast<int64_t>(magnitude * 1e6 + 0.5);
        if (static_cast<double>(m) / 1e6 != magnitude || m < 100) {
            return nullptr;  // m < 100: zero, or below 1e-4
        }
        *p = '-';
        p += std::signbit(v);
        const uint64_t whole = static_cast<uint64_t>(m) / 1000000u;
        const uint32_t frac = static_cast<uint32_t>(static_cast<uint64_t>(m) % 1000000u);
        p = glz::to_chars(p, whole);
        if (frac == 0) {
            return p;
        }
        *p++ = '.';
        const uint32_t hi = frac / 10000u;
        const uint32_t mid = (frac / 100u) % 100u;
        const uint32_t lo = frac % 100u;
        std::memcpy(p, kDigitPairs + hi * 2, 2);
        std::memcpy(p + 2, kDigitPairs + mid * 2, 2);
        std::memcpy(p + 4, kDigitPairs + lo * 2, 2);
        // Drop trailing zeros; frac != 0, so at least one digit stays.
        unsigned length = 2u - kTrailingZeros[hi];
        length = mid ? 4u - kTrailingZeros[mid] : length;
        length = lo ? 6u - kTrailingZeros[lo] : length;
        return p + length;
    }

    if (magnitude < 1e15) {
        // A large whole number (a counter, a byte total).
        const int64_t m = static_cast<int64_t>(magnitude);
        if (static_cast<double>(m) != magnitude) {
            return nullptr;
        }
        *p = '-';
        p += std::signbit(v);
        return glz::to_chars(p, static_cast<uint64_t>(m));
    }

    return nullptr;  // large magnitudes, NaN, infinities
}

}  // namespace timestar::http
