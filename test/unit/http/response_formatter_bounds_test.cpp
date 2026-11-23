// The JSON formatter reserves its output from per-type worst-case lengths and
// writes numbers straight into that storage, so those lengths are a memory-
// safety contract, not a tuning knob. These tests hold the contract to the
// number writer itself, and push the longest possible values through the
// formatter at a size where there is no slack left to hide behind.

#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/response_formatter.hpp"
#include "../../../lib/http/short_decimal.hpp"

#include <glaze/util/itoa.hpp>
#include <glaze/util/zmij.hpp>

#include <gtest/gtest.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <string>
#include <vector>

using namespace timestar;

namespace {

constexpr size_t kMaxDoubleChars = 24;
// The double writer may use this many bytes from a value's start as scratch,
// whatever the final length (its documented contract).
constexpr size_t kDoubleScratch = glz::zmij::double_buffer_size;
constexpr size_t kMaxIntChars = 20;

// Length of the text written for `value`, and how far into the buffer the
// writer touched at all (it pre-fills scratch digits past the final length).
template <class T>
std::pair<size_t, size_t> writtenExtent(T value) {
    char buffer[96];
    std::memset(buffer, 0x7f, sizeof buffer);
    const char* end = glz::to_chars(buffer, value);
    size_t touched = 0;
    for (size_t i = 0; i < sizeof buffer; ++i) {
        if (buffer[i] != 0x7f) {
            touched = i + 1;
        }
    }
    return {static_cast<size_t>(end - buffer), touched};
}

}  // namespace

TEST(ResponseFormatterBoundsTest, NumberWriterStaysWithinTheReservedWidths) {
    std::mt19937_64 rng(99);
    size_t longest = 0;
    size_t furthest = 0;
    auto probe = [&](double v) {
        const auto [length, touched] = writtenExtent(v);
        longest = std::max(longest, length);
        furthest = std::max(furthest, touched);
    };
    for (int i = 0; i < 2'000'000; ++i) {
        const double v = std::bit_cast<double>(rng());
        if (std::isfinite(v)) {
            probe(v);
        }
        // Small negative magnitudes exercise the fixed-notation path.
        probe(-std::ldexp(static_cast<double>(rng() % (1ULL << 53)) + 0.5, static_cast<int>(rng() % 80) - 110));
    }
    for (double v : {std::numeric_limits<double>::max(), -std::numeric_limits<double>::max(),
                     -std::numeric_limits<double>::min(), -std::numeric_limits<double>::denorm_min(),
                     -4.3602479229489575e-06, -1.2345678901234567e-5, -1.2345678901234567e20, -9.999999999999999e20,
                     1e21, -1e22, 123456789012345680000.0, -1.5131402663111935e-222, -1.8395347440392536e+199}) {
        probe(v);
    }
    EXPECT_EQ(longest, kMaxDoubleChars);
    EXPECT_LE(furthest, kDoubleScratch);

    for (uint64_t v : {uint64_t{0}, std::numeric_limits<uint64_t>::max(), uint64_t{1} << 63}) {
        EXPECT_LE(writtenExtent(v).second, kMaxIntChars);
        EXPECT_LE(writtenExtent(static_cast<int64_t>(v)).second, kMaxIntChars);
    }
    EXPECT_LE(writtenExtent(std::numeric_limits<int64_t>::min()).second, kMaxIntChars);
}

TEST(ResponseFormatterBoundsTest, LongestValuesFormatCorrectlyAtEverySize) {
    const double widest = -1.5131402663111935e-222;  // 24 characters
    ASSERT_EQ(writtenExtent(widest).first, kMaxDoubleChars);

    // Sizes around the slice boundary and well past it, so the widest values
    // fill the reservation exactly, slice after slice.
    for (size_t points : {size_t{1}, size_t{7}, size_t{65535}, size_t{65536}, size_t{65537}, size_t{200001}}) {
        QueryResponse response;
        response.success = true;
        SeriesResult series;
        series.measurement = "m";
        std::vector<uint64_t> timestamps(points, std::numeric_limits<uint64_t>::max());
        std::vector<double> doubles(points, widest);
        std::vector<int64_t> integers(points, std::numeric_limits<int64_t>::min());
        series.fields["d"] = std::make_pair(timestamps, FieldValues(doubles));
        series.fields["i"] = std::make_pair(timestamps, FieldValues(integers));
        response.series.push_back(std::move(series));
        response.statistics.seriesCount = 1;
        response.statistics.pointCount = points * 2;

        const auto body = ResponseFormatter::formatBody(response).get();
        const std::string text(body.data(), body.size());

        const std::string ts = "18446744073709551615";
        const std::string dv = "-1.5131402663111935E-222";
        const std::string iv = "-9223372036854775808";
        auto repeat = [points](const std::string& item) {
            std::string out;
            out.reserve(points * (item.size() + 1));
            for (size_t i = 0; i < points; ++i) {
                if (i) {
                    out += ',';
                }
                out += item;
            }
            return out;
        };
        const std::string expectedDoubles =
            "\"d\":{\"timestamps\":[" + repeat(ts) + "],\"values\":[" + repeat(dv) + "]}";
        const std::string expectedIntegers =
            "\"i\":{\"timestamps\":[" + repeat(ts) + "],\"values\":[" + repeat(iv) + "]}";
        ASSERT_NE(text.find(expectedDoubles), std::string::npos) << "points=" << points;
        ASSERT_NE(text.find(expectedIntegers), std::string::npos) << "points=" << points;
        ASSERT_EQ(text.back(), '}');
        // The std::string overload is the same bytes.
        if (points <= 7) {
            QueryResponse again;
            again.success = true;
            SeriesResult s2;
            s2.measurement = "m";
            s2.fields["d"] = std::make_pair(timestamps, FieldValues(doubles));
            s2.fields["i"] = std::make_pair(timestamps, FieldValues(integers));
            again.series.push_back(std::move(s2));
            again.statistics.seriesCount = 1;
            again.statistics.pointCount = points * 2;
            EXPECT_EQ(ResponseFormatter::format(again).get(), text);
        }
    }
}

// Short decimals are written by a dedicated fast path. A response must not
// depend on which writer produced a number, so wherever the fast path answers
// its bytes must be exactly the general writer's — and it must stay inside the
// width reserved per value.
TEST(ResponseFormatterBoundsTest, ShortDecimalWriterMatchesTheGeneralWriter) {
    std::mt19937_64 rng(2026);
    size_t taken = 0;
    size_t longest = 0;
    auto check = [&](double v) {
        char fast[64];
        char general[64];
        std::memset(fast, 0x7f, sizeof fast);
        const char* end = timestar::http::writeShortDecimal(fast, v);
        if (!end) {
            return;
        }
        ++taken;
        const size_t length = static_cast<size_t>(end - fast);
        longest = std::max(longest, length);
        const char* generalEnd = glz::to_chars(general, v);
        ASSERT_EQ(std::string(fast, length), std::string(general, static_cast<size_t>(generalEnd - general)))
            << "value " << std::hexfloat << v;
        size_t touched = 0;
        for (size_t i = 0; i < sizeof fast; ++i) {
            if (fast[i] != 0x7f) {
                touched = i + 1;
            }
        }
        ASSERT_LE(touched, kMaxDoubleChars) << "value " << std::hexfloat << v;
    };

    static const double scales[] = {1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8};
    for (int i = 0; i < 3'000'000; ++i) {
        // Decimals of every scale and magnitude, their neighbours (which are
        // NOT short decimals), and arbitrary bit patterns.
        const double v = static_cast<double>(static_cast<int64_t>(rng() >> (rng() % 50)) % 2'000'000'000'000'000LL) /
                         scales[rng() % 9];
        check(v);
        check(-v);
        check(std::nextafter(v, 1e300));
        check(std::nextafter(v, -1e300));
        check(std::bit_cast<double>(rng()));
        const double whole = static_cast<double>(rng() % 2'000'000'000'000'000ULL);
        check(whole);
        check(whole + 0.5);
        if (HasFatalFailure()) {
            return;
        }
    }
    for (double v : {0.0,
                     -0.0,
                     1e-4,
                     9.9e-5,
                     0.00011,
                     0.000001,
                     1e-7,
                     0.1,
                     0.2,
                     0.3,
                     0.7,
                     0.5,
                     1.0,
                     100.0,
                     123456.789012,
                     999999999.999999,
                     1e9,
                     1e9 + 0.5,
                     999999999999999.0,
                     1e15,
                     1e15 + 2,
                     1e16,
                     9007199254740992.0,
                     5e-324,
                     1.7976931348623157e308,
                     std::numeric_limits<double>::quiet_NaN(),
                     std::numeric_limits<double>::infinity()}) {
        check(v);
        check(-v);
    }
    // The fast path is exercised, not vacuous, and its longest output fits.
    EXPECT_GT(taken, size_t{3'000'000});
    EXPECT_LE(longest, kMaxDoubleChars);

    // What it must decline, so the general writer's notation is what appears.
    char buf[64];
    for (double v : {0.0, -0.0, 9.9e-5, 1e-7, 1e15, 1e16, 1e9 + 0.5, 0.1234567, 1.0 / 3.0,
                     std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()}) {
        EXPECT_EQ(timestar::http::writeShortDecimal(buf, v), nullptr) << v;
    }
    // And what it must take.
    for (double v : {1.0, -1.0, 0.5, 23.125, 0.0001, 999999999.999999, 1e9, 123456789012345.0, -42.0}) {
        EXPECT_NE(timestar::http::writeShortDecimal(buf, v), nullptr) << v;
    }
}
