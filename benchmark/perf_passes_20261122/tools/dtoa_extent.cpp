#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <glaze/util/zmij.hpp>
#include <glaze/util/itoa.hpp>
#include <bit>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <random>
int main() {
    std::mt19937_64 rng(1);
    size_t maxLen = 0, maxExtent = 0, maxILen = 0, maxIExt = 0; double worstLen = 0, worstExt = 0;
    char buf[128];
    auto probe = [&](double v) {
        std::memset(buf, 0x7f, sizeof buf);
        char* end = glz::to_chars(buf, v);
        size_t len = size_t(end - buf);
        size_t ext = 0;
        for (size_t i = 0; i < sizeof buf; ++i) if (buf[i] != 0x7f) ext = i + 1;
        // 0x7f could be legitimately absent from output (digits only), so ext is exact
        if (len > maxLen) { maxLen = len; worstLen = v; }
        if (ext > maxExtent) { maxExtent = ext; worstExt = v; }
    };
    for (size_t i = 0; i < 300000000ULL; ++i) {
        uint64_t bits = rng();
        double v = std::bit_cast<double>(bits);
        if (std::isnan(v) || std::isinf(v)) continue;
        probe(v);
        if ((i & 7) == 0) { probe(-std::ldexp(double(rng() % (1ULL << 53)) + 0.5, int(rng() % 80) - 110)); probe(double(int64_t(rng())) * 1e-6); probe(-1.0 / double(rng() % 100000 + 1) * 1e-5); }
    }
    for (double v : {std::numeric_limits<double>::max(), -std::numeric_limits<double>::max(), std::numeric_limits<double>::min(), -std::numeric_limits<double>::min(), std::numeric_limits<double>::denorm_min(), -std::numeric_limits<double>::denorm_min(), -1.2345678901234567e-6, -1.2345678901234567e-5, -1.2345678901234567e21, -1.2345678901234567e20, -9.999999999999999e20, 1e21, 1e22, -1e-7, 123456789012345680000.0})
        probe(v);
    for (uint64_t x : {0ULL, ~0ULL, 1ULL << 63}) { std::memset(buf, 0x7f, sizeof buf); char* e = glz::to_chars(buf, x); size_t ext = 0; for (size_t i = 0; i < sizeof buf; ++i) if (buf[i] != 0x7f) ext = i + 1; maxILen = std::max(maxILen, size_t(e - buf)); maxIExt = std::max(maxIExt, ext);
        std::memset(buf, 0x7f, sizeof buf); e = glz::to_chars(buf, int64_t(x)); ext = 0; for (size_t i = 0; i < sizeof buf; ++i) if (buf[i] != 0x7f) ext = i + 1; maxILen = std::max(maxILen, size_t(e - buf)); maxIExt = std::max(maxIExt, ext); }
    for (size_t i = 0; i < 50000000; ++i) { uint64_t x = rng() >> (rng() % 64); std::memset(buf, 0x7f, 40); char* e = glz::to_chars(buf, x); size_t ext = 0; for (size_t k = 0; k < 40; ++k) if (buf[k] != 0x7f) ext = k + 1; maxILen = std::max(maxILen, size_t(e - buf)); maxIExt = std::max(maxIExt, ext);
        std::memset(buf, 0x7f, 40); e = glz::to_chars(buf, -int64_t(x >> 1)); ext = 0; for (size_t k = 0; k < 40; ++k) if (buf[k] != 0x7f) ext = k + 1; maxILen = std::max(maxILen, size_t(e - buf)); maxIExt = std::max(maxIExt, ext); }
    printf("double: max length %zu (%.17g), max write extent %zu (%.17g)\nint: max length %zu, max write extent %zu\n", maxLen, worstLen, maxExtent, worstExt, maxILen, maxIExt);
}
