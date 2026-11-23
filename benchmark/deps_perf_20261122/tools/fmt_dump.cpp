#include <cmath>
#include <cstdlib>
// Emits the JSON text glaze produces for a fixed corpus of doubles and ints,
// one per line, so two glaze versions can be diffed.
#include FLOAT_HDR
#include <glaze/util/itoa.hpp>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <random>
int main(int argc, char** argv) {
    size_t n = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 2000000;
    std::mt19937_64 rng(7);
    char buf[128];
    auto out = [&](double v) { char* e = glz::to_chars(buf, v); *e = 0; std::printf("%a %s\n", v, buf); };
    for (double v : {0.0, -0.0, 1.0, -1.0, 0.1, 0.5, 23.5, 100.0, 1e3, 1e6, 1e9, 1e15, 1e16, 1e17, 1e20, 1e21, 1e22, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7,
                     123456789.125, 1.7976931348623157e308, 5e-324, 2.2250738585072014e-308, 9007199254740993.0, 4503599627370496.0,
                     std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity()})
        out(v);
    for (int e = -30; e <= 30; ++e) { out(std::pow(10.0, e)); out(-1.2345678901234567 * std::pow(10.0, e)); out(7.0 * std::pow(10.0, e)); }
    for (size_t i = 0; i < n; ++i) {
        out(std::bit_cast<double>(rng()));                                  // random bits
        out(double(int64_t(rng() % 2000000) - 1000000) / 1000.0);           // 3-decimal metrics
        out(double(int64_t(rng() % 200000000000ULL)) );                      // integral doubles
        out(std::ldexp(double(rng() % (1ULL << 53)), int(rng() % 120) - 90)); // wide magnitude
    }
    for (size_t i = 0; i < n; ++i) { uint64_t x = rng() >> (rng() % 64); char* e = glz::to_chars(buf, x); *e = 0; std::puts(buf); e = glz::to_chars(buf, -int64_t(x >> 1)); *e = 0; std::puts(buf); }
}
