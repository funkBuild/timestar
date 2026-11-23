// Short-decimal fast path for double -> JSON text, against glaze's zmij: identical bytes, and how much faster.
#include <glaze/util/itoa.hpp>
#include <glaze/util/zmij.hpp>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <string>
#include <vector>
using Clock = std::chrono::steady_clock;

#include "http/short_decimal.hpp"
using timestar::http::writeShortDecimal;

int main() {
    std::mt19937_64 rng(11);
    struct Set { const char* name; std::vector<double> v; };
    std::vector<Set> sets;
    const size_t N = 1000000;
    { Set s{"3 decimals (0..100)", {}}; for (size_t i = 0; i < N; ++i) s.v.push_back(double(rng() % 100000) / 1000.0); sets.push_back(s); }
    { Set s{"2 decimals (x0.25)", {}}; for (size_t i = 0; i < N; ++i) s.v.push_back(double(rng() % 1000) * 0.25); sets.push_back(s); }
    { Set s{"integers < 1e6", {}}; for (size_t i = 0; i < N; ++i) s.v.push_back(double(rng() % 1000000)); sets.push_back(s); }
    { Set s{"1 decimal, negative mix", {}}; for (size_t i = 0; i < N; ++i) s.v.push_back((double(rng() % 20000) - 10000.0) / 10.0); sets.push_back(s); }
    { Set s{"6 decimals", {}}; for (size_t i = 0; i < N; ++i) s.v.push_back(double(rng() % 100000000) / 1e6); sets.push_back(s); }
    { Set s{"large integers (1e9..1e13)", {}}; for (size_t i = 0; i < N; ++i) s.v.push_back(double(1000000000ULL + rng() % 10000000000000ULL)); sets.push_back(s); }
    { Set s{"full precision", {}}; std::uniform_real_distribution<double> d(0, 100); for (size_t i = 0; i < N; ++i) s.v.push_back(d(rng)); sets.push_back(s); }
    { Set s{"float32 widened", {}}; std::uniform_real_distribution<float> d(0, 100); for (size_t i = 0; i < N; ++i) s.v.push_back(double(d(rng))); sets.push_back(s); }

    // 1. identity over a large corpus: wherever the fast path answers, bytes must equal zmij's.
    size_t checked = 0, taken = 0, mismatches = 0;
    char a[64], b[64];
    auto check = [&](double v) {
        ++checked;
        char* e = writeShortDecimal(a, v);
        if (!e) return;
        ++taken;
        char* z = glz::to_chars(b, v);
        if (size_t(e - a) != size_t(z - b) || std::memcmp(a, b, size_t(e - a)) != 0) {
            if (mismatches++ < 10) printf("MISMATCH %a fast=%.*s zmij=%.*s\n", v, int(e - a), a, int(z - b), b);
        }
    };
    for (auto& s : sets) for (double v : s.v) { check(v); check(-v); }
    for (size_t i = 0; i < 60000000; ++i) {
        const uint64_t r = rng();
        const int k = int(r % 7); static const double p10[] = {1, 10, 100, 1e3, 1e4, 1e5, 1e6};
        const double v = double(int64_t((rng() >> (rng() % 50))) % 2000000000000000LL) / p10[k];   // decimals of every scale and magnitude
        check(v); check(std::nextafter(v, 1e300)); check(std::nextafter(v, -1e300));
        if ((i & 3) == 0) check(std::bit_cast<double>(rng()));
        if ((i & 7) == 0) { const double big = double(rng() % 2000000000000000ULL); check(big); check(-big); check(big + 0.5); check(std::nextafter(big, 1e300)); }
    }
    for (double v : {0.0, -0.0, 1e-4, 9.9e-5, 0.0001, 0.00011, 1e9, 999999999.999999, 1e15, 0.1, 0.2, 0.3, 0.7, 1.0, -1.0, 100.0, 0.000001, 0.5, 123456.789012, 1e-7, 5e-324, 1.7976931348623157e308}) { check(v); check(-v); }
    printf("identity: checked %zu values, fast path took %zu, mismatches %zu\n", checked, taken, mismatches);

    // 2. speed
    std::vector<char> out(N * 26 + 64);
    for (auto& s : sets) {
        double best[2] = {1e30, 1e30}; size_t bytes[2] = {0, 0};
        for (int rep = 0; rep < 7; ++rep) {
            for (int mode = 0; mode < 2; ++mode) {
                char* p = out.data(); auto t0 = Clock::now();
                if (mode == 0) { for (double v : s.v) { p = glz::to_chars(p, v); *p++ = ','; } }
                else { for (double v : s.v) { char* e = writeShortDecimal(p, v); p = e ? e : glz::to_chars(p, v); *p++ = ','; } }
                auto t1 = Clock::now(); best[mode] = std::min(best[mode], std::chrono::duration<double, std::nano>(t1 - t0).count() / double(N)); bytes[mode] = size_t(p - out.data());
            }
        }
        printf("%-26s zmij %5.2f ns/value   with fast path %5.2f ns/value   %.2fx   %s\n", s.name, best[0], best[1], best[0] / best[1], bytes[0] == bytes[1] ? "same bytes" : "DIFFERENT SIZE");
    }
}
