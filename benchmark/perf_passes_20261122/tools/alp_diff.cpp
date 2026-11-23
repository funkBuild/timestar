// Differential check: new timestar::alp::findBestExpFac vs the original brute-force scan.
#include "encoding/alp/alp_constants.hpp"
#include "encoding/alp/alp_param_search.hpp"
#include <algorithm>
#include <array>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <random>
#include <vector>
namespace ref {
struct ScaleResult { int64_t encoded; bool exact; };
inline ScaleResult scaleValue(double value, uint8_t exp, uint8_t fac) {
    double scaled = value * timestar::alp::FACT_ARR[exp];
    double rounded = std::round(scaled);
    if (rounded > static_cast<double>(timestar::alp::MAX_SAFE_INT) || rounded < static_cast<double>(timestar::alp::MIN_SAFE_INT)) return {0, false};
    int64_t encoded = static_cast<int64_t>(rounded);
    if (fac > 0) { int64_t factor = static_cast<int64_t>(timestar::alp::FACT_ARR[fac]); encoded = encoded / factor; }
    double decoded = static_cast<double>(encoded) * timestar::alp::FRAC_ARR[fac] / timestar::alp::FACT_ARR[exp];
    return {encoded, decoded == value};
}
struct BestPair { uint8_t exp = 0, fac = 0; size_t exceptions = 0; };
BestPair findBestExpFac(const double* values, size_t count, size_t* ops) {
    const size_t sample_size = std::min(count, timestar::alp::ALP_SAMPLE_SIZE);
    BestPair best; best.exceptions = sample_size + 1;
    std::array<size_t, 256> idx;
    if (sample_size == count) { for (size_t i = 0; i < count; ++i) idx[i] = i; }
    else { for (size_t i = 0; i < sample_size; ++i) idx[i] = (i * count) / sample_size; }
    for (uint8_t exp = 0; exp < timestar::alp::EXP_COUNT; ++exp) for (uint8_t fac = 0; fac <= exp; ++fac) {
        size_t exceptions = 0;
        for (size_t s = 0; s < sample_size; ++s) {
            double v = values[idx[s]];
            if (std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v))) exceptions++;
            else { ++*ops; if (!scaleValue(v, exp, fac).exact) exceptions++; }
            if (exceptions >= best.exceptions) break;
        }
        if (exceptions < best.exceptions) { best.exp = exp; best.fac = fac; best.exceptions = exceptions; }
        if (exceptions == 0) return best;
    }
    return best;
}
}
int main(int argc, char** argv) {
    uint64_t seed = argc > 1 ? strtoull(argv[1], 0, 10) : 1;
    size_t iters = argc > 2 ? strtoull(argv[2], 0, 10) : 200000;
    std::mt19937_64 rng(seed);
    size_t mismatches = 0, alpCases = 0, rdCases = 0, grayWins = 0, facWins = 0;
    for (size_t it = 0; it < iters; ++it) {
        size_t n = 1 + rng() % 3000; if (rng() % 4 == 0) n = 1 + rng() % 40;
        std::vector<double> v(n);
        int shape = rng() % 12;
        int digits = rng() % 19;
        double mag = std::pow(10.0, (int)(rng() % 22) - 4);
        double fracBad = (rng() % 5 == 0) ? 0.0 : (double)(rng() % 1000) / 1000.0 * ((rng() % 3) ? 0.2 : 1.0);
        std::uniform_real_distribution<double> u(0.0, 1.0);
        for (size_t i = 0; i < n; ++i) {
            double x = u(rng) * mag; if (rng() % 2) x = -x;
            double scale = std::pow(10.0, digits);
            switch (shape) {
                case 0: case 1: case 2: x = std::round(x * scale) / scale; break;            // decimals
                case 3: x = std::round(x) * std::pow(10.0, rng() % 6); break;                 // trailing zeros (fac)
                case 4: break;                                                                // full precision
                case 5: x = (double)(float)x; break;                                          // float32
                case 6: x = std::round(x * scale) / scale; if (u(rng) < fracBad) x = u(rng) * mag / 3; break;  // decimals + outliers
                case 7: { int64_t big = (int64_t)(u(rng) * 9.0e15); x = (double)big / std::pow(10.0, rng() % 19); if (rng() % 2) x = -x; break; }  // near 2^53 mantissas
                case 8: { int64_t big = (1LL << 50) + (int64_t)(rng() % (7LL << 50)); x = (double)big / std::pow(10.0, digits); break; } // gray zone
                case 9: x = std::round(x * scale) / scale; if (u(rng) < fracBad) { int k = rng() % 4; x = k == 0 ? std::numeric_limits<double>::quiet_NaN() : k == 1 ? std::numeric_limits<double>::infinity() : k == 2 ? -0.0 : -std::numeric_limits<double>::infinity(); } break;
                case 10: x = (double)(int64_t)(rng() % 2000) * std::pow(10.0, (int)(rng() % 30) - 15); break;  // few sig digits, wide exponents
                case 11: { int64_t big = (int64_t)(rng() % (1ULL << 53)); x = (double)(big - big % (int64_t)std::pow(10.0, rng() % 10) + (int64_t)(rng() % 3)) / std::pow(10.0, digits); break; } // rem in {0,1,2}
            }
            v[i] = x;
        }
        size_t ops = 0;
        auto r = ref::findBestExpFac(v.data(), n, &ops);
        auto g = timestar::alp::findBestExpFac(v.data(), n);
        size_t ss = std::min(n, timestar::alp::ALP_SAMPLE_SIZE);
        bool refRd = 2 * r.exceptions > ss, newRd = 2 * g.exceptions > ss;
        bool ok = refRd ? newRd : (!newRd && r.exp == g.exp && r.fac == g.fac && r.exceptions == g.exceptions);
        if (refRd) ++rdCases; else { ++alpCases; if (r.fac) ++facWins; }
        if (!ok) { if (++mismatches <= 10) printf("MISMATCH it=%zu shape=%d n=%zu ref=(%d,%d,%zu) new=(%d,%d,%zu)\n", it, shape, n, r.exp, r.fac, r.exceptions, g.exp, g.fac, g.exceptions); }
    }
    printf("seed=%llu iters=%zu alp=%zu rd=%zu fac>0 wins=%zu mismatches=%zu\n", (unsigned long long)seed, iters, alpCases, rdCases, facWins, mismatches);
    return mismatches ? 1 : 0;
}
