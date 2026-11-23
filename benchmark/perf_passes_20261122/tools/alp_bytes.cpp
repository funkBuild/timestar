// The ALP parameter search and the delta decision are both shortcuts around a
// straightforward scan. These tests keep that scan as the specification: a
// literal brute-force reference lives here, and the encoder must agree with it
// on every input, so the shortcuts can only ever change how long encoding
// takes — never which bytes are written.

#include "encoding/alp/alp_constants.hpp"
#include "encoding/alp/alp_encoder.hpp"
#include <cstdio>
#include <string>
namespace timestar::alp { struct AlpParams { uint8_t exp = 0; uint8_t fac = 0; size_t exceptions = 0; }; }
#include "storage/aligned_buffer.hpp"


#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <limits>
#include <random>
#include <vector>

namespace {

using timestar::alp::ALP_SAMPLE_SIZE;
using timestar::alp::ALP_VECTOR_SIZE;
using timestar::alp::EXP_COUNT;
using timestar::alp::FACT_ARR;
using timestar::alp::FRAC_ARR;

struct RefScale {
    int64_t encoded;
    bool exact;
};

RefScale refScale(double value, uint8_t exp, uint8_t fac) {
    const double rounded = std::round(value * FACT_ARR[exp]);
    if (rounded > static_cast<double>(timestar::alp::MAX_SAFE_INT) ||
        rounded < static_cast<double>(timestar::alp::MIN_SAFE_INT)) {
        return {0, false};
    }
    int64_t encoded = static_cast<int64_t>(rounded);
    if (fac > 0) {
        encoded /= static_cast<int64_t>(FACT_ARR[fac]);
    }
    const double decoded = static_cast<double>(encoded) * FRAC_ARR[fac] / FACT_ARR[exp];
    return {encoded, decoded == value};
}

bool isSpecial(double v) {
    return std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v));
}

// Every (exp, fac) pair, every sampled value, first pair with the fewest
// exceptions. No early exit of any kind.
timestar::alp::AlpParams refSearch(const std::vector<double>& values) {
    const size_t count = values.size();
    const size_t sampleSize = std::min(count, ALP_SAMPLE_SIZE);
    timestar::alp::AlpParams best;
    best.exceptions = sampleSize + 1;
    for (uint8_t exp = 0; exp < EXP_COUNT; ++exp) {
        for (uint8_t fac = 0; fac <= exp; ++fac) {
            size_t exceptions = 0;
            for (size_t s = 0; s < sampleSize; ++s) {
                const double v = values[sampleSize == count ? s : (s * count) / sampleSize];
                if (isSpecial(v) || !refScale(v, exp, fac).exact) {
                    ++exceptions;
                }
            }
            if (exceptions < best.exceptions) {
                best = {exp, fac, exceptions};
            }
        }
    }
    return best;
}

bool usesAlpRd(size_t exceptions, size_t count) {
    const double rate = static_cast<double>(exceptions) / static_cast<double>(std::min(count, ALP_SAMPLE_SIZE));
    return rate > timestar::alp::ALP_RD_EXCEPTION_THRESHOLD;
}

// The scheme the encoder must pick, computed the slow way: scale the first
// block value by value and compare the plain and delta bit widths.
uint8_t refScheme(const std::vector<double>& values) {
    const auto best = refSearch(values);
    if (usesAlpRd(best.exceptions, values.size())) {
        return timestar::alp::SCHEME_ALP_RD;
    }
    auto width = [](uint64_t range) -> int { return range == 0 ? 0 : 64 - __builtin_clzll(range); };
    int64_t absMin = std::numeric_limits<int64_t>::max();
    int64_t absMax = std::numeric_limits<int64_t>::min();
    uint64_t zzMin = UINT64_MAX;
    uint64_t zzMax = 0;
    int64_t prev = 0;
    bool first = true;
    const size_t n = std::min(values.size(), ALP_VECTOR_SIZE);
    for (size_t i = 0; i < n; ++i) {
        if (isSpecial(values[i])) {
            continue;
        }
        const auto r = refScale(values[i], best.exp, best.fac);
        if (!r.exact) {
            continue;
        }
        absMin = std::min(absMin, r.encoded);
        absMax = std::max(absMax, r.encoded);
        if (!first) {
            const int64_t delta = r.encoded - prev;
            const uint64_t zz = (static_cast<uint64_t>(delta) << 1) ^ static_cast<uint64_t>(delta >> 63);
            zzMin = std::min(zzMin, zz);
            zzMax = std::max(zzMax, zz);
        }
        first = false;
        prev = r.encoded;
    }
    const int absBw = absMin == absMax ? 0 : width(static_cast<uint64_t>(absMax) - static_cast<uint64_t>(absMin));
    const int deltaBw = zzMin <= zzMax ? width(zzMax - zzMin) : absBw;
    return deltaBw < absBw ? timestar::alp::SCHEME_ALP_DELTA : timestar::alp::SCHEME_ALP;
}

struct EncodedHeader {
    uint8_t scheme;
    uint8_t exp;
    uint8_t fac;
};

EncodedHeader encodeAndReadHeader(const std::vector<double>& values) {
    AlignedBuffer buffer;
    ALPEncoder::encodeInto(values, buffer);
    uint64_t header1 = 0;
    uint64_t block0 = 0;
    std::memcpy(&header1, buffer.data.data() + 8, 8);
    std::memcpy(&block0, buffer.data.data() + 16, 8);
    return {static_cast<uint8_t>(header1 >> 32), static_cast<uint8_t>(block0), static_cast<uint8_t>(block0 >> 8)};
}

// One array per call, cycling through the shapes that stress different parts
// of the search: clean decimals, trailing zeros, full precision, float32
// widened to double, decimals with outliers or special values, and mantissas
// sitting right at the 2^50..2^53 boundary where the fac shortcut stops
// applying and pairs are evaluated one by one.
std::vector<double> randomArray(std::mt19937_64& rng, int shape) {
    size_t n = 1 + rng() % 3000;
    if (rng() % 4 == 0) {
        n = 1 + rng() % 40;
    }
    std::vector<double> v(n);
    const int digits = static_cast<int>(rng() % 19);
    const double scale = std::pow(10.0, digits);
    const double mag = std::pow(10.0, static_cast<int>(rng() % 22) - 4);
    const double badRate = (rng() % 5 == 0) ? 0.0 : static_cast<double>(rng() % 1000) / 1000.0 * ((rng() % 3) ? 0.2 : 1.0);
    std::uniform_real_distribution<double> u(0.0, 1.0);
    for (size_t i = 0; i < n; ++i) {
        double x = u(rng) * mag;
        if (rng() % 2) {
            x = -x;
        }
        switch (shape) {
            case 0:
                x = std::round(x * scale) / scale;
                break;
            case 1:
                x = std::round(x) * std::pow(10.0, static_cast<int>(rng() % 6));
                break;
            case 2:
                break;
            case 3:
                x = static_cast<double>(static_cast<float>(x));
                break;
            case 4:
                x = std::round(x * scale) / scale;
                if (u(rng) < badRate) {
                    x = u(rng) * mag / 3;
                }
                break;
            case 5: {
                const auto big = static_cast<int64_t>(u(rng) * 9.0e15);
                x = static_cast<double>(big) / std::pow(10.0, static_cast<int>(rng() % 19));
                break;
            }
            case 6: {
                const int64_t big = (1LL << 50) + static_cast<int64_t>(rng() % (7ULL << 50));
                x = static_cast<double>(big) / scale;
                break;
            }
            case 7:
                x = std::round(x * scale) / scale;
                if (u(rng) < badRate) {
                    switch (rng() % 4) {
                        case 0:
                            x = std::numeric_limits<double>::quiet_NaN();
                            break;
                        case 1:
                            x = std::numeric_limits<double>::infinity();
                            break;
                        case 2:
                            x = -0.0;
                            break;
                        default:
                            x = -std::numeric_limits<double>::infinity();
                    }
                }
                break;
            case 8:
                x = static_cast<double>(rng() % 2000) * std::pow(10.0, static_cast<int>(rng() % 30) - 15);
                break;
            default: {
                const auto big = static_cast<int64_t>(rng() % (1ULL << 53));
                const auto step = static_cast<int64_t>(std::pow(10.0, static_cast<int>(rng() % 10)));
                x = static_cast<double>(big - big % step + static_cast<int64_t>(rng() % 3)) / scale;
            }
        }
        v[i] = x;
    }
    return v;
}

constexpr int kShapes = 10;


}
int main(int argc, char** argv) {
    std::mt19937_64 rng(argc > 1 ? atoll(argv[1]) : 77);
    size_t iters = argc > 2 ? atoll(argv[2]) : 30000;
    uint64_t fp = 1469598103934665603ULL; size_t bytes = 0;
    for (size_t iteration = 0; iteration < iters; ++iteration) {
        auto values = randomArray(rng, iteration % kShapes);
        if (iteration % 3 == 1) std::sort(values.begin(), values.end(), [](double a, double b) { return (std::isnan(a) ? 1 : 0) < (std::isnan(b) ? 1 : 0) || (!std::isnan(a) && !std::isnan(b) && a < b); });
        else if (iteration % 3 == 2) for (size_t i = 0; i < values.size(); ++i) values[i] = 1000.0 + 0.25 * static_cast<double>(i) + ((rng() % 50 == 0) ? 0.5 : 0.0);
        AlignedBuffer buffer; ALPEncoder::encodeInto(values, buffer);
        bytes += buffer.size();
        for (size_t i = 0; i < buffer.size(); ++i) fp = (fp ^ buffer.data[i]) * 1099511628211ULL;
    }
    printf("arrays=%zu bytes=%zu fnv=%016llx\n", iters, bytes, (unsigned long long)fp);
}
