#include "encoding/alp/alp_encoder.hpp"
#include "encoding/alp/alp_decoder.hpp"
#include "storage/aligned_buffer.hpp"
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <random>
#include <vector>
#include <algorithm>
using Clock = std::chrono::steady_clock;
static std::vector<double> gen(const char* shape, size_t n, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> d(0.0, 100.0);
    std::vector<double> v(n);
    std::string s(shape);
    for (size_t i = 0; i < n; ++i) {
        double x = d(rng);
        if (s == "dec1") x = std::round(x * 10) / 10;
        else if (s == "dec3") x = std::round(x * 1000) / 1000;
        else if (s == "dec6") x = std::round(x * 1e6) / 1e6;
        else if (s == "int") x = std::round(x * 100);
        else if (s == "full") { /* full-precision random */ }
        else if (s == "f32") x = static_cast<double>(static_cast<float>(std::round(x * 10) / 10));
        else if (s == "dec2nan") { x = std::round(x * 100) / 100; if (i % 400 == 7) x = std::numeric_limits<double>::quiet_NaN(); }
        else if (s == "dec2out") { x = std::round(x * 100) / 100; if (i % 400 == 7) x = d(rng) / 3.0; }
        else if (s == "mixed40") { x = (i % 5 < 2) ? d(rng) / 7.0 : std::round(x * 100) / 100; }
        v[i] = x;
    }
    return v;
}
int main(int argc, char** argv) {
    const size_t blocks = 256;
    const char* shapes[] = {"dec1", "dec3", "dec6", "int", "full", "f32", "dec2nan", "dec2out", "mixed40"};
    const size_t sizes[] = {3000, 10000};
    int reps = argc > 1 ? atoi(argv[1]) : 7;
    const char* only = getenv("SHAPE");
    for (size_t n : sizes) for (const char* shape : shapes) {
        if (only && std::string(only) != shape) continue;
        std::vector<std::vector<double>> data;
        for (size_t b = 0; b < blocks; ++b) data.push_back(gen(shape, n, 1234 + b));
        std::vector<double> times; size_t bytes = 0; uint64_t fp = 1469598103934665603ULL;
        for (int r = 0; r < reps + 1; ++r) {
            AlignedBuffer buf; buf.reserve(blocks * n * 9);
            auto t0 = Clock::now();
            for (size_t b = 0; b < blocks; ++b) ALPEncoder::encodeInto(data[b], buf);
            auto t1 = Clock::now();
            if (r) times.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count() / (blocks * n));
            bytes = buf.size();
            if (r == 0) { for (size_t i = 0; i < buf.size(); ++i) fp = (fp ^ buf.data[i]) * 1099511628211ULL; }
        }
        std::sort(times.begin(), times.end());
        printf("shape=%-8s n=%-6zu ns/value=%8.3f  bytes/value=%6.3f  fnv=%016llx\n", shape, n, times[times.size() / 2],
               double(bytes) / (blocks * n), (unsigned long long)fp);
    }
}
