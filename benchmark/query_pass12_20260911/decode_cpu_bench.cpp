#include "integer_encoder_ffor.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <random>
#include <stdexcept>
#include <string>

int main() {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    for (size_t n : {1024, 65537}) {
        for (std::string shape : {"regular", "jitter8", "jitter20", "jitter32", "exceptions"}) {
            std::mt19937_64 rng(42);
            std::vector<uint64_t> input(n);
            uint64_t ts = 1700000000000000000ULL;
            for (size_t i = 0; i < n; ++i) {
                uint64_t jitter = shape == "regular" ? 0 : rng() & ((uint64_t{1} << (shape == "jitter32" ? 32 : shape == "jitter20" ? 20 : 8)) - 1);
                if (shape == "exceptions" && i % 103 == 0) jitter += uint64_t{1} << 40;
                input[i] = ts;
                ts += 1000000 + jitter;
            }
            auto encoded = IntegerEncoderFFOR::encode(input);
            for (size_t offset : {0, 3}) {
                std::vector<uint8_t> bytes(encoded.size() + offset);
                std::memcpy(bytes.data() + offset, encoded.data.data(), encoded.size());
                for (std::string filter : {"full", "middle", "early"}) {
                    const uint64_t lo = filter == "middle" ? input[n / 3] : 0;
                    const uint64_t hi = filter == "middle" ? input[2 * n / 3] : filter == "early" ? input[127] : UINT64_MAX;
                    auto first = std::lower_bound(input.begin(), input.end(), lo);
                    auto last = std::upper_bound(first, input.end(), hi);
                    std::vector<uint64_t> expected(first, last), output;
                    output.reserve(n);
                    const size_t loops = std::max(size_t(32), size_t(2097152) / (filter == "early" ? 1024 : n));
                    std::vector<double> times;
                    for (int rep = 0; rep < 7; ++rep) {
                        const auto start = std::chrono::steady_clock::now();
                        for (size_t j = 0; j < loops; ++j) {
                            output.clear();
                            Slice slice(bytes.data() + offset, encoded.size());
                            const auto [skipped, added] = IntegerEncoderFFOR::decode(slice, n, output, lo, hi);
                            if (skipped != size_t(first - input.begin()) || added != expected.size()) throw std::runtime_error("counts mismatch");
                        }
                        const double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count() / loops;
                        if (output != expected) throw std::runtime_error("timestamps mismatch");
                        if (rep) times.push_back(ns);
                    }
                    std::sort(times.begin(), times.end());
                    std::printf("%s n=%zu offset=%zu filter=%s median_ns=%.3f min_ns=%.3f max_ns=%.3f encoded_bytes=%zu output_capacity_bytes=%zu\n", shape.c_str(), n, offset, filter.c_str(),
                                (times[2] + times[3]) / 2, times.front(), times.back(), encoded.size(), output.capacity() * sizeof(uint64_t));
                }
            }
        }
    }
}
