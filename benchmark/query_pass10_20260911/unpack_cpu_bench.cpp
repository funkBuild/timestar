#include "alp_ffor.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>

[[gnu::noinline]] void unpackAligned(const uint64_t* in, uint8_t bw, uint64_t* out) {
    timestar::alp::ffor_unpack_u64(in, 1024, 17, bw, out);
}
[[gnu::noinline]] void unpackPayload(const uint64_t* storage, uint8_t bw, uint64_t* out) {
    const auto* bytes = reinterpret_cast<const uint8_t*>(storage) + 3;
#ifdef PASS10_BEFORE
    uint64_t scratch[1024];
    std::memcpy(scratch, bytes, timestar::alp::ffor_packed_words(1024, bw) * 8);
    timestar::alp::ffor_unpack_u64(scratch, 1024, 17, bw, out);
#else
    timestar::alp::ffor_unpack_u64(bytes, 1024, 17, bw, out);
#endif
}
int main() {
    for (uint8_t bw = 0; bw <= 64; ++bw) {
        std::vector<uint64_t> input(1024), packed(1025), out(1024);
        const auto mask = bw == 64 ? UINT64_MAX : (uint64_t{1} << bw) - 1;
        for (size_t i = 0; i < 1024; ++i) input[i] = 17 + ((i * 0x9e3779b97f4a7c15ULL) & mask);
        for (bool bytePayload : {false, true}) {
            auto* bytes = reinterpret_cast<uint8_t*>(packed.data()) + (bytePayload ? 3 : 0);
            timestar::alp::ffor_pack_u64(input.data(), input.size(), 17, bw, bytes);
            auto unpack = bytePayload ? unpackPayload : unpackAligned;
            std::vector<double> times;
            for (int rep = 0; rep < 7; ++rep) {
                const auto start = std::chrono::steady_clock::now();
                for (int j = 0; j < 2048; ++j) {
                    unpack(packed.data(), bw, out.data());
                    asm volatile("" : : "g"(out.data()) : "memory");
                }
                const double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count() / 2048;
                if (out != input) throw std::runtime_error("unpack mismatch");
                if (rep) times.push_back(ns);
            }
            std::sort(times.begin(), times.end());
            std::printf("bw=%u byte_payload=%d median_ns=%.3f min_ns=%.3f max_ns=%.3f\n", bw, bytePayload, (times[2]+times[3])/2, times.front(), times.back());
        }
    }
}
