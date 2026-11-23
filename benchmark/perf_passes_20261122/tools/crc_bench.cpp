#include "utils/crc32.hpp"
#include <chrono>
#include <cstdio>
#include <random>
#include <vector>
using Clock = std::chrono::steady_clock;
int main() {
    std::mt19937_64 rng(5);
    std::vector<uint8_t> buf(1 << 20);
    for (auto& b : buf) b = uint8_t(rng());
    size_t mism = 0, cases = 0;
    for (size_t len = 0; len <= 5000; ++len) for (size_t off : {size_t{0}, size_t{1}, size_t{7}, size_t{13}}) {
        uint32_t seed = uint32_t(rng());
        for (uint32_t init : {CRC32::INIT, seed}) { ++cases; if (CRC32::update(init, buf.data() + off, len) != CRC32::updateTables(init, buf.data() + off, len)) ++mism; }
    }
    for (int i = 0; i < 200000; ++i) {  // random lengths and chained updates
        size_t off = rng() % 4096, len = rng() % 70000, split = len ? rng() % len : 0;
        uint32_t a = CRC32::update(CRC32::update(CRC32::INIT, buf.data() + off, split), buf.data() + off + split, len - split);
        ++cases; if (a != CRC32::updateTables(CRC32::INIT, buf.data() + off, len)) ++mism;
    }
    const char* check = "123456789";
    printf("cases=%zu mismatches=%zu  crc32(\"123456789\")=%08x (expect cbf43926)\n", cases, mism, CRC32::compute(check, 9));
    for (size_t len : {size_t{64}, size_t{128}, size_t{256}, size_t{1024}, size_t{16384}, size_t{262144}}) {
        size_t iters = (size_t{1} << 28) / len; uint32_t sink = 0;
        auto t0 = Clock::now(); for (size_t i = 0; i < iters; ++i) sink ^= CRC32::updateTables(uint32_t(i), buf.data() + (i & 63), len); auto t1 = Clock::now();
        for (size_t i = 0; i < iters; ++i) sink ^= CRC32::update(uint32_t(i), buf.data() + (i & 63), len); auto t2 = Clock::now();
        double tb = std::chrono::duration<double>(t1 - t0).count(), ts = std::chrono::duration<double>(t2 - t1).count();
        printf("len=%-7zu tables %6.2f GB/s   update() %6.2f GB/s   %.2fx   (sink %08x)\n", len, iters * len / tb / 1e9, iters * len / ts / 1e9, tb / ts, sink);
    }
    return mism != 0;
}
