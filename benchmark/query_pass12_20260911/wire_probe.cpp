#include "integer_encoder_ffor.hpp"
#include <fstream>
#include <random>
#include <stdexcept>
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    std::ofstream file(argv[1], std::ios::binary);
    std::mt19937_64 rng(42);
    for (size_t n : {1, 2, 3, 7, 17, 1023, 1024, 1025, 2051, 65537}) {
        for (int shape = 0; shape < 3; ++shape) {
            std::vector<uint64_t> input(n);
            for (size_t i = 0; i < n; ++i) {
                const uint64_t edges[] = {UINT64_MAX, 0, (uint64_t{1} << 63) - 1, uint64_t{1} << 63, 1, 7};
                input[i] = shape == 0 ? rng() : shape == 1 ? edges[i % 6] : 1700000000000000000ULL + i * 1000000 + rng() % 1000;
            }
            auto encoded = IntegerEncoderFFOR::encode(input);
            const uint64_t size = encoded.size();
            file.write(reinterpret_cast<const char*>(&size), sizeof(size));
            file.write(reinterpret_cast<const char*>(encoded.data.data()), size);
            Slice slice(encoded.data.data(), encoded.size());
            std::vector<uint64_t> output;
            IntegerEncoderFFOR::decode(slice, n, output);
            if (output != input) throw std::runtime_error("wire fixture roundtrip mismatch");
        }
    }
    return !file;
}
