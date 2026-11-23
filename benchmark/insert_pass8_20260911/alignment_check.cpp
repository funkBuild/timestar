#include "alp_ffor.hpp"
#include <cassert>
#include <vector>
#include <cstring>
int main() {
    for (size_t count : {0, 1, 3, 65, 1024}) {
        for (uint8_t bw = 0; bw <= 64; ++bw) {
            const uint64_t mask = bw == 64 ? UINT64_MAX : (uint64_t{1} << bw) - 1;
            std::vector<uint64_t> values(count), recovered(count);
            for (size_t i = 0; i < count; ++i) values[i] = ((i + 1) * 0x9e3779b97f4a7c15ULL) & mask;
            const size_t words = timestar::alp::ffor_packed_words(count, bw);
            std::vector<uint64_t> aligned(words);
            timestar::alp::ffor_pack_u64(values.data(), count, 0, bw, aligned.data());
            timestar::alp::ffor_unpack_u64(aligned.data(), count, 0, bw, recovered.data());
            assert(recovered == values);
            for (size_t offset = 0; offset < 8; ++offset) {
                std::vector<uint8_t> bytes(words * 8 + 16, 0xcd);
#ifdef BASELINE
                auto* dest = reinterpret_cast<uint64_t*>(bytes.data() + offset);
#else
                auto* dest = bytes.data() + offset;
#endif
                timestar::alp::ffor_pack_u64(values.data(), count, 0, bw, dest);
                assert(!words || std::memcmp(bytes.data() + offset, aligned.data(), words * 8) == 0);
                for (size_t i = 0; i < offset; ++i) assert(bytes[i] == 0xcd);
                for (size_t i = offset + words * 8; i < bytes.size(); ++i) assert(bytes[i] == 0xcd);
            }
        }
    }
}
