// Encoded float bytes for the data WALTSMRolloverTest writes (500 series x 5000 points).
#include "encoding/alp/alp_encoder.hpp"
#include "storage/aligned_buffer.hpp"
#include <cstdio>
#include <vector>
int main() {
    size_t total = 0, n = 0;
    for (size_t s = 0; s < 500; s++) {
        std::vector<double> v;
        for (size_t i = 0; i < 5000; i++) v.push_back(s * 100.0 + i * 0.7 + (i % 7) * 3.14159);
        auto buf = ALPEncoder::encode(v);
        total += buf.size(); n += v.size();
    }
    printf("values=%zu encoded=%zu bytes (%.2f MB) %.2f B/value\n", n, total, total / 1048576.0, double(total) / n);
}
