#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include <algorithm>
#include <cstring>
#include <string>
#include <numeric>
#include <cassert>

#include "../../lib/encoding/integer_encoder.hpp"
#include "../../lib/encoding/integer/integer_encoder_ffor.hpp"
#include "../../lib/storage/aligned_buffer.hpp"
#include "../../lib/storage/slice_buffer.hpp"

// ANSI color codes
#define RESET   "\033[0m"
#define BOLD    "\033[1m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"
#define DIM     "\033[2m"

// ---------------------------------------------------------------------------
// Dataset generators
// ---------------------------------------------------------------------------

// Perfectly constant-interval timestamps (best case for FFOR)
std::vector<uint64_t> genConstantInterval(size_t count,
                                          uint64_t start = 1'000'000'000'000ULL,
                                          uint64_t interval = 1'000'000'000ULL) {
    std::vector<uint64_t> v(count);
    for (size_t i = 0; i < count; ++i) v[i] = start + i * interval;
    return v;
}

// Timestamps with small jitter (common TSDB pattern)
std::vector<uint64_t> genSmallJitter(size_t count,
                                     uint64_t start = 1'000'000'000'000ULL,
                                     uint64_t interval = 1'000'000'000ULL,
                                     uint64_t jitter = 1'000'000ULL) {
    std::vector<uint64_t> v(count);
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int64_t> dist(-static_cast<int64_t>(jitter),
                                                 static_cast<int64_t>(jitter));
    uint64_t ts = start;
    for (size_t i = 0; i < count; ++i) {
        v[i] = ts;
        ts += interval + dist(rng);
    }
    return v;
}

// Timestamps with occasional large gaps (tests exception mechanism)
std::vector<uint64_t> genWithGaps(size_t count,
                                  uint64_t start = 1'000'000'000'000ULL,
                                  uint64_t interval = 1'000'000'000ULL,
                                  double gap_probability = 0.02,
                                  uint64_t gap_size = 60'000'000'000ULL) {
    std::vector<uint64_t> v(count);
    std::mt19937_64 rng(123);
    std::uniform_real_distribution<double> prob(0.0, 1.0);
    std::uniform_int_distribution<int64_t> jitter(-500'000LL, 500'000LL);
    uint64_t ts = start;
    for (size_t i = 0; i < count; ++i) {
        v[i] = ts;
        if (prob(rng) < gap_probability) {
            ts += gap_size;  // big gap
        } else {
            ts += interval + jitter(rng);
        }
    }
    return v;
}

// Moderate jitter (e.g. network-collected timestamps)
std::vector<uint64_t> genModerateJitter(size_t count,
                                        uint64_t start = 1'000'000'000'000ULL,
                                        uint64_t interval = 1'000'000'000ULL,
                                        uint64_t jitter = 100'000'000ULL) {
    std::vector<uint64_t> v(count);
    std::mt19937_64 rng(77);
    std::uniform_int_distribution<int64_t> dist(-static_cast<int64_t>(jitter),
                                                 static_cast<int64_t>(jitter));
    uint64_t ts = start;
    for (size_t i = 0; i < count; ++i) {
        v[i] = ts;
        ts += interval + dist(rng);
    }
    return v;
}

// Random monotonic (worst case for FFOR: wide deltas)
std::vector<uint64_t> genRandomMonotonic(size_t count,
                                         uint64_t start = 1'000'000'000'000ULL) {
    std::vector<uint64_t> v(count);
    std::mt19937_64 rng(999);
    std::uniform_int_distribution<uint64_t> dist(1, 10'000'000'000ULL);
    uint64_t ts = start;
    for (size_t i = 0; i < count; ++i) {
        v[i] = ts;
        ts += dist(rng);
    }
    return v;
}

// Non-monotonic timestamps (unordered, stress test)
std::vector<uint64_t> genNonMonotonic(size_t count) {
    std::vector<uint64_t> v(count);
    std::mt19937_64 rng(555);
    std::uniform_int_distribution<uint64_t> dist(1'000'000'000'000ULL,
                                                  2'000'000'000'000ULL);
    for (size_t i = 0; i < count; ++i) {
        v[i] = dist(rng);
    }
    std::sort(v.begin(), v.end());
    return v;
}

// ---------------------------------------------------------------------------
// Benchmark infrastructure
// ---------------------------------------------------------------------------

struct EncoderResult {
    std::string encoder_name;
    double encode_ns_per_value;
    double decode_ns_per_value;
    size_t compressed_bytes;
    size_t original_bytes;
    double bits_per_value;
    bool correct;
};

struct DatasetResult {
    std::string dataset_name;
    size_t count;
    EncoderResult simple16;
    EncoderResult ffor;
};

// Benchmark the Simple16-based encoder (IntegerEncoderBasic via IntegerEncoder)
EncoderResult benchSimple16(const std::vector<uint64_t> &data, int warmup, int runs) {
    EncoderResult r;
    r.encoder_name = "Simple16";
    r.original_bytes = data.size() * sizeof(uint64_t);

    // Force basic implementation
    IntegerEncoder::setImplementation(IntegerEncoder::BASIC);

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        auto enc = IntegerEncoder::encode(data);
    }

    // Encode timing
    auto enc_start = std::chrono::high_resolution_clock::now();
    AlignedBuffer encoded;
    for (int i = 0; i < runs; ++i) {
        encoded = IntegerEncoder::encode(data);
    }
    auto enc_end = std::chrono::high_resolution_clock::now();

    double enc_total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(enc_end - enc_start).count();
    r.encode_ns_per_value = enc_total_ns / (static_cast<double>(runs) * data.size());
    r.compressed_bytes = encoded.size();
    r.bits_per_value = (r.compressed_bytes * 8.0) / data.size();

    // Decode timing
    std::vector<uint64_t> decoded;
    decoded.reserve(data.size());

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        IntegerEncoder::decode(s, data.size(), decoded);
    }

    auto dec_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runs; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        IntegerEncoder::decode(s, data.size(), decoded);
    }
    auto dec_end = std::chrono::high_resolution_clock::now();

    double dec_total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dec_end - dec_start).count();
    r.decode_ns_per_value = dec_total_ns / (static_cast<double>(runs) * data.size());

    // Verify correctness
    r.correct = (decoded.size() == data.size());
    if (r.correct) {
        for (size_t i = 0; i < data.size(); ++i) {
            if (decoded[i] != data[i]) {
                r.correct = false;
                std::cerr << "Simple16 mismatch at [" << i << "]: expected "
                          << data[i] << " got " << decoded[i] << "\n";
                break;
            }
        }
    }

    IntegerEncoder::setImplementation(IntegerEncoder::AUTO);
    return r;
}

// Benchmark the FFOR-based encoder
EncoderResult benchFFOR(const std::vector<uint64_t> &data, int warmup, int runs) {
    EncoderResult r;
    r.encoder_name = "FFOR";
    r.original_bytes = data.size() * sizeof(uint64_t);

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        auto enc = IntegerEncoderFFOR::encode(data);
    }

    // Encode timing
    auto enc_start = std::chrono::high_resolution_clock::now();
    AlignedBuffer encoded;
    for (int i = 0; i < runs; ++i) {
        encoded = IntegerEncoderFFOR::encode(data);
    }
    auto enc_end = std::chrono::high_resolution_clock::now();

    double enc_total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(enc_end - enc_start).count();
    r.encode_ns_per_value = enc_total_ns / (static_cast<double>(runs) * data.size());
    r.compressed_bytes = encoded.size();
    r.bits_per_value = (r.compressed_bytes * 8.0) / data.size();

    // Decode timing
    std::vector<uint64_t> decoded;
    decoded.reserve(data.size());

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        IntegerEncoderFFOR::decode(s, data.size(), decoded);
    }

    auto dec_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runs; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        IntegerEncoderFFOR::decode(s, data.size(), decoded);
    }
    auto dec_end = std::chrono::high_resolution_clock::now();

    double dec_total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(dec_end - dec_start).count();
    r.decode_ns_per_value = dec_total_ns / (static_cast<double>(runs) * data.size());

    // Verify correctness
    r.correct = (decoded.size() == data.size());
    if (r.correct) {
        for (size_t i = 0; i < data.size(); ++i) {
            if (decoded[i] != data[i]) {
                r.correct = false;
                std::cerr << "FFOR mismatch at [" << i << "]: expected "
                          << data[i] << " got " << decoded[i] << "\n";
                break;
            }
        }
    }

    return r;
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

void printHeader() {
    std::cout << BOLD << MAGENTA
              << "\n+======================================================================+\n"
              << "|     INTEGER ENCODER COMPARISON: Simple16 vs FFOR Bit-Packing        |\n"
              << "|     Delta-of-delta + ZigZag -> [Simple16 | FFOR+Exceptions]          |\n"
              << "+======================================================================+\n"
              << RESET << "\n";
}

void printDatasetHeader(const std::string &name, size_t count) {
    std::cout << BOLD << CYAN << "--- " << name
              << " (" << count << " values) ---" << RESET << "\n";
}

std::string fmtDelta(double a, double b) {
    // format percentage change from b to a (negative = a is better)
    double pct = ((a - b) / b) * 100.0;
    char buf[32];
    if (pct < -1.0) {
        snprintf(buf, sizeof(buf), GREEN "%.1f%%" RESET, pct);
    } else if (pct > 1.0) {
        snprintf(buf, sizeof(buf), RED "+%.1f%%" RESET, pct);
    } else {
        snprintf(buf, sizeof(buf), DIM "~0%%" RESET);
    }
    return buf;
}

void printComparison(const DatasetResult &dr) {
    printDatasetHeader(dr.dataset_name, dr.count);

    auto &s = dr.simple16;
    auto &f = dr.ffor;

    // Table header
    std::cout << BOLD
              << "  " << std::setw(12) << std::left << "Encoder"
              << std::setw(14) << std::right << "Enc ns/val"
              << std::setw(14) << "Dec ns/val"
              << std::setw(14) << "Comp bytes"
              << std::setw(14) << "bits/val"
              << std::setw(10) << "Ratio"
              << std::setw(8) << "OK?"
              << RESET << "\n";

    auto printRow = [](const EncoderResult &r) {
        std::cout << "  " << std::setw(12) << std::left << r.encoder_name
                  << std::setw(14) << std::right << std::fixed << std::setprecision(1) << r.encode_ns_per_value
                  << std::setw(14) << std::fixed << std::setprecision(1) << r.decode_ns_per_value
                  << std::setw(14) << r.compressed_bytes
                  << std::setw(14) << std::fixed << std::setprecision(2) << r.bits_per_value
                  << std::setw(9) << std::fixed << std::setprecision(1) << (r.original_bytes * 1.0 / r.compressed_bytes) << "x"
                  << "  " << (r.correct ? (GREEN "PASS" RESET) : (RED "FAIL" RESET))
                  << "\n";
    };

    printRow(s);
    printRow(f);

    // Delta summary
    std::cout << DIM << "  FFOR vs Simple16: "
              << "encode " << fmtDelta(f.encode_ns_per_value, s.encode_ns_per_value) << DIM
              << "  decode " << fmtDelta(f.decode_ns_per_value, s.decode_ns_per_value) << DIM
              << "  size " << fmtDelta(f.compressed_bytes, s.compressed_bytes)
              << RESET << "\n\n";
}

void printSummaryTable(const std::vector<DatasetResult> &results) {
    std::cout << BOLD << YELLOW
              << "\n+======================================================================+\n"
              << "|                         SUMMARY TABLE                                |\n"
              << "+======================================================================+\n"
              << RESET;

    std::cout << BOLD
              << std::setw(24) << std::left << "  Dataset"
              << std::setw(16) << std::right << "Enc speedup"
              << std::setw(16) << "Dec speedup"
              << std::setw(16) << "Size reduction"
              << std::setw(14) << "FFOR bits/v"
              << RESET << "\n"
              << std::string(86, '-') << "\n";

    for (auto &dr : results) {
        double enc_speedup = dr.simple16.encode_ns_per_value / dr.ffor.encode_ns_per_value;
        double dec_speedup = dr.simple16.decode_ns_per_value / dr.ffor.decode_ns_per_value;
        double size_reduction = 1.0 - (static_cast<double>(dr.ffor.compressed_bytes) / dr.simple16.compressed_bytes);

        const char *enc_color = (enc_speedup >= 1.05) ? GREEN : (enc_speedup <= 0.95) ? RED : "";
        const char *dec_color = (dec_speedup >= 1.05) ? GREEN : (dec_speedup <= 0.95) ? RED : "";
        const char *size_color = (size_reduction > 0.05) ? GREEN : (size_reduction < -0.05) ? RED : "";

        std::cout << std::setw(24) << std::left << ("  " + dr.dataset_name);

        char buf[64];
        snprintf(buf, sizeof(buf), "%s%.2fx" RESET, enc_color, enc_speedup);
        std::cout << std::setw(28) << std::right << buf;

        snprintf(buf, sizeof(buf), "%s%.2fx" RESET, dec_color, dec_speedup);
        std::cout << std::setw(28) << std::right << buf;

        snprintf(buf, sizeof(buf), "%s%.1f%%" RESET, size_color, size_reduction * 100.0);
        std::cout << std::setw(28) << std::right << buf;

        std::cout << std::setw(14) << std::fixed << std::setprecision(2) << dr.ffor.bits_per_value;
        std::cout << "\n";
    }

    std::cout << "\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    printHeader();

    const int WARMUP = 10;
    const int RUNS = 200;

    struct DatasetDef {
        std::string name;
        std::vector<uint64_t> data;
    };

    // Generate datasets at two sizes
    std::vector<DatasetDef> datasets = {
        {"Constant 1s (10K)",      genConstantInterval(10'000)},
        {"Constant 1s (100K)",     genConstantInterval(100'000)},
        {"Small jitter (10K)",     genSmallJitter(10'000)},
        {"Small jitter (100K)",    genSmallJitter(100'000)},
        {"Moderate jitter (10K)",  genModerateJitter(10'000)},
        {"Moderate jitter (100K)", genModerateJitter(100'000)},
        {"With gaps 2% (10K)",     genWithGaps(10'000)},
        {"With gaps 2% (100K)",    genWithGaps(100'000)},
        {"Random mono (10K)",      genRandomMonotonic(10'000)},
        {"Random mono (100K)",     genRandomMonotonic(100'000)},
        {"Non-monotonic (10K)",    genNonMonotonic(10'000)},
        {"Non-monotonic (100K)",   genNonMonotonic(100'000)},
    };

    std::vector<DatasetResult> results;

    for (auto &ds : datasets) {
        DatasetResult dr;
        dr.dataset_name = ds.name;
        dr.count = ds.data.size();
        dr.simple16 = benchSimple16(ds.data, WARMUP, RUNS);
        dr.ffor = benchFFOR(ds.data, WARMUP, RUNS);
        results.push_back(std::move(dr));
        printComparison(results.back());
    }

    printSummaryTable(results);

    // Check all passed
    bool all_correct = true;
    for (auto &dr : results) {
        if (!dr.simple16.correct || !dr.ffor.correct) {
            all_correct = false;
            std::cerr << RED << "CORRECTNESS FAILURE in " << dr.dataset_name << RESET << "\n";
        }
    }

    return all_correct ? 0 : 1;
}
