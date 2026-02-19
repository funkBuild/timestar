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

#include "../../lib/encoding/bool_encoder.hpp"
#include "../../lib/encoding/bool_encoder_rle.hpp"
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

std::vector<bool> genAllTrue(size_t count) {
    return std::vector<bool>(count, true);
}

std::vector<bool> genAllFalse(size_t count) {
    return std::vector<bool>(count, false);
}

std::vector<bool> genLongRuns(size_t count, size_t runLen = 1000) {
    std::vector<bool> v;
    v.reserve(count);
    bool val = true;
    while (v.size() < count) {
        size_t n = std::min(runLen, count - v.size());
        for (size_t i = 0; i < n; ++i) v.push_back(val);
        val = !val;
    }
    return v;
}

std::vector<bool> genMediumRuns(size_t count, size_t runLen = 100) {
    return genLongRuns(count, runLen);
}

std::vector<bool> genShortRuns(size_t count, size_t runLen = 10) {
    return genLongRuns(count, runLen);
}

std::vector<bool> genAlternating(size_t count) {
    std::vector<bool> v;
    v.reserve(count);
    for (size_t i = 0; i < count; ++i) v.push_back(i % 2 == 0);
    return v;
}

std::vector<bool> genRandom5050(size_t count) {
    std::vector<bool> v;
    v.reserve(count);
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, 1);
    for (size_t i = 0; i < count; ++i) v.push_back(dist(rng) == 1);
    return v;
}

std::vector<bool> genBiased95(size_t count) {
    std::vector<bool> v;
    v.reserve(count);
    std::mt19937 rng(123);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (size_t i = 0; i < count; ++i) v.push_back(dist(rng) < 0.95);
    return v;
}

std::vector<bool> genBiased99(size_t count) {
    std::vector<bool> v;
    v.reserve(count);
    std::mt19937 rng(456);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (size_t i = 0; i < count; ++i) v.push_back(dist(rng) < 0.99);
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
    size_t original_values;
    double bits_per_value;
    bool correct;
};

struct DatasetResult {
    std::string dataset_name;
    size_t count;
    EncoderResult bitpack;
    EncoderResult rle;
};

EncoderResult benchBitPack(const std::vector<bool> &data, int warmup, int runs) {
    EncoderResult r;
    r.encoder_name = "BitPack";
    r.original_values = data.size();

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        auto enc = BoolEncoder::encode(data);
    }

    // Encode timing
    auto enc_start = std::chrono::high_resolution_clock::now();
    AlignedBuffer encoded;
    for (int i = 0; i < runs; ++i) {
        encoded = BoolEncoder::encode(data);
    }
    auto enc_end = std::chrono::high_resolution_clock::now();

    double enc_total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(enc_end - enc_start).count();
    r.encode_ns_per_value = enc_total_ns / (static_cast<double>(runs) * data.size());
    r.compressed_bytes = encoded.size();
    r.bits_per_value = (r.compressed_bytes * 8.0) / data.size();

    // Decode timing
    std::vector<bool> decoded;
    decoded.reserve(data.size());

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        BoolEncoder::decode(s, 0, data.size(), decoded);
    }

    auto dec_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runs; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        BoolEncoder::decode(s, 0, data.size(), decoded);
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
                std::cerr << "BitPack mismatch at [" << i << "]: expected "
                          << data[i] << " got " << decoded[i] << "\n";
                break;
            }
        }
    }

    return r;
}

EncoderResult benchRLE(const std::vector<bool> &data, int warmup, int runs) {
    EncoderResult r;
    r.encoder_name = "RLE";
    r.original_values = data.size();

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        auto enc = BoolEncoderRLE::encode(data);
    }

    // Encode timing
    auto enc_start = std::chrono::high_resolution_clock::now();
    AlignedBuffer encoded;
    for (int i = 0; i < runs; ++i) {
        encoded = BoolEncoderRLE::encode(data);
    }
    auto enc_end = std::chrono::high_resolution_clock::now();

    double enc_total_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(enc_end - enc_start).count();
    r.encode_ns_per_value = enc_total_ns / (static_cast<double>(runs) * data.size());
    r.compressed_bytes = encoded.size();
    r.bits_per_value = (r.compressed_bytes * 8.0) / data.size();

    // Decode timing
    std::vector<bool> decoded;
    decoded.reserve(data.size());

    // Warmup
    for (int i = 0; i < warmup; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        BoolEncoderRLE::decode(s, 0, data.size(), decoded);
    }

    auto dec_start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < runs; ++i) {
        decoded.clear();
        Slice s(encoded.data.data(), encoded.size());
        BoolEncoderRLE::decode(s, 0, data.size(), decoded);
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
                std::cerr << "RLE mismatch at [" << i << "]: expected "
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
              << "|     BOOLEAN ENCODER COMPARISON: BitPack vs Run-Length Encoding       |\n"
              << "|     BitPack: 1 bit/value  |  RLE: varint run lengths (LEB128)       |\n"
              << "+======================================================================+\n"
              << RESET << "\n";
}

void printDatasetHeader(const std::string &name, size_t count) {
    std::cout << BOLD << CYAN << "--- " << name
              << " (" << count << " values) ---" << RESET << "\n";
}

std::string fmtDelta(double a, double b) {
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

    auto &bp = dr.bitpack;
    auto &rle = dr.rle;

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

    size_t raw_bytes = (dr.count + 7) / 8; // minimum: 1 bit per bool

    auto printRow = [&](const EncoderResult &r) {
        double ratio = (raw_bytes > 0 && r.compressed_bytes > 0)
                            ? static_cast<double>(raw_bytes) / r.compressed_bytes
                            : 0.0;
        std::cout << "  " << std::setw(12) << std::left << r.encoder_name
                  << std::setw(14) << std::right << std::fixed << std::setprecision(1) << r.encode_ns_per_value
                  << std::setw(14) << std::fixed << std::setprecision(1) << r.decode_ns_per_value
                  << std::setw(14) << r.compressed_bytes
                  << std::setw(14) << std::fixed << std::setprecision(3) << r.bits_per_value
                  << std::setw(9) << std::fixed << std::setprecision(1) << ratio << "x"
                  << "  " << (r.correct ? (GREEN "PASS" RESET) : (RED "FAIL" RESET))
                  << "\n";
    };

    printRow(bp);
    printRow(rle);

    // Delta summary
    std::cout << DIM << "  RLE vs BitPack: "
              << "encode " << fmtDelta(rle.encode_ns_per_value, bp.encode_ns_per_value) << DIM
              << "  decode " << fmtDelta(rle.decode_ns_per_value, bp.decode_ns_per_value) << DIM
              << "  size " << fmtDelta(static_cast<double>(rle.compressed_bytes),
                                       static_cast<double>(bp.compressed_bytes))
              << RESET << "\n\n";
}

void printSummaryTable(const std::vector<DatasetResult> &results) {
    std::cout << BOLD << YELLOW
              << "\n+======================================================================+\n"
              << "|                         SUMMARY TABLE                                |\n"
              << "+======================================================================+\n"
              << RESET;

    std::cout << BOLD
              << std::setw(28) << std::left << "  Dataset"
              << std::setw(14) << std::right << "BP bytes"
              << std::setw(14) << "RLE bytes"
              << std::setw(16) << "Size reduction"
              << std::setw(14) << "RLE bits/v"
              << RESET << "\n"
              << std::string(86, '-') << "\n";

    for (auto &dr : results) {
        double size_reduction = 1.0 - (static_cast<double>(dr.rle.compressed_bytes) /
                                       std::max(dr.bitpack.compressed_bytes, static_cast<size_t>(1)));

        const char *size_color = (size_reduction > 0.05) ? GREEN : (size_reduction < -0.05) ? RED : "";

        std::cout << std::setw(28) << std::left << ("  " + dr.dataset_name);
        std::cout << std::setw(14) << std::right << dr.bitpack.compressed_bytes;
        std::cout << std::setw(14) << dr.rle.compressed_bytes;

        char buf[64];
        snprintf(buf, sizeof(buf), "%s%.1f%%" RESET, size_color, size_reduction * 100.0);
        std::cout << std::setw(28) << std::right << buf;

        std::cout << std::setw(14) << std::fixed << std::setprecision(3) << dr.rle.bits_per_value;
        std::cout << "\n";
    }

    std::cout << "\n";
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    printHeader();

    const size_t N = 10'000;
    const int WARMUP = 10;
    const int RUNS = 500;

    struct DatasetDef {
        std::string name;
        std::vector<bool> data;
    };

    std::vector<DatasetDef> datasets = {
        {"All true",           genAllTrue(N)},
        {"All false",          genAllFalse(N)},
        {"Long runs (1000)",   genLongRuns(N, 1000)},
        {"Medium runs (100)",  genMediumRuns(N, 100)},
        {"Short runs (10)",    genShortRuns(N, 10)},
        {"Alternating",        genAlternating(N)},
        {"Random 50/50",       genRandom5050(N)},
        {"Biased 95/5",        genBiased95(N)},
        {"Biased 99/1",        genBiased99(N)},
    };

    std::vector<DatasetResult> results;

    for (auto &ds : datasets) {
        DatasetResult dr;
        dr.dataset_name = ds.name;
        dr.count = ds.data.size();
        dr.bitpack = benchBitPack(ds.data, WARMUP, RUNS);
        dr.rle = benchRLE(ds.data, WARMUP, RUNS);
        results.push_back(std::move(dr));
        printComparison(results.back());
    }

    printSummaryTable(results);

    // Check all passed
    bool all_correct = true;
    for (auto &dr : results) {
        if (!dr.bitpack.correct || !dr.rle.correct) {
            all_correct = false;
            std::cerr << RED << "CORRECTNESS FAILURE in " << dr.dataset_name << RESET << "\n";
        }
    }

    return all_correct ? 0 : 1;
}
