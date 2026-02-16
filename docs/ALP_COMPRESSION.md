# ALP Float Compression

ALP (Adaptive Lossless floating-Point compression, SIGMOD 2024) is an alternative to the default Gorilla XOR-delta encoder for double-precision time series data. It converts doubles to integers via decimal scaling (`value * 10^exp / 10^fac`), then uses Frame-of-Reference (FFOR) bit-packing.

## Switching Between Algorithms

In `lib/encoding/float_encoder.hpp`, change the constexpr:

```cpp
// Options: FloatCompression::ALP or FloatCompression::GORILLA
static constexpr FloatCompression FLOAT_COMPRESSION = FloatCompression::ALP;
```

Rebuild after changing. The switch is compile-time with zero runtime overhead — `if constexpr` eliminates the unused code path entirely.

## Architecture

```
lib/encoding/alp/
├── alp_constants.hpp    # Power-of-10 tables, vector size, magic number
├── alp_ffor.hpp         # Header-only FFOR bit-packing (pack/unpack)
├── alp_encoder.hpp/cpp  # ALPEncoder: sampling + per-block encoding
├── alp_decoder.hpp/cpp  # ALPDecoder: block-level decoding with skip/limit
├── alp_rd.hpp/cpp       # ALP_RD fallback for random doubles
```

### Encoding Pipeline

1. **Sampling**: Try all (exp, fac) pairs on 256 sampled values, pick the pair with fewest exceptions (values that don't round-trip through decimal scaling).
2. **Per-1024 block**: Scale doubles to int64, verify round-trip, FFOR bit-pack using minimum bit-width, collect exceptions (NaN, Inf, -0.0, non-roundtrippable values).
3. **ALP_RD fallback**: If exception rate exceeds 50%, switch to Real Doubles mode — splits double bits into dictionary-encoded left part + FFOR-packed right part.

### Serialization Format

```
Stream Header (2 × uint64):
  Word 0: magic (0x414C5001) + total_values
  Word 1: num_blocks + tail_count + scheme (ALP or ALP_RD)

Per Block (ALP):
  Header: exp, fac, bit_width, exception_count, block_values, for_base
  FFOR Data: ceil(values × bw / 64) words
  Exception Positions: packed uint16 indices
  Exception Values: raw uint64 (bit_cast doubles)

Per Block (ALP_RD):
  Header: right_bw, left_bw, dict_size, right_bit_count, exception_count, block_values, right_for_base
  Dictionary + Left Indices + Right FFOR Data + Exceptions
```

## Benchmark Results

### TSM File Size (648M data points, post-compaction)

Dataset: 500 hosts × 259,200 minutes × 5 fields (temperature, humidity, pressure, cpu_pct, mem_pct). Raw size 4.9 GiB. Server restarted after insertion to force full WAL→TSM compaction (0 bytes WAL residual for both).

| Metric | ALP | Gorilla | Difference |
|---|---|---|---|
| TSM files | 825 MiB | 1.2 GiB | ALP 1.44x smaller |
| Bytes/point | 1.333 | 1.925 | ALP wins |
| Compression ratio | **5.99x** | 4.15x | ALP **44% better** |
| Index files | 734 KiB | 734 KiB | identical |

### Insert Throughput (HTTP API, 500K data points)

| Metric | ALP | Gorilla |
|---|---|---|
| Throughput | ~3.9M pts/sec | ~3.1M pts/sec |

ALP inserts are faster because FFOR bit-packing is cheaper than XOR-delta encoding with leading/trailing zero tracking.

### When to Use Which

| Data Pattern | ALP | Gorilla |
|---|---|---|
| Sensor readings (limited decimals) | Excellent | Good |
| Integer counters as doubles | Excellent | Good |
| Percentages (0-100, 1 decimal) | Excellent | Good |
| Financial ticks (random walk) | Good | Good |
| Random doubles (no decimal pattern) | Fair (ALP_RD fallback) | Fair |

ALP excels when values have limited decimal precision — the decimal-scaling step produces small integers that pack tightly. For truly random doubles, ALP falls back to ALP_RD which is comparable to Gorilla.

## Tests

```bash
# ALP-specific tests (39 tests)
./test/tsdb_test --gtest_filter="ALPEncoder*"

# Comparison benchmark (encode/decode throughput + compression ratio)
./test/benchmark/alp_gorilla_comparison_benchmark
```

## Benchmark Scripts

```bash
# End-to-end HTTP API insert + query benchmark
cd test_api/http_api_benchmark
node alp_vs_gorilla_bench.js ALP    # run with ALP build
node alp_vs_gorilla_bench.js GORILLA # run with Gorilla build

# Full file size benchmark (automated: builds both, inserts, restarts, measures)
bash filesize_bench.sh
```
