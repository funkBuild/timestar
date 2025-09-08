# SIMD Optimization for Query Processing

## Overview

This document describes the SIMD (Single Instruction, Multiple Data) optimizations implemented for the TSDB query processing engine. These optimizations leverage AVX2 instructions to accelerate aggregation operations, providing significant performance improvements for large-scale time series data processing.

## Implementation

### Core Components

1. **SimdAggregator** (`lib/query/simd_aggregator.hpp/cpp`)
   - AVX2-optimized implementations of core aggregation functions
   - Runtime CPU feature detection
   - Automatic fallback to scalar implementations when AVX2 is unavailable

2. **Integration with Aggregator** (`lib/query/aggregator.cpp`)
   - Transparent SIMD acceleration - no API changes required
   - Automatic selection of optimal implementation based on CPU capabilities

### Optimized Operations

The following aggregation operations have been optimized with SIMD:

- **SUM**: Parallel addition of 4 doubles per instruction
- **AVG**: SIMD sum followed by scalar division
- **MIN**: Parallel minimum comparison across 4 lanes
- **MAX**: Parallel maximum comparison across 4 lanes
- **VARIANCE**: Vectorized sum of squared differences
- **DOT PRODUCT**: Parallel multiply-add operations

### Additional Features

- **Batch Bucket Processing**: Process multiple time buckets in parallel
- **Fast Histogram Computation**: SIMD-accelerated binning for percentile calculations
- **Aligned Memory Operations**: Optimal memory access patterns for SIMD

## Performance Benefits

### Expected Speedups

Based on the implementation, typical performance improvements are:

- **Small datasets (< 100 elements)**: Minimal improvement (scalar is preferred)
- **Medium datasets (100-10K elements)**: 2-3x speedup
- **Large datasets (10K+ elements)**: 3-4x speedup
- **Very large datasets (1M+ elements)**: Up to 4x speedup (memory bandwidth limited)

### Factors Affecting Performance

1. **Data Size**: Larger arrays benefit more from SIMD
2. **Memory Alignment**: Aligned data provides better performance
3. **CPU Cache**: Data that fits in L1/L2 cache sees maximum benefit
4. **Memory Bandwidth**: Very large datasets may be bandwidth-limited

## Architecture Support

### CPU Requirements

- **Minimum**: x86-64 processor with AVX2 support
- **Recommended**: Intel Haswell (2013) or newer, AMD Excavator (2015) or newer
- **Optimal**: Intel Ice Lake or AMD Zen 3 with improved AVX2 throughput

### Runtime Detection

The implementation includes runtime CPU feature detection:
```cpp
if (SimdAggregator::isAvx2Available()) {
    // Use SIMD path
} else {
    // Use scalar fallback
}
```

## Building with SIMD Support

The project automatically enables SIMD when building:

```bash
cmake .. # Uses -march=native to enable CPU-specific optimizations
make -j8
```

To explicitly enable AVX2:
```bash
cmake -DCMAKE_CXX_FLAGS="-mavx2 -mfma" ..
make -j8
```

## Testing and Benchmarking

### Running the Benchmark

```bash
./test/benchmark/simd_aggregator_benchmark
```

This will output performance comparisons between scalar and SIMD implementations across various data sizes.

### Integration with Query Engine

SIMD optimizations are automatically used by the query engine when:
1. Processing aggregation queries (`avg`, `min`, `max`, `sum`)
2. Computing time-bucketed aggregations
3. Performing group-by operations

Example query that benefits from SIMD:
```json
{
  "query": "avg:temperature(value){location:us-west}",
  "startTime": 1709251200000000000,
  "endTime": 1709337600000000000,
  "aggregationInterval": "5m"
}
```

## Future Enhancements

### Planned Optimizations

1. **AVX-512 Support**: 8-wide operations for newer Intel/AMD processors
2. **Streaming Aggregations**: Process data as it arrives without buffering
3. **SIMD Compression**: Accelerate timestamp/value encoding/decoding
4. **Parallel Query Execution**: Process multiple series simultaneously
5. **GPU Acceleration**: Offload massive aggregations to GPU when available

### Potential Improvements

- **Prefetching**: Explicit cache prefetch instructions for better memory throughput
- **Loop Unrolling**: Process 8 or 16 values per iteration for reduced overhead
- **Compiler Intrinsics**: Use compiler-specific optimizations for better code generation
- **NUMA Awareness**: Optimize for multi-socket systems

## Maintenance Notes

### Adding New SIMD Functions

To add a new SIMD-optimized function:

1. Add the declaration to `simd_aggregator.hpp`
2. Implement both SIMD and scalar versions in `simd_aggregator.cpp`
3. Update the main aggregator to use the SIMD version when available
4. Add unit tests to verify correctness
5. Add benchmark comparisons

### Debugging SIMD Code

- Use `-fsanitize=address` to catch alignment issues
- Verify results match scalar implementation exactly
- Test with various data sizes including non-multiple-of-4 counts
- Check edge cases: empty arrays, single elements, NaN/Inf values

## Performance Monitoring

To monitor SIMD utilization in production:

1. Check CPU performance counters for AVX2 instruction usage
2. Monitor query latency improvements
3. Track CPU utilization (should decrease for same workload)
4. Measure memory bandwidth utilization

## References

- [Intel Intrinsics Guide](https://software.intel.com/sites/landingpage/IntrinsicsGuide/)
- [AVX2 Programming Reference](https://www.intel.com/content/www/us/en/docs/intrinsics-guide/index.html)
- [Agner Fog's Optimization Manuals](https://www.agner.org/optimize/)
- [SIMD for Time Series Databases](https://www.timescale.com/blog/how-we-made-distinct-queries-up-to-8000x-faster-on-postgresql/)