#ifndef SIMD_HELPERS_HPP_INCLUDED
#define SIMD_HELPERS_HPP_INCLUDED

// Shared SIMD helper functions for query subsystem (anomaly, forecast, aggregation).

#if !TIMESTAR_ANOMALY_DISABLE_SIMD && (defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86))
#include <immintrin.h>

namespace timestar::simd {

// Horizontal sum of a 256-bit double vector (4 lanes → scalar).
inline double hsum_avx(__m256d v) {
    __m128d vlow  = _mm256_castpd256_pd128(v);
    __m128d vhigh = _mm256_extractf128_pd(v, 1);
    vlow = _mm_add_pd(vlow, vhigh);
    __m128d high64 = _mm_unpackhi_pd(vlow, vlow);
    return _mm_cvtsd_f64(_mm_add_sd(vlow, high64));
}

} // namespace timestar::simd

#endif // !TIMESTAR_ANOMALY_DISABLE_SIMD

#endif // SIMD_HELPERS_HPP_INCLUDED
