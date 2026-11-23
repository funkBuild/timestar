#pragma once

#include <cstddef>
#include <cstdint>

namespace timestar::alp {

// Parameters chosen for one ALP stream.
struct AlpParams {
    uint8_t exp = 0;
    uint8_t fac = 0;
    // Sampled values that do not round-trip under (exp, fac). The caller only
    // compares this against ALP_RD_EXCEPTION_THRESHOLD: once it exceeds half
    // the sample the stream is ALP_RD-encoded and exp/fac are never read, so
    // in that case the count is only guaranteed to be above the threshold.
    size_t exceptions = 0;
};

// Choose (exp, fac) from up to ALP_SAMPLE_SIZE evenly spaced values.
//
// The answer is the candidate with the fewest sampled exceptions, ties going
// to the smallest exp and then the smallest fac — exactly what a brute-force
// scan of all 190 pairs over the whole sample returns whenever the stream ends
// up ALP-encoded. See the implementation for how it avoids that scan.
AlpParams findBestExpFac(const double* values, size_t count);

}  // namespace timestar::alp
