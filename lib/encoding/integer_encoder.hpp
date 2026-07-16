#pragma once

#include "integer/integer_encoder_ffor.hpp"

/**
 * IntegerEncoder - FFOR (Frame-of-Reference) based integer encoder.
 *
 * Alias for IntegerEncoderFFOR: delta-of-delta + ZigZag preprocessing
 * followed by block-based FFOR bit-packing with an exception mechanism
 * for outliers. Google Highway SIMD is used for vectorized operations.
 */
using IntegerEncoder = IntegerEncoderFFOR;
