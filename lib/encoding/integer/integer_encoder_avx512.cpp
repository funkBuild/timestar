// IntegerEncoderAVX512 implementation has been merged into integer_encoder_simd.cpp.
// Both IntegerEncoderSIMD and IntegerEncoderAVX512 now use Google Highway for
// portable SIMD dispatch (SSE4, AVX2, AVX-512 selected automatically at runtime).
//
// The public API (encode, decode, isAvailable) for IntegerEncoderAVX512 is
// defined in the HWY_ONCE block of integer_encoder_simd.cpp, which is compiled
// exactly once despite the foreach_target re-inclusion pattern.
//
// This file is intentionally empty to avoid duplicate symbol definitions.
