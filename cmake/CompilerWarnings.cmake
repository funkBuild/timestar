# CompilerWarnings.cmake
# Centralized compiler warning configuration for TimeStar.
#
# timestar_set_project_warnings(<target>) applies the canonical warning set to
# a first-party target (libtimestar, libtimestar_proto_conv, bin/ executables).
# Third-party code is deliberately excluded: Seastar and the FetchContent deps
# are consumed as SYSTEM includes, and the protobuf-generated timestar_proto
# library is built with -w (we cannot fix generated code).
#
# Flag curation (measured with GCC 14, Jul 2026):
# The previous "maximal" set in this file was never include()d anywhere. When
# trialled against the codebase it produced warnings at a rate of ~490 per 5
# translation units (i.e. thousands across the ~110-file library), almost all
# low-value churn, so the following were dropped:
#   -Wconversion / -Wsign-conversion  — by far the biggest offenders; size_t
#       vs. smaller-int narrowing is pervasive and intentional in the encoder
#       and aggregation hot paths.
#   -Wold-style-cast  — C casts throughout bit-twiddling / SIMD glue code.
#   -Wuseless-cast    — a handful of first-party hits that are deliberate
#       explicitness around templated size types.
#   -Wdouble-promotion — noisy in a float-heavy TSDB where promotion to
#       double is often intentional (aggregation accumulators).
# What remains is a strict superset of the old inline set from
# lib/CMakeLists.txt (-Wall -Wextra -Wpedantic -Wshadow -Wnull-dereference
# -Wformat=2) and builds warning-free today, which keeps
# TIMESTAR_WARNINGS_AS_ERRORS=ON usable in CI.

option(TIMESTAR_WARNINGS_AS_ERRORS "Treat compiler warnings as errors" OFF)

function(timestar_set_project_warnings target)
    set(CLANG_WARNINGS
        -Wall
        -Wextra
        -Wpedantic
        -Wshadow
        -Wnull-dereference
        -Wformat=2
        -Wnon-virtual-dtor
        -Woverloaded-virtual
        -Wcast-align
        -Wunused
        -Wimplicit-fallthrough
    )

    set(GCC_WARNINGS
        ${CLANG_WARNINGS}
        -Wmisleading-indentation
        -Wduplicated-cond
        -Wduplicated-branches
        -Wlogical-op
    )

    if(TIMESTAR_WARNINGS_AS_ERRORS)
        set(CLANG_WARNINGS ${CLANG_WARNINGS} -Werror)
        # GCC middle-end diagnostics (emitted after inlining, with the reported
        # location inside libstdc++/Seastar/intrinsics headers, so -isystem does
        # not suppress them) are demoted to plain warnings: with GCC 14 at -O3,
        # ~160 false positives fire inside seastar/core/future.hh coroutine
        # frames, bits/string_fortified.h and avx512 intrinsics across ~30
        # otherwise-clean TUs. They still print; every front-end (deterministic,
        # fixable) warning remains a hard error.
        set(GCC_WARNINGS
            ${GCC_WARNINGS}
            -Werror
            -Wno-error=null-dereference
            -Wno-error=uninitialized
            -Wno-error=maybe-uninitialized
            -Wno-error=array-bounds)
    endif()

    if(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
        set(PROJECT_WARNINGS ${CLANG_WARNINGS})
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(PROJECT_WARNINGS ${GCC_WARNINGS})
    else()
        message(AUTHOR_WARNING "No compiler warnings set for '${CMAKE_CXX_COMPILER_ID}' compiler.")
    endif()

    target_compile_options(${target} PRIVATE ${PROJECT_WARNINGS})
endfunction()
