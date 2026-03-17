# Helper: add a benchmark executable with standard settings.
# Usage: timestar_add_benchmark(NAME source.cpp [EXTRA_LIBS lib1 ...] [EXTRA_FLAGS -flag ...] [EXTRA_INCLUDES dir ...])
function(timestar_add_benchmark TARGET_NAME SOURCE_FILE)
    cmake_parse_arguments(BENCH "" "" "EXTRA_LIBS;EXTRA_FLAGS;EXTRA_INCLUDES" ${ARGN})

    add_executable(${TARGET_NAME} ${SOURCE_FILE})
    target_link_libraries(${TARGET_NAME} PRIVATE libtimestar pthread ${BENCH_EXTRA_LIBS})
    target_include_directories(${TARGET_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/lib/encoding
        ${CMAKE_SOURCE_DIR}/lib/storage
        ${CMAKE_SOURCE_DIR}/lib/core
        ${BENCH_EXTRA_INCLUDES}
    )
    target_compile_options(${TARGET_NAME} PRIVATE -O3 -march=native -mtune=native ${BENCH_EXTRA_FLAGS})
    add_test(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME})
    set_tests_properties(${TARGET_NAME} PROPERTIES LABELS "benchmark")
endfunction()

# --- Encoder benchmarks ---

timestar_add_benchmark(encoder_benchmark encoder_benchmark.cpp)

timestar_add_benchmark(bit_operations_benchmark bit_operations_benchmark.cpp
    EXTRA_LIBS benchmark::benchmark
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/utils
    EXTRA_FLAGS -mavx2 -msse4.2)

timestar_add_benchmark(float_decoder_ultimate_benchmark float_decoder_ultimate_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/encoding/float
    EXTRA_FLAGS -mavx2 -mavx512f -mavx512dq)

timestar_add_benchmark(float_encoder_ultimate_benchmark float_encoder_ultimate_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/encoding/float
    EXTRA_FLAGS -mavx2 -mavx512f -mavx512dq)

timestar_add_benchmark(float_decoder_buffer_optimization_test float_decoder_buffer_optimization_test.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/encoding/float)

timestar_add_benchmark(integer_encoder_benchmark integer_encoder_benchmark.cpp)

timestar_add_benchmark(integer_encoder_comparison integer_encoder_comparison.cpp
    EXTRA_FLAGS -mavx2)

timestar_add_benchmark(integer_encoder_variants_benchmark integer_encoder_variants_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/encoding/integer
    EXTRA_FLAGS -mavx2 -mavx512f -mavx512dq)

# --- Query / aggregator benchmarks ---

timestar_add_benchmark(aggregator_performance_benchmark aggregator_performance_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/query ${CMAKE_SOURCE_DIR}/lib/utils
    EXTRA_FLAGS -mavx2)

# --- Storage benchmarks ---

timestar_add_benchmark(tsm_compaction_benchmark tsm_compaction_benchmark.cpp)

add_custom_target(run-compaction-bench
    COMMAND tsm_compaction_benchmark
    DEPENDS tsm_compaction_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM compaction performance benchmark"
)

add_executable(tsm_batch_read_benchmark tsm_batch_read_benchmark.cpp)
target_link_libraries(tsm_batch_read_benchmark PRIVATE libtimestar Seastar::seastar)
target_include_directories(tsm_batch_read_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/storage ${CMAKE_SOURCE_DIR}/lib/core ${CMAKE_SOURCE_DIR}/lib/encoding)
target_compile_options(tsm_batch_read_benchmark PRIVATE -O3 -march=native -mtune=native)
add_custom_target(run-batch-read-bench
    COMMAND tsm_batch_read_benchmark
    DEPENDS tsm_batch_read_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM batch read performance benchmark"
)

# --- Comparison benchmarks ---

timestar_add_benchmark(alp_gorilla_comparison_benchmark alp_gorilla_comparison_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/encoding/float)

add_custom_target(run-alp-bench
    COMMAND alp_gorilla_comparison_benchmark
    DEPENDS alp_gorilla_comparison_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running ALP vs Gorilla comparison benchmark"
)

timestar_add_benchmark(integer_ffor_vs_simple16_benchmark integer_ffor_vs_simple16_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/encoding/integer)

add_custom_target(run-ffor-bench
    COMMAND integer_ffor_vs_simple16_benchmark
    DEPENDS integer_ffor_vs_simple16_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running FFOR vs Simple16 integer encoder comparison benchmark"
)

timestar_add_benchmark(bool_encoder_benchmark bool_encoder_benchmark.cpp)

add_custom_target(run-bool-bench
    COMMAND bool_encoder_benchmark
    DEPENDS bool_encoder_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running BitPack vs RLE boolean encoder comparison benchmark"
)
