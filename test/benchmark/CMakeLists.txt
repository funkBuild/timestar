# Benchmarks respect the project-wide TIMESTAR_NATIVE_ARCH option (default
# OFF for portability — see the top-level CMakeLists.txt). When ON, benchmark
# targets add -march=native -mtune=native for a CPU-pinned local build; when
# OFF they use plain -O3 like the rest of the project.
if(TIMESTAR_NATIVE_ARCH)
    set(TIMESTAR_BENCH_ARCH_FLAGS -march=native -mtune=native)
else()
    set(TIMESTAR_BENCH_ARCH_FLAGS "")
endif()

# Helper: add a benchmark executable with standard settings.
# Usage: timestar_add_benchmark(NAME source.cpp [EXTRA_LIBS lib1 ...] [EXTRA_FLAGS -flag ...] [EXTRA_INCLUDES dir ...])
function(timestar_add_benchmark TARGET_NAME SOURCE_FILE)
    cmake_parse_arguments(BENCH "" "" "EXTRA_LIBS;EXTRA_FLAGS;EXTRA_INCLUDES" ${ARGN})

    add_executable(${TARGET_NAME} ${SOURCE_FILE})
    target_link_libraries(${TARGET_NAME} PRIVATE libtimestar libtimestar_proto_conv pthread ${BENCH_EXTRA_LIBS})
    target_include_directories(${TARGET_NAME} PRIVATE
        ${CMAKE_SOURCE_DIR}/lib/encoding
        ${CMAKE_SOURCE_DIR}/lib/storage
        ${CMAKE_SOURCE_DIR}/lib/core
        ${BENCH_EXTRA_INCLUDES}
    )
    target_compile_options(${TARGET_NAME} PRIVATE -O3 ${TIMESTAR_BENCH_ARCH_FLAGS} ${BENCH_EXTRA_FLAGS})
    add_test(NAME ${TARGET_NAME} COMMAND ${TARGET_NAME})
    set_tests_properties(${TARGET_NAME} PROPERTIES LABELS "benchmark")
endfunction()

# --- Encoder benchmarks ---

timestar_add_benchmark(encoder_benchmark encoder_benchmark.cpp)

timestar_add_benchmark(bit_operations_benchmark bit_operations_benchmark.cpp
    EXTRA_LIBS benchmark::benchmark
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/utils)

timestar_add_benchmark(integer_encoder_benchmark integer_encoder_benchmark.cpp)

timestar_add_benchmark(integer_encoder_comparison integer_encoder_comparison.cpp)

# --- Query / aggregator benchmarks ---

timestar_add_benchmark(aggregator_performance_benchmark aggregator_performance_benchmark.cpp
    EXTRA_INCLUDES ${CMAKE_SOURCE_DIR}/lib/query ${CMAKE_SOURCE_DIR}/lib/utils)

# --- Storage benchmarks ---

timestar_add_benchmark(tsm_compaction_benchmark tsm_compaction_benchmark.cpp)

add_custom_target(run-compaction-bench
    COMMAND tsm_compaction_benchmark
    DEPENDS tsm_compaction_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM compaction performance benchmark"
)

add_executable(tsm_batch_read_benchmark tsm_batch_read_benchmark.cpp)
target_link_libraries(tsm_batch_read_benchmark PRIVATE libtimestar libtimestar_proto_conv Seastar::seastar)
target_include_directories(tsm_batch_read_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/storage ${CMAKE_SOURCE_DIR}/lib/core ${CMAKE_SOURCE_DIR}/lib/encoding)
target_compile_options(tsm_batch_read_benchmark PRIVATE -O3 ${TIMESTAR_BENCH_ARCH_FLAGS})
add_custom_target(run-batch-read-bench
    COMMAND tsm_batch_read_benchmark
    DEPENDS tsm_batch_read_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM batch read performance benchmark"
)

# --- Comparison benchmarks ---

