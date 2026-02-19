# Encoder Benchmark Test
add_executable(encoder_benchmark encoder_benchmark.cpp)

target_link_libraries(encoder_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(encoder_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(encoder_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

# Add as a test so it can be run with ctest
add_test(NAME encoder_benchmark COMMAND encoder_benchmark)

# Bit Operations Benchmark Test
add_executable(bit_operations_benchmark bit_operations_benchmark.cpp)

target_link_libraries(bit_operations_benchmark PRIVATE
    libtsdb
    pthread
    benchmark::benchmark
)

target_include_directories(bit_operations_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/utils
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(bit_operations_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
    -mavx2
    -msse4.2
)

# Add as a test so it can be run with ctest
add_test(NAME bit_operations_benchmark COMMAND bit_operations_benchmark)

# Float Decoder Ultimate Benchmark Test
add_executable(float_decoder_ultimate_benchmark float_decoder_ultimate_benchmark.cpp)

target_link_libraries(float_decoder_ultimate_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(float_decoder_ultimate_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/encoding/float
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(float_decoder_ultimate_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
    -mavx2
    -mavx512f
    -mavx512dq
)

# Add as a test so it can be run with ctest
add_test(NAME float_decoder_ultimate_benchmark COMMAND float_decoder_ultimate_benchmark)

# Float Encoder Ultimate Benchmark Test
add_executable(float_encoder_ultimate_benchmark float_encoder_ultimate_benchmark.cpp)

target_link_libraries(float_encoder_ultimate_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(float_encoder_ultimate_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/encoding/float
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(float_encoder_ultimate_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
    -mavx2
    -mavx512f
    -mavx512dq
)

# Add as a test so it can be run with ctest
add_test(NAME float_encoder_ultimate_benchmark COMMAND float_encoder_ultimate_benchmark)

# Float Decoder Buffer Optimization Test
add_executable(float_decoder_buffer_optimization_test float_decoder_buffer_optimization_test.cpp)

target_link_libraries(float_decoder_buffer_optimization_test PRIVATE
    libtsdb
    pthread
)

target_include_directories(float_decoder_buffer_optimization_test PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/encoding/float
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags
target_compile_options(float_decoder_buffer_optimization_test PRIVATE
    -O3
    -march=native
    -mtune=native
)

# Add as a test
add_test(NAME float_decoder_buffer_optimization_test COMMAND float_decoder_buffer_optimization_test)

# Integer Encoder Benchmark Test
add_executable(integer_encoder_benchmark integer_encoder_benchmark.cpp)

target_link_libraries(integer_encoder_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(integer_encoder_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(integer_encoder_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

# Add as a test so it can be run with ctest
add_test(NAME integer_encoder_benchmark COMMAND integer_encoder_benchmark)

# Integer Encoder Comparison Test
add_executable(integer_encoder_comparison integer_encoder_comparison.cpp)

target_link_libraries(integer_encoder_comparison PRIVATE
    libtsdb
    pthread
)

target_include_directories(integer_encoder_comparison PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags
target_compile_options(integer_encoder_comparison PRIVATE
    -O3
    -march=native
    -mtune=native
    -mavx2
)

# Add as a test
add_test(NAME integer_encoder_comparison COMMAND integer_encoder_comparison)

# Integer Encoder Variants Benchmark Test
add_executable(integer_encoder_variants_benchmark integer_encoder_variants_benchmark.cpp)

target_link_libraries(integer_encoder_variants_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(integer_encoder_variants_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/encoding/integer
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags
target_compile_options(integer_encoder_variants_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
    -mavx2
    -mavx512f
    -mavx512dq
)

# Add as a test
add_test(NAME integer_encoder_variants_benchmark COMMAND integer_encoder_variants_benchmark)

# Aggregator Performance Benchmark
add_executable(aggregator_performance_benchmark aggregator_performance_benchmark.cpp)

target_link_libraries(aggregator_performance_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(aggregator_performance_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/query
    ${CMAKE_SOURCE_DIR}/lib/utils
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(aggregator_performance_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
    -mavx2
)

# Add as a test so it can be run with ctest
add_test(NAME aggregator_performance_benchmark COMMAND aggregator_performance_benchmark)

# TSM Compaction Benchmark
add_executable(tsm_compaction_benchmark tsm_compaction_benchmark.cpp)

target_link_libraries(tsm_compaction_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(tsm_compaction_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
    ${CMAKE_SOURCE_DIR}/lib/encoding
)

# Set optimization flags for benchmark
target_compile_options(tsm_compaction_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

# Add custom target for easy running
add_custom_target(run-compaction-bench
    COMMAND tsm_compaction_benchmark
    DEPENDS tsm_compaction_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM compaction performance benchmark"
)

# TSM Batch Read Benchmark
add_executable(tsm_batch_read_benchmark tsm_batch_read_benchmark.cpp)

target_link_libraries(tsm_batch_read_benchmark PRIVATE
    libtsdb
    Seastar::seastar
)

target_include_directories(tsm_batch_read_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
    ${CMAKE_SOURCE_DIR}/lib/encoding
)

# Set optimization flags for benchmark
target_compile_options(tsm_batch_read_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

# Add custom target for easy running
add_custom_target(run-batch-read-bench
    COMMAND tsm_batch_read_benchmark
    DEPENDS tsm_batch_read_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM batch read performance benchmark"
)

# ALP vs Gorilla Comparison Benchmark
add_executable(alp_gorilla_comparison_benchmark alp_gorilla_comparison_benchmark.cpp)

target_link_libraries(alp_gorilla_comparison_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(alp_gorilla_comparison_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/encoding/float
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

# Set optimization flags for benchmark
target_compile_options(alp_gorilla_comparison_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

# Add as a test so it can be run with ctest
add_test(NAME alp_gorilla_comparison_benchmark COMMAND alp_gorilla_comparison_benchmark)

# Add custom target for easy running
add_custom_target(run-alp-bench
    COMMAND alp_gorilla_comparison_benchmark
    DEPENDS alp_gorilla_comparison_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running ALP vs Gorilla comparison benchmark"
)

# Integer FFOR vs Simple16 Comparison Benchmark
add_executable(integer_ffor_vs_simple16_benchmark integer_ffor_vs_simple16_benchmark.cpp)

target_link_libraries(integer_ffor_vs_simple16_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(integer_ffor_vs_simple16_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/encoding/integer
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

target_compile_options(integer_ffor_vs_simple16_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

add_test(NAME integer_ffor_vs_simple16_benchmark COMMAND integer_ffor_vs_simple16_benchmark)

add_custom_target(run-ffor-bench
    COMMAND integer_ffor_vs_simple16_benchmark
    DEPENDS integer_ffor_vs_simple16_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running FFOR vs Simple16 integer encoder comparison benchmark"
)

# Boolean Encoder Benchmark: BitPack vs RLE
add_executable(bool_encoder_benchmark bool_encoder_benchmark.cpp)

target_link_libraries(bool_encoder_benchmark PRIVATE
    libtsdb
    pthread
)

target_include_directories(bool_encoder_benchmark PRIVATE
    ${CMAKE_SOURCE_DIR}/lib/encoding
    ${CMAKE_SOURCE_DIR}/lib/storage
    ${CMAKE_SOURCE_DIR}/lib/core
)

target_compile_options(bool_encoder_benchmark PRIVATE
    -O3
    -march=native
    -mtune=native
)

add_test(NAME bool_encoder_benchmark COMMAND bool_encoder_benchmark)

add_custom_target(run-bool-bench
    COMMAND bool_encoder_benchmark
    DEPENDS bool_encoder_benchmark
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running BitPack vs RLE boolean encoder comparison benchmark"
)

