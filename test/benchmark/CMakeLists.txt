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