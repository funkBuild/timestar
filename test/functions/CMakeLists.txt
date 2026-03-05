# CMakeLists.txt for function tests

# Find required packages - use the same approach as the main test directory
# GTest and GMock are already available via FetchContent in parent

# Include directories for function library
include_directories(${CMAKE_SOURCE_DIR}/lib)
include_directories(${CMAKE_SOURCE_DIR}/lib/functions)

# Add function test executable (excluding files with their own main functions)
add_executable(function_tests
    test_arithmetic_functions.cpp
    test_smoothing_functions.cpp
    test_series_alignment.cpp
    test_vectorized_series.cpp
    test_interpolation_functions.cpp
    test_function_integration.cpp
    test_function_thread_safety.cpp
    test_real_data_integration.cpp
    test_resource_management_basic.cpp
    test_edge_cases.cpp
    test_performance_regression.cpp
    test_production_monitoring.cpp
)

# Link with required libraries
target_link_libraries(function_tests
    PRIVATE
    gtest_main
    gmock
    libtsdb  # Main TSDB library with function framework
    pthread
)

# Set C++ standard
target_compile_features(function_tests PRIVATE cxx_std_23)

# Add compiler flags
target_compile_options(function_tests PRIVATE
    -Wall
    -Wextra
    -O2
    -march=native  # Enable SIMD optimizations
)

# Enable Google Test discovery
include(GoogleTest)
gtest_discover_tests(function_tests)

# Create individual test executables for focused testing
add_executable(test_framework_only
    test_function_framework.cpp
)

target_link_libraries(test_framework_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_framework_only PRIVATE cxx_std_23)
target_compile_options(test_framework_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_arithmetic_only
    test_arithmetic_functions.cpp
)

target_link_libraries(test_arithmetic_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_arithmetic_only PRIVATE cxx_std_23)
target_compile_options(test_arithmetic_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_smoothing_only
    test_smoothing_functions.cpp
)

target_link_libraries(test_smoothing_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_smoothing_only PRIVATE cxx_std_23)
target_compile_options(test_smoothing_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_alignment_only
    test_series_alignment.cpp
)

target_link_libraries(test_alignment_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_alignment_only PRIVATE cxx_std_23)
target_compile_options(test_alignment_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_vectorized_only
    test_vectorized_series.cpp
)

target_link_libraries(test_vectorized_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_vectorized_only PRIVATE cxx_std_23)
target_compile_options(test_vectorized_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_interpolation_only
    test_interpolation_functions.cpp
)

target_link_libraries(test_interpolation_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_interpolation_only PRIVATE cxx_std_23)
target_compile_options(test_interpolation_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_thread_safety_only
    test_function_thread_safety.cpp
)

target_link_libraries(test_thread_safety_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_thread_safety_only PRIVATE cxx_std_23)
target_compile_options(test_thread_safety_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_real_data_integration_only
    test_real_data_integration.cpp
)

target_link_libraries(test_real_data_integration_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_real_data_integration_only PRIVATE cxx_std_23)
target_compile_options(test_real_data_integration_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_real_data_integration_simplified
    test_real_data_integration_simplified.cpp
)

target_link_libraries(test_real_data_integration_simplified
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_real_data_integration_simplified PRIVATE cxx_std_23)
target_compile_options(test_real_data_integration_simplified PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_resource_management_only
    test_resource_management.cpp
)

target_link_libraries(test_resource_management_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_resource_management_only PRIVATE cxx_std_23)
target_compile_options(test_resource_management_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_resource_management_simple
    test_resource_management_simple.cpp
)

target_link_libraries(test_resource_management_simple
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_resource_management_simple PRIVATE cxx_std_23)
target_compile_options(test_resource_management_simple PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_resource_management_basic
    test_resource_management_basic.cpp
)

target_link_libraries(test_resource_management_basic
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_resource_management_basic PRIVATE cxx_std_23)
target_compile_options(test_resource_management_basic PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_edge_cases_only
    test_edge_cases.cpp
)

target_link_libraries(test_edge_cases_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_edge_cases_only PRIVATE cxx_std_23)
target_compile_options(test_edge_cases_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_function_security_only
    test_function_security.cpp
)

target_link_libraries(test_function_security_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_function_security_only PRIVATE cxx_std_23)
target_compile_options(test_function_security_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_function_query_parser_only
    test_function_query_parser.cpp
)

target_link_libraries(test_function_query_parser_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_function_query_parser_only PRIVATE cxx_std_23)
target_compile_options(test_function_query_parser_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_performance_regression_only
    test_performance_regression.cpp
)

target_link_libraries(test_performance_regression_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_performance_regression_only PRIVATE cxx_std_23)
target_compile_options(test_performance_regression_only PRIVATE -Wall -Wextra -O2 -march=native)

add_executable(test_production_monitoring_only
    test_production_monitoring.cpp
)

target_link_libraries(test_production_monitoring_only
    PRIVATE
    gtest_main
    gmock
    libtsdb
    pthread
)

target_compile_features(test_production_monitoring_only PRIVATE cxx_std_23)
target_compile_options(test_production_monitoring_only PRIVATE -Wall -Wextra -O2 -march=native)

# Add test targets to make them discoverable
gtest_discover_tests(test_framework_only)
gtest_discover_tests(test_arithmetic_only)
gtest_discover_tests(test_smoothing_only)
gtest_discover_tests(test_alignment_only)
gtest_discover_tests(test_vectorized_only)
gtest_discover_tests(test_interpolation_only)
gtest_discover_tests(test_thread_safety_only)
gtest_discover_tests(test_real_data_integration_only)
gtest_discover_tests(test_real_data_integration_simplified)
gtest_discover_tests(test_resource_management_only)
gtest_discover_tests(test_resource_management_simple)
gtest_discover_tests(test_resource_management_basic)
gtest_discover_tests(test_edge_cases_only)
gtest_discover_tests(test_function_security_only)
gtest_discover_tests(test_performance_regression_only)
gtest_discover_tests(test_production_monitoring_only)
gtest_discover_tests(test_function_query_parser_only)