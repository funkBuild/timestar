# CMakeLists.txt for function tests

# Find required packages - use the same approach as the main test directory
# GTest and GMock are already available via FetchContent in parent

# Include directories for function library
include_directories(${CMAKE_SOURCE_DIR}/lib)
include_directories(${CMAKE_SOURCE_DIR}/lib/functions)

# Helper: add a single-file function test with standard settings.
function(timestar_add_function_test TARGET_NAME SOURCE_FILE)
    add_executable(${TARGET_NAME} ${SOURCE_FILE})
    target_link_libraries(${TARGET_NAME} PRIVATE gtest_main gmock libtimestar pthread)
    target_compile_features(${TARGET_NAME} PRIVATE cxx_std_23)
    target_compile_options(${TARGET_NAME} PRIVATE -Wall -Wextra -O2 -march=native)
    gtest_discover_tests(${TARGET_NAME})
endfunction()

# Enable Google Test discovery
include(GoogleTest)

# Combined test executable (all function tests in one binary)
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
target_link_libraries(function_tests PRIVATE gtest_main gmock libtimestar pthread)
target_compile_features(function_tests PRIVATE cxx_std_23)
target_compile_options(function_tests PRIVATE -Wall -Wextra -O2 -march=native)
gtest_discover_tests(function_tests)

# Individual test executables for focused testing
timestar_add_function_test(test_framework_only                   test_function_framework.cpp)
timestar_add_function_test(test_arithmetic_only                  test_arithmetic_functions.cpp)
timestar_add_function_test(test_smoothing_only                   test_smoothing_functions.cpp)
timestar_add_function_test(test_alignment_only                   test_series_alignment.cpp)
timestar_add_function_test(test_vectorized_only                  test_vectorized_series.cpp)
timestar_add_function_test(test_interpolation_only               test_interpolation_functions.cpp)
timestar_add_function_test(test_thread_safety_only               test_function_thread_safety.cpp)
timestar_add_function_test(test_real_data_integration_only       test_real_data_integration.cpp)
timestar_add_function_test(test_real_data_integration_simplified test_real_data_integration_simplified.cpp)
timestar_add_function_test(test_resource_management_only         test_resource_management.cpp)
timestar_add_function_test(test_resource_management_simple       test_resource_management_simple.cpp)
timestar_add_function_test(test_resource_management_basic        test_resource_management_basic.cpp)
timestar_add_function_test(test_edge_cases_only                  test_edge_cases.cpp)
timestar_add_function_test(test_function_security_only           test_function_security.cpp)
timestar_add_function_test(test_function_query_parser_only       test_function_query_parser.cpp)
timestar_add_function_test(test_performance_regression_only      test_performance_regression.cpp)
timestar_add_function_test(test_production_monitoring_only       test_production_monitoring.cpp)
