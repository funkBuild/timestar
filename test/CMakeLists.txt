include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# Collect test files from organized subdirectories
file(GLOB_RECURSE UNIT_STORAGE_TESTS unit/storage/*.cpp)
file(GLOB_RECURSE UNIT_ENCODING_TESTS unit/encoding/*.cpp)
file(GLOB_RECURSE UNIT_QUERY_TESTS unit/query/*.cpp)
file(GLOB_RECURSE UNIT_INDEX_TESTS unit/index/*.cpp)
file(GLOB_RECURSE UNIT_HTTP_TESTS unit/http/*.cpp)
file(GLOB_RECURSE UNIT_CORE_TESTS unit/core/*.cpp)
file(GLOB_RECURSE INTEGRATION_TESTS integration/*.cpp)
file(GLOB_RECURSE E2E_TESTS e2e/*.cpp)
file(GLOB_RECURSE PERFORMANCE_TESTS performance/*.cpp)

# Identify standalone test executables with their own main() functions
# These are built separately and should NOT be included in the main test executables
set(STANDALONE_TESTS
    unit/query/query_parser_detailed_test.cpp
)

# Remove standalone tests with their own main() from the main test lists
foreach(STANDALONE_TEST ${STANDALONE_TESTS})
    list(REMOVE_ITEM UNIT_STORAGE_TESTS ${CMAKE_CURRENT_SOURCE_DIR}/${STANDALONE_TEST})
    list(REMOVE_ITEM UNIT_ENCODING_TESTS ${CMAKE_CURRENT_SOURCE_DIR}/${STANDALONE_TEST})
    list(REMOVE_ITEM UNIT_QUERY_TESTS ${CMAKE_CURRENT_SOURCE_DIR}/${STANDALONE_TEST})
    list(REMOVE_ITEM UNIT_INDEX_TESTS ${CMAKE_CURRENT_SOURCE_DIR}/${STANDALONE_TEST})
    list(REMOVE_ITEM UNIT_HTTP_TESTS ${CMAKE_CURRENT_SOURCE_DIR}/${STANDALONE_TEST})
endforeach()

# Combine all test sources
set(TEST_SRCS
    main.cpp
    ${UNIT_STORAGE_TESTS}
    ${UNIT_ENCODING_TESTS}
    ${UNIT_QUERY_TESTS}
    ${UNIT_INDEX_TESTS}
    ${UNIT_HTTP_TESTS}
    ${UNIT_CORE_TESTS}
    ${INTEGRATION_TESTS}
    ${E2E_TESTS}
    ${PERFORMANCE_TESTS}
)

# Main test executable with all tests
add_executable(tsdb_test ${TEST_SRCS})
target_link_libraries(tsdb_test libtsdb gtest_main)
target_include_directories(tsdb_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Discover tests for CTest
gtest_discover_tests(tsdb_test)

# Optional: Create separate test executables for different categories
# This allows running specific test categories independently

# Unit tests only
set(UNIT_TEST_SRCS
    main.cpp
    ${UNIT_STORAGE_TESTS}
    ${UNIT_ENCODING_TESTS}
    ${UNIT_QUERY_TESTS}
    ${UNIT_INDEX_TESTS}
    ${UNIT_HTTP_TESTS}
    ${UNIT_CORE_TESTS}
)
add_executable(tsdb_unit_test ${UNIT_TEST_SRCS})
target_link_libraries(tsdb_unit_test libtsdb gtest_main)
target_include_directories(tsdb_unit_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Integration tests only
if(INTEGRATION_TESTS)
    add_executable(tsdb_integration_test 
        main.cpp
        ${INTEGRATION_TESTS}
    )
    target_link_libraries(tsdb_integration_test libtsdb gtest_main)
    target_include_directories(tsdb_integration_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# E2E tests only
if(E2E_TESTS)
    add_executable(tsdb_e2e_test 
        main.cpp
        ${E2E_TESTS}
    )
    target_link_libraries(tsdb_e2e_test libtsdb gtest_main)
    target_include_directories(tsdb_e2e_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# Performance tests only (run separately due to their nature)
if(PERFORMANCE_TESTS)
    add_executable(tsdb_perf_test 
        main.cpp
        ${PERFORMANCE_TESTS}
    )
    target_link_libraries(tsdb_perf_test libtsdb gtest_main)
    target_include_directories(tsdb_perf_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# Add custom targets for running specific test categories
add_custom_target(test-unit
    COMMAND tsdb_unit_test
    DEPENDS tsdb_unit_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running unit tests"
)

add_custom_target(test-integration
    COMMAND tsdb_integration_test
    DEPENDS tsdb_integration_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running integration tests"
)

add_custom_target(test-e2e
    COMMAND tsdb_e2e_test
    DEPENDS tsdb_e2e_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running end-to-end tests"
)

add_custom_target(test-perf
    COMMAND tsdb_perf_test
    DEPENDS tsdb_perf_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running performance tests"
)

# Standalone HTTP handler test (for targeted testing)
add_executable(tsdb_http_test
    unit/http/http_query_handler_test.cpp
)
target_link_libraries(tsdb_http_test libtsdb gtest_main)
target_include_directories(tsdb_http_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Include subdirectories with standalone tests
add_subdirectory(benchmark)