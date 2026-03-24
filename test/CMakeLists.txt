include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.15.2
  GIT_SHALLOW TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# Collect test files from organized subdirectories
file(GLOB_RECURSE UNIT_STORAGE_TESTS CONFIGURE_DEPENDS unit/storage/*.cpp)
file(GLOB_RECURSE UNIT_ENCODING_TESTS CONFIGURE_DEPENDS unit/encoding/*.cpp)
file(GLOB_RECURSE UNIT_QUERY_TESTS CONFIGURE_DEPENDS unit/query/*.cpp)
file(GLOB_RECURSE UNIT_INDEX_TESTS CONFIGURE_DEPENDS unit/index/*.cpp)
list(FILTER UNIT_INDEX_TESTS EXCLUDE REGEX ".*/deprecated/.*")
file(GLOB_RECURSE UNIT_HTTP_TESTS CONFIGURE_DEPENDS unit/http/*.cpp)
# Exclude proto_converter_test — it is built as a standalone test binary
# to avoid ODR violations between proto-generated and internal timestar types.
list(FILTER UNIT_HTTP_TESTS EXCLUDE REGEX ".*/proto_converter_test\\.cpp$")
# Exclude proto_write_fast_path_test — same reason: ODR isolation.
list(FILTER UNIT_HTTP_TESTS EXCLUDE REGEX ".*/proto_write_fast_path_test\\.cpp$")
# Exclude protobuf_integration_test — needs explicit timestar_proto link.
list(FILTER UNIT_HTTP_TESTS EXCLUDE REGEX ".*/protobuf_integration_test\\.cpp$")
file(GLOB_RECURSE UNIT_CORE_TESTS CONFIGURE_DEPENDS unit/core/*.cpp)
file(GLOB_RECURSE UNIT_RETENTION_TESTS CONFIGURE_DEPENDS unit/retention/*.cpp)
file(GLOB_RECURSE UNIT_FUNCTIONS_TESTS CONFIGURE_DEPENDS unit/functions/*.cpp)
file(GLOB_RECURSE UNIT_UTILS_TESTS CONFIGURE_DEPENDS unit/utils/*.cpp)
file(GLOB_RECURSE INTEGRATION_TESTS CONFIGURE_DEPENDS integration/*.cpp)
file(GLOB_RECURSE E2E_TESTS CONFIGURE_DEPENDS e2e/*.cpp)
file(GLOB_RECURSE PERFORMANCE_TESTS CONFIGURE_DEPENDS performance/*.cpp)

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
    ${UNIT_RETENTION_TESTS}
    ${UNIT_FUNCTIONS_TESTS}
    ${UNIT_UTILS_TESTS}
    ${INTEGRATION_TESTS}
    ${E2E_TESTS}
    ${PERFORMANCE_TESTS}
)

# Source file paths for source-inspection tests — shared across test targets
set(SOURCE_INSPECTION_DEFS
    TSM_COMPACTOR_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm_compactor.cpp"
    QUERY_RUNNER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/query/query_runner.cpp"
    WAL_FILE_MANAGER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/wal_file_manager.cpp"
    QUERY_RESULT_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/query/query_result.hpp"
    ENGINE_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/core/engine.cpp"
    WAL_FILE_MANAGER_CPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/wal_file_manager.cpp"
    ENGINE_CPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/core/engine.cpp"
    TSM_TOMBSTONE_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm_tombstone.cpp"
    TSM_FILE_MANAGER_CPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm_file_manager.cpp"
    TSM_HPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm.hpp"
    BLOOM_FILTER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/bloom_filter.hpp"

    HTTP_SERVER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../bin/timestar_http_server.cpp"
    HTTP_LIB_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../lib/http"
    HTTP_STREAM_HANDLER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/http/http_stream_handler.cpp"
    HTTP_METADATA_HANDLER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/http/http_metadata_handler.cpp"
    DERIVED_QUERY_EXECUTOR_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/query/derived_query_executor.cpp"
    HTTP_DERIVED_QUERY_HANDLER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/http/http_derived_query_handler.cpp"
    WAL_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/wal.cpp"
    HTTP_WRITE_HANDLER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/http/http_write_handler.cpp"
    TSM_CPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm.cpp"
    ALP_ENCODER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/encoding/alp/alp_encoder.cpp"
    ALP_DECODER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/encoding/alp/alp_decoder.cpp"
    MANIFEST_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/index/native/manifest.cpp"
    TSM_WRITER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm_writer.cpp"
    SSTABLE_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/index/native/sstable.cpp"
    BOOL_ENCODER_RLE_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/encoding/bool_encoder_rle.cpp"
    FUNCTION_MONITORING_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/functions/function_monitoring.cpp"
    FUNCTION_HTTP_HANDLER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/functions/function_http_handler.cpp"
    INTERPOLATION_FUNCTIONS_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/functions/interpolation_functions.cpp"
    FUNCTION_TYPES_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/functions/function_types.hpp"
    SIMD_AGGREGATOR_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/query/simd_aggregator.cpp"
    TSM_COMPACTOR_HPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/tsm_compactor.hpp"
    STRING_ENCODER_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/encoding/string_encoder.cpp"
    COMPACTION_CPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/index/native/compaction.cpp"
    MANIFEST_HPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/index/native/manifest.hpp"
    LINE_PARSER_HPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/utils/line_parser.hpp"
    WAL_FILE_MANAGER_HPP_SOURCE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/../lib/storage/wal_file_manager.hpp"
)

# Main test executable with all tests
add_executable(timestar_test ${TEST_SRCS})
target_link_libraries(timestar_test libtimestar libtimestar_proto_conv gtest_main)
target_include_directories(timestar_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(timestar_test PRIVATE ${SOURCE_INSPECTION_DEFS})

# Discover tests for CTest
gtest_discover_tests(timestar_test)

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
    ${UNIT_RETENTION_TESTS}
    ${UNIT_FUNCTIONS_TESTS}
    ${UNIT_UTILS_TESTS}
)
add_executable(timestar_unit_test ${UNIT_TEST_SRCS})
target_link_libraries(timestar_unit_test libtimestar libtimestar_proto_conv gtest_main)
target_include_directories(timestar_unit_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_definitions(timestar_unit_test PRIVATE ${SOURCE_INSPECTION_DEFS})
gtest_discover_tests(timestar_unit_test)

# Integration tests only
if(INTEGRATION_TESTS)
    add_executable(timestar_integration_test 
        main.cpp
        ${INTEGRATION_TESTS}
    )
    target_link_libraries(timestar_integration_test libtimestar libtimestar_proto_conv gtest_main)
    target_include_directories(timestar_integration_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# E2E tests only
if(E2E_TESTS)
    add_executable(timestar_e2e_test 
        main.cpp
        ${E2E_TESTS}
    )
    target_link_libraries(timestar_e2e_test libtimestar libtimestar_proto_conv gtest_main)
    target_include_directories(timestar_e2e_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# Performance tests only (run separately due to their nature)
if(PERFORMANCE_TESTS)
    add_executable(timestar_perf_test 
        main.cpp
        ${PERFORMANCE_TESTS}
    )
    target_link_libraries(timestar_perf_test libtimestar libtimestar_proto_conv gtest_main)
    target_include_directories(timestar_perf_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
endif()

# Add custom targets for running specific test categories
add_custom_target(test-unit
    COMMAND timestar_unit_test
    DEPENDS timestar_unit_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running unit tests"
)

if(INTEGRATION_TESTS)
    add_custom_target(test-integration
        COMMAND timestar_integration_test
        DEPENDS timestar_integration_test
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running integration tests"
    )
endif()

if(E2E_TESTS)
    add_custom_target(test-e2e
        COMMAND timestar_e2e_test
        DEPENDS timestar_e2e_test
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running end-to-end tests"
    )
endif()

if(PERFORMANCE_TESTS)
    add_custom_target(test-perf
        COMMAND timestar_perf_test
        DEPENDS timestar_perf_test
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running performance tests"
    )
endif()

# Standalone HTTP handler test (for targeted testing)
add_executable(timestar_http_test
    unit/http/http_query_handler_test.cpp
)
target_link_libraries(timestar_http_test libtimestar libtimestar_proto_conv gtest_main)
target_include_directories(timestar_http_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

# Proto converter tests — standalone binary that links ONLY against
# libtimestar_proto_conv (not libtimestar) to avoid ODR violations between
# proto-generated classes and identically-named internal classes.
add_executable(timestar_proto_conv_test
    unit/http/proto_converter_test.cpp
    unit/http/proto_write_fast_path_test.cpp
)
target_link_libraries(timestar_proto_conv_test libtimestar_proto_conv timestar_proto gtest gtest_main)
target_include_directories(timestar_proto_conv_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/http
)
gtest_discover_tests(timestar_proto_conv_test)

# Protobuf integration tests — tests the full handler stack with protobuf
# wire format.  Needs timestar_proto for constructing request messages and
# parsing response messages.  Links both libtimestar (for handlers/engine)
# and libtimestar_proto_conv (for converter functions).
add_executable(timestar_proto_integration_test
    main.cpp
    unit/http/protobuf_integration_test.cpp
)
target_link_libraries(timestar_proto_integration_test
    libtimestar libtimestar_proto_conv timestar_proto gtest gtest_main)
target_include_directories(timestar_proto_integration_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../lib/http
)
gtest_discover_tests(timestar_proto_integration_test)

# Include subdirectories with standalone tests
add_subdirectory(benchmark)
add_subdirectory(functions)