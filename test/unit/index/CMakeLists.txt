# Standalone async test runner for LevelDB index tests
add_executable(leveldb_async_test
    leveldb_async_test_runner.cpp
    leveldb_index_test.cpp
)
target_link_libraries(leveldb_async_test libtsdb gtest)
target_include_directories(leveldb_async_test PRIVATE ${CMAKE_SOURCE_DIR}/lib)

# Add custom target to run the async tests
add_custom_target(test-leveldb-async
    COMMAND leveldb_async_test
    DEPENDS leveldb_async_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running LevelDB async tests"
)
