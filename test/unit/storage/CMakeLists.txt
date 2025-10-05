# Standalone WAL deletion test
add_executable(wal_deletion_test wal_deletion_test.cpp)
target_link_libraries(wal_deletion_test libtsdb)
target_include_directories(wal_deletion_test PRIVATE ${CMAKE_SOURCE_DIR}/lib)

# Add custom target to run the test
add_custom_target(test-wal-deletion
    COMMAND wal_deletion_test
    DEPENDS wal_deletion_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running WAL deletion test"
)

# Standalone async test runners
# TSM async tests
add_executable(tsm_async_test
    tsm_async_test_runner.cpp
    tsm_test.cpp
)
target_link_libraries(tsm_async_test libtsdb gtest)
target_include_directories(tsm_async_test PRIVATE ${CMAKE_SOURCE_DIR}/lib)

# WAL async tests
add_executable(wal_async_test
    wal_async_test_runner.cpp
    wal_test.cpp
)
target_link_libraries(wal_async_test libtsdb gtest)
target_include_directories(wal_async_test PRIVATE ${CMAKE_SOURCE_DIR}/lib)

# MemoryStore async tests
add_executable(memory_store_async_test
    memory_store_async_test_runner.cpp
    memory_store_test.cpp
)
target_link_libraries(memory_store_async_test libtsdb gtest)
target_include_directories(memory_store_async_test PRIVATE ${CMAKE_SOURCE_DIR}/lib)

# Add custom targets to run the async tests
add_custom_target(test-tsm-async
    COMMAND tsm_async_test
    DEPENDS tsm_async_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running TSM async tests"
)

add_custom_target(test-wal-async
    COMMAND wal_async_test
    DEPENDS wal_async_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running WAL async tests"
)

add_custom_target(test-memory-store-async
    COMMAND memory_store_async_test
    DEPENDS memory_store_async_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running MemoryStore async tests"
)