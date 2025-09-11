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