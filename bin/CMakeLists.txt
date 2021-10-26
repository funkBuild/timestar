file(GLOB SRC_FILES *.cpp)
add_executable(tsdb ${SRC_FILES})
target_link_libraries(tsdb libtsdb) 