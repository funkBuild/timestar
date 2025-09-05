# Main HTTP server - the primary way to interact with TSDB
add_executable(tsdb_http_server tsdb_http_server.cpp)
target_link_libraries(tsdb_http_server libtsdb) 

# Benchmark tool for performance testing
add_executable(tsdb_benchmark tsdb_benchmark.cpp)
target_link_libraries(tsdb_benchmark libtsdb)