# Main HTTP server - the primary way to interact with TSDB
add_executable(tsdb_http_server tsdb_http_server.cpp)
target_link_libraries(tsdb_http_server libtsdb) 

# Benchmark tool for performance testing
add_executable(tsdb_benchmark tsdb_benchmark.cpp)
target_link_libraries(tsdb_benchmark libtsdb)

# HTTP insert benchmark (Seastar HTTP client, deterministic data)
add_executable(tsdb_insert_bench tsdb_insert_bench.cpp)
target_link_libraries(tsdb_insert_bench libtsdb)

# Expression function benchmark (Google Benchmark, no Seastar)
add_executable(expression_benchmark expression_benchmark.cpp)
target_link_libraries(expression_benchmark libtsdb benchmark::benchmark)

# Forecast function benchmark (Google Benchmark, no Seastar)
add_executable(forecast_benchmark forecast_benchmark.cpp)
target_link_libraries(forecast_benchmark libtsdb benchmark::benchmark)