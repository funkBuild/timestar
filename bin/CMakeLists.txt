# Main HTTP server - the primary way to interact with TimeStar
add_executable(timestar_http_server timestar_http_server.cpp)
target_link_libraries(timestar_http_server libtimestar libtimestar_proto_conv)

# Benchmark tool for performance testing
add_executable(timestar_benchmark timestar_benchmark.cpp)
target_link_libraries(timestar_benchmark libtimestar libtimestar_proto_conv)

# HTTP insert benchmark (Seastar HTTP client, deterministic data)
# Links timestar_proto directly for #include "timestar.pb.h" (protobuf format support)
add_executable(timestar_insert_bench timestar_insert_bench.cpp)
target_link_libraries(timestar_insert_bench libtimestar libtimestar_proto_conv timestar_proto)

# HTTP query benchmark (Seastar HTTP client, comprehensive query profiling)
# Links timestar_proto directly for #include "timestar.pb.h" (protobuf format support)
add_executable(timestar_query_bench timestar_query_bench.cpp)
target_link_libraries(timestar_query_bench libtimestar libtimestar_proto_conv timestar_proto)

# Expression function benchmark (Google Benchmark, no Seastar)
add_executable(expression_benchmark expression_benchmark.cpp)
target_link_libraries(expression_benchmark libtimestar libtimestar_proto_conv benchmark::benchmark)

# Forecast function benchmark (Google Benchmark, no Seastar)
add_executable(forecast_benchmark forecast_benchmark.cpp)
target_link_libraries(forecast_benchmark libtimestar libtimestar_proto_conv benchmark::benchmark)