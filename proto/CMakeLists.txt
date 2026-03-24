# proto/CMakeLists.txt — compile timestar.proto into a static library
#
# Seastar already finds Protobuf (via SeastarDependencies.cmake), so the
# imported targets protobuf::libprotobuf and protobuf::protoc are available
# by the time this file is processed.

find_package(Protobuf REQUIRED)

# Generate C++ sources from .proto files
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS timestar.proto)

# Static library from generated code
add_library(timestar_proto STATIC ${PROTO_SRCS} ${PROTO_HDRS})

# Suppress compiler warnings in protobuf-generated code (we cannot fix those)
target_compile_options(timestar_proto PRIVATE -w)

# The generated .pb.h files land in ${CMAKE_CURRENT_BINARY_DIR}.
# Export that directory so dependents can #include "timestar.pb.h".
target_include_directories(timestar_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

# Link the protobuf runtime
target_link_libraries(timestar_proto PUBLIC protobuf::libprotobuf)
