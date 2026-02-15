# TSDBVersion.cmake
# Version management for TSDB

set(TSDB_VERSION_MAJOR 0)
set(TSDB_VERSION_MINOR 1)
set(TSDB_VERSION_PATCH 0)
set(TSDB_VERSION "${TSDB_VERSION_MAJOR}.${TSDB_VERSION_MINOR}.${TSDB_VERSION_PATCH}")

# Generate version header
function(tsdb_generate_version_header)
    configure_file(
        "${CMAKE_SOURCE_DIR}/cmake/version.hpp.in"
        "${CMAKE_BINARY_DIR}/generated/tsdb/version.hpp"
        @ONLY
    )
endfunction()
