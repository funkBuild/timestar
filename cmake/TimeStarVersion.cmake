# TimeStarVersion.cmake
# Version management for TimeStar

set(TIMESTAR_VERSION_MAJOR 0)
set(TIMESTAR_VERSION_MINOR 1)
set(TIMESTAR_VERSION_PATCH 0)
set(TIMESTAR_VERSION "${TIMESTAR_VERSION_MAJOR}.${TIMESTAR_VERSION_MINOR}.${TIMESTAR_VERSION_PATCH}")

# Generate version header
function(timestar_generate_version_header)
    configure_file(
        "${CMAKE_SOURCE_DIR}/cmake/version.hpp.in"
        "${CMAKE_BINARY_DIR}/generated/timestar/version.hpp"
        @ONLY
    )
endfunction()
