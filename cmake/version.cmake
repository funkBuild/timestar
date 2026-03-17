# Read version from VERSION file (single source of truth)
file(READ "${CMAKE_SOURCE_DIR}/VERSION" TIMESTAR_VERSION_RAW)
string(STRIP "${TIMESTAR_VERSION_RAW}" TIMESTAR_VERSION)
string(REPLACE "." ";" TIMESTAR_VERSION_LIST "${TIMESTAR_VERSION}")
list(GET TIMESTAR_VERSION_LIST 0 TIMESTAR_VERSION_MAJOR)
list(GET TIMESTAR_VERSION_LIST 1 TIMESTAR_VERSION_MINOR)
list(GET TIMESTAR_VERSION_LIST 2 TIMESTAR_VERSION_PATCH)

# Capture git commit hash (falls back to "unknown" if git unavailable)
execute_process(
    COMMAND git describe --always --dirty
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE TIMESTAR_GIT_COMMIT
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE _git_result
)
if(NOT _git_result EQUAL 0)
    set(TIMESTAR_GIT_COMMIT "unknown")
endif()

# Build timestamp (ISO 8601 UTC)
string(TIMESTAMP TIMESTAR_BUILD_TIME "%Y-%m-%dT%H:%M:%SZ" UTC)

# Generate version header
configure_file(
    "${CMAKE_SOURCE_DIR}/cmake/version.hpp.in"
    "${CMAKE_BINARY_DIR}/generated/timestar/version.hpp"
    @ONLY
)

message(STATUS "TimeStar version: ${TIMESTAR_VERSION} (${TIMESTAR_GIT_COMMIT})")
