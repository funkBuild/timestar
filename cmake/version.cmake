# Version resolution.
#
# The VERSION file is the single source of truth for local and CI builds.
# Release automation instead passes -DTIMESTAR_VERSION_OVERRIDE (the git tag)
# so a published artifact is stamped from the tag it was built from rather than
# from a file that someone has to remember to bump. See
# .github/workflows/docker-publish.yml.
if(TIMESTAR_VERSION_OVERRIDE)
    set(TIMESTAR_VERSION "${TIMESTAR_VERSION_OVERRIDE}")
else()
    file(READ "${CMAKE_SOURCE_DIR}/VERSION" TIMESTAR_VERSION_RAW)
    string(STRIP "${TIMESTAR_VERSION_RAW}" TIMESTAR_VERSION)
endif()

# Fail loudly at configure time rather than emitting a version.hpp that does not
# compile (a non-numeric component lands in a constexpr int).
if(NOT TIMESTAR_VERSION MATCHES "^([0-9]+)\\.([0-9]+)\\.([0-9]+)(-[0-9A-Za-z.-]+)?$")
    message(FATAL_ERROR
        "TimeStar version '${TIMESTAR_VERSION}' is not MAJOR.MINOR.PATCH[-PRERELEASE].\n"
        "Fix the VERSION file, or pass -DTIMESTAR_VERSION_OVERRIDE=<semver>.")
endif()
set(TIMESTAR_VERSION_MAJOR "${CMAKE_MATCH_1}")
set(TIMESTAR_VERSION_MINOR "${CMAKE_MATCH_2}")
set(TIMESTAR_VERSION_PATCH "${CMAKE_MATCH_3}")

# Commit hash. Release automation passes -DTIMESTAR_GIT_COMMIT_OVERRIDE because
# .dockerignore excludes .git/, so `git describe` inside the image build would
# otherwise always report "unknown".
if(TIMESTAR_GIT_COMMIT_OVERRIDE)
    set(TIMESTAR_GIT_COMMIT "${TIMESTAR_GIT_COMMIT_OVERRIDE}")
else()
    execute_process(
        COMMAND git describe --always --dirty
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        OUTPUT_VARIABLE TIMESTAR_GIT_COMMIT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        RESULT_VARIABLE _git_result
    )
    if(NOT _git_result EQUAL 0 OR TIMESTAR_GIT_COMMIT STREQUAL "")
        set(TIMESTAR_GIT_COMMIT "unknown")
    endif()
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
