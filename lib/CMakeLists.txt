# Collect source files from all subdirectories
file(GLOB_RECURSE SRC_FILES
    core/*.cpp
    storage/*.cpp
    encoding/*.cpp
    query/*.cpp
    index/*.cpp
    http/*.cpp
    utils/*.cpp
    retention/*.cpp
    config/*.cpp
    functions/*.cpp
)

find_package (Snappy REQUIRED)
find_package (Threads REQUIRED)
# Seastar is added via add_subdirectory in the top-level CMakeLists.txt,
# so find_package is only needed when building against a system-installed Seastar.
if(NOT TARGET Seastar::seastar)
    find_package(Seastar REQUIRED)
endif()
# xxHash is used for SeriesId128 generation (XXH3_128bits).
# It is available system-wide via libxxhash-dev.
find_path(XXHASH_INCLUDE_DIR NAMES xxhash.h)
find_library(XXHASH_LIBRARY NAMES xxhash)
if(NOT XXHASH_INCLUDE_DIR OR NOT XXHASH_LIBRARY)
    message(FATAL_ERROR "xxHash not found. Please install libxxhash-dev (Ubuntu/Debian) or xxhash-devel (CentOS/RHEL)")
endif()
message(STATUS "Found xxHash: ${XXHASH_LIBRARY}")

# Glaze is provided via FetchContent from parent CMakeLists.txt

# Find LevelDB
find_path(LEVELDB_INCLUDE_DIR NAMES leveldb/db.h)
find_library(LEVELDB_LIBRARY NAMES leveldb)

if(NOT LEVELDB_INCLUDE_DIR OR NOT LEVELDB_LIBRARY)
    message(FATAL_ERROR "LevelDB not found. Please install libleveldb-dev (Ubuntu/Debian) or leveldb-devel (CentOS/RHEL)")
endif()

message(STATUS "Found LevelDB: ${LEVELDB_LIBRARY}")

# All SIMD files use Google Highway foreach_target for automatic runtime ISA
# dispatch — no manual -mavx flags needed. Highway handles per-target
# compilation internally.

add_library(libtimestar STATIC ${SRC_FILES})
target_link_libraries(libtimestar Threads::Threads Seastar::seastar Snappy::snappy ${LEVELDB_LIBRARY} glaze::glaze ${XXHASH_LIBRARY} hwy)

# Include both the lib directory and all subdirectories for header resolution
target_include_directories(libtimestar PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/encoding
    ${CMAKE_CURRENT_SOURCE_DIR}/query
    ${CMAKE_CURRENT_SOURCE_DIR}/index
    ${CMAKE_CURRENT_SOURCE_DIR}/index/native
    ${CMAKE_CURRENT_SOURCE_DIR}/http
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${CMAKE_CURRENT_SOURCE_DIR}/retention
    ${CMAKE_CURRENT_SOURCE_DIR}/config
    ${CMAKE_CURRENT_SOURCE_DIR}/functions
    ${LEVELDB_INCLUDE_DIR}
    ${XXHASH_INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/_deps/robin-map-src/include
)