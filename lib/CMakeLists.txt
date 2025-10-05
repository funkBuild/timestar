# Collect source files from all subdirectories
file(GLOB_RECURSE SRC_FILES 
    core/*.cpp
    storage/*.cpp
    encoding/*.cpp
    query/*.cpp
    index/*.cpp
    http/*.cpp
    utils/*.cpp
)

find_package (Snappy REQUIRED)
find_package (Threads REQUIRED)
find_package (Seastar REQUIRED)
find_package (OpenSSL REQUIRED)

# Glaze is provided via FetchContent from parent CMakeLists.txt

# Find LevelDB
find_path(LEVELDB_INCLUDE_DIR NAMES leveldb/db.h)
find_library(LEVELDB_LIBRARY NAMES leveldb)

if(NOT LEVELDB_INCLUDE_DIR OR NOT LEVELDB_LIBRARY)
    message(FATAL_ERROR "LevelDB not found. Please install libleveldb-dev (Ubuntu/Debian) or leveldb-devel (CentOS/RHEL)")
endif()

message(STATUS "Found LevelDB: ${LEVELDB_LIBRARY}")

add_library(libtsdb STATIC ${SRC_FILES})
target_link_libraries(libtsdb Threads::Threads Seastar::seastar Snappy::snappy ${LEVELDB_LIBRARY} glaze::glaze OpenSSL::Crypto)

# Include both the lib directory and all subdirectories for header resolution
target_include_directories(libtsdb PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/core
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/encoding
    ${CMAKE_CURRENT_SOURCE_DIR}/query
    ${CMAKE_CURRENT_SOURCE_DIR}/index
    ${CMAKE_CURRENT_SOURCE_DIR}/http
    ${CMAKE_CURRENT_SOURCE_DIR}/utils
    ${LEVELDB_INCLUDE_DIR}
    ${CMAKE_BINARY_DIR}/_deps/robin-map-src/include
)