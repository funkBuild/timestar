file(GLOB SRC_FILES *.cpp)

find_package (Snappy REQUIRED)
find_package (Threads REQUIRED)
find_package (Seastar REQUIRED)

add_library(libtsdb STATIC ${SRC_FILES})
target_link_libraries(libtsdb Threads::Threads Seastar::seastar)
target_include_directories(libtsdb PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
