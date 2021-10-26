include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/609281088cfefc76f9d0ce82e1ff6c30cc3591e5.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

file(GLOB SRCS *.cpp)

add_executable(tsdb_test ${SRCS})
target_link_libraries(tsdb_test libtsdb gtest_main )
gtest_discover_tests(tsdb_test)