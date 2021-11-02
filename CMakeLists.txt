cmake_minimum_required(VERSION 3.14)

set(CMAKE_C_COMPILER "/usr/bin/clang-13")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++-13")

project(tsdb)

enable_testing()

enable_language(CXX)
enable_language(C)
enable_language(ASM)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g")

add_definitions(-fcoroutines-ts)


set(CMAKE_BUILD_TYPE Release)
#set(CMAKE_BUILD_TYPE Debug)

#set (CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -fno-omit-frame-pointer -fsanitize=address")
#set (CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_subdirectory(external/seastar)

add_subdirectory(lib)
add_subdirectory(bin)
add_subdirectory(test)