cmake_minimum_required(VERSION 3.20)

# ──────────────────────────────────────────────────────────────────────────────
#  timestar — top‑level build
#  ---------------------------------------------------------------------------
#  Strategy for “libstdc++ 13 + Seastar” build failures
#  ---------------------------------------------------------------------------
#  * Clang ≤ 18 trips over several C++20/23 libstdc++ headers (chrono, ranges),
#    regardless of the language level we feed it.
#  * GCC 13 also fails in C++23 mode because it lacks the parenthesised‑auto
#    placeholder accepted by libstdc++ 13 headers.
#  * GCC 14 (released May 2024) builds Seastar fine in C++23 **and** satisfies
#    libstdc++ 13.
#
#  Therefore we try to build the whole project with **GCC ≥ 14**.  If that
#  compiler is not available we fall back to GCC 13 + C++20 *only* for Seastar
#  (everything else stays on C++23).  Building with Clang is blocked with an
#  explicit diagnostic because it is known‑bad at the moment.
# ──────────────────────────────────────────────────────────────────────────────

# ───────────────────────── attempt to pick a suitable compiler ───────────────
# These variables **must** be set *before* the first `project()` call; otherwise
# CMake has already locked in the compiler.

if(NOT CMAKE_CXX_COMPILER)
  # Prefer g++‑14 if it exists
  find_program(_GXX14 g++-14)
  if(_GXX14)
    set(CMAKE_CXX_COMPILER "${_GXX14}" CACHE FILEPATH "C++ compiler" FORCE)
    get_filename_component(_GCC_DIR "${_GXX14}" DIRECTORY)
    set(CMAKE_C_COMPILER  "${_GCC_DIR}/gcc-14" CACHE FILEPATH "C compiler" FORCE)
  else()
    # Fallback to the default compiler chosen by CMake (g++‑13 or clang‑18)
  endif()
endif()

project(timestar LANGUAGES C CXX ASM)

# ──────────────────────────── version stamping ───────────────────────────────
include(cmake/version.cmake)

# ─────────────────────── sanity check on the chosen compiler ─────────────────

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 19)
    message(FATAL_ERROR "Clang ${CMAKE_CXX_COMPILER_VERSION} is known to fail \n"
                        "with libstdc++ 13 <chrono>/<ranges>.  Please install \n"
                        "either GCC ≥ 14 or Clang ≥ 19, or pass CC/CXX to cmake.")
  endif()
endif()

# ───────────────────── select dialect for building Seastar ───────────────────
#  • GCC ≥ 14 or Clang ≥ 19 ⇒ we keep Seastar at its native C++23.
#  • Otherwise (currently GCC 13) we downgrade Seastar to C++20.
# -----------------------------------------------------------------------------

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
  set(SEASTAR_CXX_STANDARD 20)
else()
  set(SEASTAR_CXX_STANDARD 23)
endif()

message(STATUS "Seastar will be compiled in C++${SEASTAR_CXX_STANDARD} using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

# Lock the choice *before* we descend into the Seastar directory.
set(CMAKE_CXX_STANDARD "${SEASTAR_CXX_STANDARD}" CACHE STRING "C++ standard for Seastar" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ───────────────────────────── common compile flags ──────────────────────────

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")
option(TIMESTAR_NATIVE_ARCH "Compile for the host CPU (-march=native)" ON)
if(TIMESTAR_NATIVE_ARCH)
  add_compile_options(-march=native)
endif()

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# ───────────────────────────── security hardening ────────────────────────────
add_compile_options(-fstack-protector-strong)
add_compile_definitions($<$<NOT:$<CONFIG:Debug>>:_FORTIFY_SOURCE=2>)
add_link_options(-Wl,-z,relro -Wl,-z,now)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ───────────────────────────── Link-Time Optimization ────────────────────────
# Enable with -DTIMESTAR_ENABLE_LTO=ON.  LTO lets the linker eliminate dead code
# across translation units, significantly reducing stripped binary size.

option(TIMESTAR_ENABLE_LTO "Enable interprocedural / link-time optimization" OFF)
if(TIMESTAR_ENABLE_LTO)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT _ipo_supported OUTPUT _ipo_error)
  if(_ipo_supported)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
    message(STATUS "LTO enabled")
  else()
    message(WARNING "LTO requested but not supported: ${_ipo_error}")
  endif()
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ───────────────────────────── build external Seastar ─────────────────────────

add_subdirectory(external/seastar)

# ───────────────────────────── project’s own targets ─────────────────────────
# If we had to lower the standard for Seastar, raise it back again for the rest
# of this repository.  GCC 13 + C++23 will still fail, so keep *everything*
# on 23 only if the compiler is actually capable.
# -----------------------------------------------------------------------------

if(NOT (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14))
  set(CMAKE_CXX_STANDARD 23)
else()
  set(CMAKE_CXX_STANDARD 20)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ───────────────────────────── fetch external dependencies ─────────────────────
include(FetchContent)

FetchContent_Declare(
  glaze
  GIT_REPOSITORY https://github.com/stephenberry/glaze.git
  GIT_TAG v7.2.1
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  robin-map
  GIT_REPOSITORY https://github.com/Tessil/robin-map.git
  GIT_TAG v1.3.0
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  highway
  GIT_REPOSITORY https://github.com/google/highway.git
  GIT_TAG 1.2.0
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  roaring
  GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
  GIT_TAG v4.1.7
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  zstd
  GIT_REPOSITORY https://github.com/facebook/zstd.git
  GIT_TAG v1.5.6
  GIT_SHALLOW TRUE
  SOURCE_SUBDIR build/cmake
)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ROARING_BUILD_LTO OFF CACHE BOOL "" FORCE)
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(glaze googlebenchmark robin-map highway roaring zstd)

# ──────────────────────────── protobuf message library ────────────────────────
# Must come after Seastar (which finds Protobuf) so the imported targets exist.

add_subdirectory(proto)

# ─────────────────────────────── sub‑directories ─────────────────────────────

add_subdirectory(lib)
add_subdirectory(bin)

option(TIMESTAR_BUILD_TESTS "Build test executables" ON)
if(TIMESTAR_BUILD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

# End of CMakeLists.txt
