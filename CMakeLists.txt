cmake_minimum_required(VERSION 3.20)

# ──────────────────────────────────────────────────────────────────────────────
#  timestar — top‑level build
#  ---------------------------------------------------------------------------
#  Compiler requirement
#  ---------------------------------------------------------------------------
#  The vendored Seastar (external/seastar, tracking upstream master) uses C++23
#  library features that first shipped in libstdc++ 15 (the ranges-aware
#  container members such as `std::vector::assign_range`).  Upstream supports
#  the two most recent GCC majors, so:
#
#  * GCC ≥ 15 builds everything in C++23.  This is the supported toolchain.
#  * GCC ≤ 14 cannot compile Seastar and is rejected below with a clear
#    diagnostic instead of a template error deep inside Seastar.
#  * Clang ≤ 18 trips over several C++20/23 libstdc++ headers (chrono, ranges)
#    and is rejected as before; newer Clang must be paired with libstdc++ ≥ 15.
# ──────────────────────────────────────────────────────────────────────────────

# ───────────────────────── attempt to pick a suitable compiler ───────────────
# These variables **must** be set *before* the first `project()` call; otherwise
# CMake has already locked in the compiler.

if(NOT CMAKE_CXX_COMPILER AND NOT DEFINED ENV{CXX})
  # Prefer a versioned GCC 15 if it exists: the unversioned `g++` on older
  # distributions is too old.  Otherwise fall through to CMake's default, which
  # the sanity check below validates.
  find_program(_GXX15 g++-15)
  if(_GXX15)
    set(CMAKE_CXX_COMPILER "${_GXX15}" CACHE FILEPATH "C++ compiler" FORCE)
    get_filename_component(_GCC_DIR "${_GXX15}" DIRECTORY)
    set(CMAKE_C_COMPILER  "${_GCC_DIR}/gcc-15" CACHE FILEPATH "C compiler" FORCE)
  endif()
endif()

project(timestar LANGUAGES C CXX ASM)

# ──────────────────────────── version stamping ───────────────────────────────
include(cmake/version.cmake)

# ─────────────────────────── canonical warning flags ─────────────────────────
# Provides timestar_set_project_warnings(<target>), applied to every
# first-party target (libtimestar, libtimestar_proto_conv, bin/ executables).
# Toggle -Werror with -DTIMESTAR_WARNINGS_AS_ERRORS=ON.
include(cmake/CompilerWarnings.cmake)

# ─────────────────────── sanity check on the chosen compiler ─────────────────

if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 19)
    message(FATAL_ERROR "Clang ${CMAKE_CXX_COMPILER_VERSION} is known to fail \n"
                        "with libstdc++ <chrono>/<ranges>.  Please install \n"
                        "GCC ≥ 15, or pass CC/CXX to cmake.")
  endif()
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 15)
  message(FATAL_ERROR "GCC ${CMAKE_CXX_COMPILER_VERSION} cannot build the vendored Seastar: \n"
                      "it needs C++23 library features from libstdc++ 15 \n"
                      "(e.g. std::vector::assign_range).  Please install GCC ≥ 15 \n"
                      "and pass -DCMAKE_C_COMPILER=gcc-15 -DCMAKE_CXX_COMPILER=g++-15.")
endif()

# ───────────────────── select dialect for building Seastar ───────────────────
# Seastar is built at its native C++23, the same dialect as the rest of the
# project.  Lock the choice *before* we descend into the Seastar directory.
# -----------------------------------------------------------------------------

set(SEASTAR_CXX_STANDARD 23)
message(STATUS "Seastar will be compiled in C++${SEASTAR_CXX_STANDARD} using ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

set(CMAKE_CXX_STANDARD "${SEASTAR_CXX_STANDARD}" CACHE STRING "C++ standard for Seastar" FORCE)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ───────────────────────────── common compile flags ──────────────────────────

set(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -DNDEBUG")
# Default OFF for portability: -march=native bakes in the build host's
# instruction set, so a binary built on a newer CPU SIGILLs on an older one —
# a hazard for any distributed/deployed build. The SIMD hot paths go through
# Google Highway, which selects the optimal kernel at runtime regardless of
# this flag, so -march=native only affects scalar autovectorization. Opt in
# with -DTIMESTAR_NATIVE_ARCH=ON for a CPU-pinned local/bench build.
option(TIMESTAR_NATIVE_ARCH "Compile for the host CPU (-march=native)" OFF)
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

# ──────────────── compatibility floor for vendored dependencies ──────────────
# Some bundled/fetched dependencies (e.g. Tessil/robin-map) still declare
# `cmake_minimum_required(VERSION <3.5)`, which CMake ≥ 4.0 rejects outright.
# Raise the effective policy floor to 3.5 so those subprojects configure
# without requiring a manual -DCMAKE_POLICY_VERSION_MINIMUM=3.5 on the command
# line. Only affects subprojects that ask for less than 3.5; ours stays at 3.20.
if(NOT DEFINED CMAKE_POLICY_VERSION_MINIMUM)
  set(CMAKE_POLICY_VERSION_MINIMUM 3.5 CACHE STRING
      "Minimum CMake policy version for legacy vendored dependencies")
endif()

# ───────────────────────────── build external Seastar ─────────────────────────

# Seastar (post-25.05) enables LTTng-UST I/O tracepoints by default, which makes
# lttng-ust development headers a hard build requirement. TimeStar does not use
# them, so default the option OFF here (before Seastar declares it) to keep the
# dependency set unchanged. Opt back in with -DSeastar_LTTNG=ON.
set(Seastar_LTTNG OFF CACHE BOOL "Enable LTTng-UST tracepoint support for IO tracing.")

add_subdirectory(external/seastar)

# Consume Seastar's headers as SYSTEM includes so third-party diagnostics
# (-Wshadow/-Wpedantic/-Wnull-dereference hits inside seastar/core/*.hh) do not
# leak into first-party builds under our warning set. FetchContent already does
# this for the fetched deps (CMake ≥ 3.25 marks them SYSTEM by default); this
# mirrors that for the vendored Seastar submodule without requiring the
# CMake 3.25 `add_subdirectory(... SYSTEM)` keyword.
foreach(_seastar_tgt seastar seastar_testing seastar_perf_testing)
  if(TARGET ${_seastar_tgt})
    get_target_property(_seastar_inc ${_seastar_tgt} INTERFACE_INCLUDE_DIRECTORIES)
    if(_seastar_inc)
      set_target_properties(${_seastar_tgt} PROPERTIES
        INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${_seastar_inc}")
    endif()
  endif()
endforeach()

# ───────────────────────────── project’s own targets ─────────────────────────

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ───────────────────────────── fetch external dependencies ─────────────────────
include(FetchContent)

FetchContent_Declare(
  glaze
  GIT_REPOSITORY https://github.com/stephenberry/glaze.git
  GIT_TAG v9.1.0
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.10.0
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  robin-map
  GIT_REPOSITORY https://github.com/Tessil/robin-map.git
  GIT_TAG v1.4.1
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  highway
  GIT_REPOSITORY https://github.com/google/highway.git
  GIT_TAG 1.4.0
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  roaring
  GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
  GIT_TAG v5.5.5
  GIT_SHALLOW TRUE
)

FetchContent_Declare(
  zstd
  GIT_REPOSITORY https://github.com/facebook/zstd.git
  GIT_TAG v1.5.7
  GIT_SHALLOW TRUE
  SOURCE_SUBDIR build/cmake
)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
# Keep `make install` free of embedded-dependency artifacts where the deps
# provide a toggle (gtest is handled next to its FetchContent in test/).
# glaze/roaring/zstd/robin-map offer no such toggle; use
# `cmake --install build --component timestar` for a TimeStar-only install.
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(ROARING_BUILD_LTO OFF CACHE BOOL "" FORCE)
set(ENABLE_ROARING_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(glaze googlebenchmark robin-map highway roaring zstd)

# Consume the fetched libraries' headers as SYSTEM includes, exactly as done for
# Seastar above, so diagnostics inside third-party headers do not count against
# first-party code under -Werror (Highway >= 1.3 uses `unsigned __int128`, which
# -Wpedantic rejects in every SIMD translation unit that includes it). Done via
# the target property rather than FetchContent's SYSTEM keyword, which needs
# CMake 3.25.
foreach(_dep_tgt hwy roaring roaring-headers roaring-headers-cpp libzstd_static benchmark tsl::robin_map)
  if(TARGET ${_dep_tgt})
    get_target_property(_dep_aliased ${_dep_tgt} ALIASED_TARGET)
    if(_dep_aliased)
      set(_dep_real ${_dep_aliased})
    else()
      set(_dep_real ${_dep_tgt})
    endif()
    get_target_property(_dep_inc ${_dep_real} INTERFACE_INCLUDE_DIRECTORIES)
    if(_dep_inc)
      set_target_properties(${_dep_real} PROPERTIES
        INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${_dep_inc}")
    endif()
  endif()
endforeach()

# ──────────────────────────── protobuf message library ────────────────────────
# Must come after Seastar (which finds Protobuf) so the imported targets exist.

add_subdirectory(proto)

# ─────────────────────────────── sub‑directories ─────────────────────────────

# GNUInstallDirs defines CMAKE_INSTALL_BINDIR / CMAKE_INSTALL_DATADIR used by
# the install() rules in bin/ and at the bottom of this file.
include(GNUInstallDirs)

add_subdirectory(lib)
add_subdirectory(bin)

option(TIMESTAR_BUILD_TESTS "Build test executables" ON)
if(TIMESTAR_BUILD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

# ─────────────────────────── coverage instrumentation ────────────────────────
# Adds --coverage -O0 -g to first-party targets only (lib/ bin/ proto/ test/);
# Seastar and the FetchContent _deps keep their normal flags.  Use via the
# `coverage` preset (CMAKE_BUILD_TYPE=Debug, Seastar_SANITIZE=OFF) — see
# cmake/Coverage.cmake for why Coverage is not a build type here.  test/ is
# instrumented too so lib/ header (template/inline) code that is only
# instantiated in test translation units still shows up in the report.
option(TIMESTAR_COVERAGE "Instrument first-party targets for gcov coverage" OFF)
if(TIMESTAR_COVERAGE)
  include(cmake/Coverage.cmake)
  set(_timestar_cov_dirs
      ${CMAKE_CURRENT_SOURCE_DIR}/lib
      ${CMAKE_CURRENT_SOURCE_DIR}/bin
      ${CMAKE_CURRENT_SOURCE_DIR}/proto)
  if(TIMESTAR_BUILD_TESTS)
    list(APPEND _timestar_cov_dirs ${CMAKE_CURRENT_SOURCE_DIR}/test)
  endif()
  timestar_apply_coverage(${_timestar_cov_dirs})
  unset(_timestar_cov_dirs)
endif()

# ────────────────────────────────── install ──────────────────────────────────
# Minimal install: the server binary (installed from bin/CMakeLists.txt) plus a
# sample configuration file. Honors CMAKE_INSTALL_PREFIX via GNUInstallDirs
# (included above, before the bin/ subdirectory).
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/timestar.toml
        DESTINATION ${CMAKE_INSTALL_DATADIR}/timestar
        RENAME timestar.toml.sample
        COMPONENT timestar)

# End of CMakeLists.txt
