# ──────────────────────────────────────────────────────────────────────────────
#  Coverage.cmake — per-target gcov instrumentation for first-party code
#  ---------------------------------------------------------------------------
#  Why per-target flags instead of a custom CMAKE_BUILD_TYPE=Coverage:
#  Seastar's CMakeLists force-caches CMAKE_BUILD_TYPE with its own mode list
#  ("None Debug Release RelWithDebInfo MinSizeRel Dev Sanitize") and keys its
#  tri-state defaults (Seastar_SANITIZE, stack guards, debug shared_ptr) and
#  per-config flag variables off those known names.  An unknown "Coverage"
#  config would build Seastar with empty per-config flags and unmatched
#  tri-state conditions.  Instead, the `coverage` preset uses
#  CMAKE_BUILD_TYPE=Debug (no -DNDEBUG, -g, no optimization) with
#  Seastar_SANITIZE=OFF — mirroring the `debug` preset — and this module
#  applies `--coverage -O0` to first-party targets only, leaving Seastar and
#  the FetchContent dependencies (_deps) uninstrumented.
#
#  Usage (top-level CMakeLists.txt, after all add_subdirectory() calls):
#      include(cmake/Coverage.cmake)
#      timestar_apply_coverage(<dir> [<dir> ...])
# ──────────────────────────────────────────────────────────────────────────────

# Recursively collect every target defined in `dir` and its subdirectories.
function(_timestar_collect_buildsystem_targets dir out_var)
  get_property(_tgts DIRECTORY "${dir}" PROPERTY BUILDSYSTEM_TARGETS)
  get_property(_subdirs DIRECTORY "${dir}" PROPERTY SUBDIRECTORIES)
  foreach(_sub IN LISTS _subdirs)
    _timestar_collect_buildsystem_targets("${_sub}" _sub_tgts)
    list(APPEND _tgts ${_sub_tgts})
  endforeach()
  set(${out_var} ${_tgts} PARENT_SCOPE)
endfunction()

# Apply gcov instrumentation to every first-party compiled target defined in
# the given directories.  "First-party" = the target's SOURCE_DIR is inside
# the source tree but not under external/ (the Seastar submodule) and not
# under the binary dir (FetchContent _deps sources live there).
function(timestar_apply_coverage)
  if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    message(WARNING
      "TIMESTAR_COVERAGE is designed for GCC/gcov; compiler is "
      "${CMAKE_CXX_COMPILER_ID}. --coverage will still be applied but the "
      "report tooling (scripts/run_coverage.sh) expects gcov output.")
  endif()

  set(_count 0)
  foreach(_dir IN LISTS ARGN)
    _timestar_collect_buildsystem_targets("${_dir}" _tgts)
    foreach(_tgt IN LISTS _tgts)
      get_target_property(_type ${_tgt} TYPE)
      if(NOT _type MATCHES "^(EXECUTABLE|STATIC_LIBRARY|SHARED_LIBRARY|MODULE_LIBRARY|OBJECT_LIBRARY)$")
        continue()
      endif()

      get_target_property(_srcdir ${_tgt} SOURCE_DIR)
      string(FIND "${_srcdir}" "${CMAKE_SOURCE_DIR}/" _in_src)
      string(FIND "${_srcdir}" "${CMAKE_SOURCE_DIR}/external" _in_ext)
      string(FIND "${_srcdir}" "${CMAKE_BINARY_DIR}" _in_bin)
      if(NOT _in_src EQUAL 0 OR _in_ext EQUAL 0 OR _in_bin EQUAL 0)
        continue()
      endif()

      # -O0 wins over any earlier optimization flag (last -O on the command
      # line takes effect).  -fprofile-update=atomic: Seastar runs one reactor
      # thread per shard plus syscall/IO helper threads, and non-atomic
      # counter updates can corrupt .gcda data.  Restrict to C/C++ so the
      # flags never reach ASM sources.
      target_compile_options(${_tgt} PRIVATE
        $<$<COMPILE_LANGUAGE:C,CXX>:--coverage;-O0;-g;-fprofile-update=atomic>)
      if(NOT _type STREQUAL "OBJECT_LIBRARY")
        target_link_options(${_tgt} PRIVATE --coverage)
      endif()
      math(EXPR _count "${_count} + 1")

      # Google Highway TUs cannot compile at -O0 with GCC: hwy wrappers are
      # plain `inline` (not always_inline), so without inlining the AVX-512
      # intrinsic immediates are no longer compile-time constants and GCC
      # errors with "the last argument must be an 8-bit immediate".  Bump
      # those TUs to -O2 — source-file COMPILE_OPTIONS are appended after the
      # target-level options, so this -O2 overrides the -O0 above.  They stay
      # instrumented (--coverage still applies); line attribution is merely
      # less exact in these ~10 SIMD kernel files.
      get_target_property(_srcs ${_tgt} SOURCES)
      foreach(_src IN LISTS _srcs)
        if(_src MATCHES "^\\$<" OR NOT _src MATCHES "\\.(cpp|cc|cxx)$")
          continue()
        endif()
        set(_abs_src "${_src}")
        if(NOT IS_ABSOLUTE "${_abs_src}")
          set(_abs_src "${_srcdir}/${_src}")
        endif()
        if(NOT EXISTS "${_abs_src}")
          continue()
        endif()
        file(STRINGS "${_abs_src}" _hwy_hits REGEX "(hwy/|HWY_)" LIMIT_COUNT 1)
        if(_hwy_hits)
          set_source_files_properties("${_abs_src}"
            TARGET_DIRECTORY ${_tgt}
            PROPERTIES COMPILE_OPTIONS "-O2")
          message(STATUS "Coverage: ${_abs_src} kept at -O2 (Highway TU)")
        endif()
      endforeach()
    endforeach()
  endforeach()
  message(STATUS "Coverage: instrumented ${_count} first-party targets (--coverage -O0 -g)")
endfunction()
