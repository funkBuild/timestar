#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
#  TimeStar coverage runner
#  ---------------------------------------------------------------------------
#  Configures + builds the `coverage` CMake preset (build-coverage/), runs the
#  unit test suite, and produces a gcovr report filtered to lib/:
#      build-coverage/coverage-html/index.html   (HTML details)
#      build-coverage/coverage.xml               (Cobertura, for CI)
#      console per-file table + line/branch summary
#
#  Idempotent: stale .gcda counters are wiped before the test run; re-running
#  reconfigures/rebuilds incrementally.
#
#  Usage (from anywhere; paths are resolved from the script location):
#      ./scripts/run_coverage.sh
#
#  Environment knobs:
#      COVERAGE_FAIL_UNDER_LINE   fail if lib/ line coverage < N%   (default 0 = off)
#      COVERAGE_GTEST_FILTER      --gtest_filter value for the test run
#      COVERAGE_CMAKE_ARGS        extra args appended to the configure step
#                                 (e.g. "-G Ninja -DCMAKE_CXX_COMPILER_LAUNCHER=ccache")
#      COVERAGE_SKIP_TESTS=1      only regenerate the report from existing .gcda
#      COVERAGE_JOBS              build parallelism (default: nproc)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build-coverage"
HTML_DIR="${BUILD_DIR}/coverage-html"
JOBS="${COVERAGE_JOBS:-$(nproc)}"
FAIL_UNDER_LINE="${COVERAGE_FAIL_UNDER_LINE:-0}"

cd "${PROJECT_ROOT}"

# ─────────────────────────── 1. configure + build ─────────────────────────────
echo "==> Configuring coverage preset (${BUILD_DIR})"
# shellcheck disable=SC2086  # COVERAGE_CMAKE_ARGS is intentionally word-split
cmake --preset coverage ${COVERAGE_CMAKE_ARGS:-}

echo "==> Building timestar_unit_test (-j${JOBS})"
cmake --build --preset coverage -j "${JOBS}" --target timestar_unit_test

# ─────────────────────────────── 2. run tests ─────────────────────────────────
if [[ "${COVERAGE_SKIP_TESTS:-0}" != "1" ]]; then
  echo "==> Clearing stale .gcda counters"
  find "${BUILD_DIR}" -name '*.gcda' -delete

  echo "==> Running unit tests (this is an -O0 instrumented build; expect it to be slow)"
  GTEST_ARGS=()
  if [[ -n "${COVERAGE_GTEST_FILTER:-}" ]]; then
    GTEST_ARGS+=("--gtest_filter=${COVERAGE_GTEST_FILTER}")
  fi
  # Run from the build root: tests create shard_N/ data dirs relative to CWD,
  # and each gtest fixture spawns its own seastar app_template internally (no
  # seastar CLI args like --smp are accepted by the gtest main).
  (cd "${BUILD_DIR}" && ./test/timestar_unit_test "${GTEST_ARGS[@]}")
fi

# ──────────────────────────── 3. locate gcovr ─────────────────────────────────
# Preference order: gcovr on PATH → python3 -m gcovr → private venv in the
# build dir (pip install) → pip install --user → lcov/genhtml fallback.
GCOVR=()
find_gcovr() {
  if command -v gcovr >/dev/null 2>&1; then
    GCOVR=(gcovr); return 0
  fi
  if python3 -m gcovr --version >/dev/null 2>&1; then
    GCOVR=(python3 -m gcovr); return 0
  fi
  local venv="${BUILD_DIR}/gcovr-venv"
  if [[ ! -x "${venv}/bin/gcovr" ]]; then
    echo "==> gcovr not found; installing into ${venv}"
    if python3 -m venv "${venv}" 2>/dev/null \
        && "${venv}/bin/pip" install --quiet gcovr 2>/dev/null; then
      :
    else
      echo "==> venv install failed; trying pip install --user gcovr"
      python3 -m pip install --user --quiet gcovr 2>/dev/null || true
      hash -r
      if command -v gcovr >/dev/null 2>&1; then GCOVR=(gcovr); return 0; fi
      if [[ -x "${HOME}/.local/bin/gcovr" ]]; then GCOVR=("${HOME}/.local/bin/gcovr"); return 0; fi
      if python3 -m gcovr --version >/dev/null 2>&1; then GCOVR=(python3 -m gcovr); return 0; fi
      return 1
    fi
  fi
  GCOVR=("${venv}/bin/gcovr"); return 0
}

# Pick the gcov that matches the compiler that produced the .gcno files
# (e.g. g++-14 needs gcov-14; the system default gcov may be a newer major).
detect_gcov() {
  local cxx major
  cxx="$(sed -n 's/^CMAKE_CXX_COMPILER:[^=]*=//p' "${BUILD_DIR}/CMakeCache.txt" | head -1)"
  if [[ -n "${cxx}" && -x "${cxx}" ]]; then
    major="$("${cxx}" -dumpversion 2>/dev/null | cut -d. -f1)"
    if [[ -n "${major}" ]] && command -v "gcov-${major}" >/dev/null 2>&1; then
      echo "gcov-${major}"; return
    fi
  fi
  echo "gcov"
}
GCOV_TOOL="$(detect_gcov)"
echo "==> Using gcov executable: ${GCOV_TOOL}"

# ─────────────────────────── 4. generate reports ──────────────────────────────
mkdir -p "${HTML_DIR}"

if find_gcovr; then
  echo "==> Generating gcovr report (filtered to lib/)"
  GCOVR_ARGS=(
    --root "${PROJECT_ROOT}"
    --filter 'lib/'
    --exclude-throw-branches
    --exclude-unreachable-branches
    --gcov-executable "${GCOV_TOOL}"
    --gcov-ignore-parse-errors=negative_hits.warn_once_per_file
    --print-summary
    --sort-percentage
    --html-details "${HTML_DIR}/index.html"
    --cobertura "${BUILD_DIR}/coverage.xml"
    --json "${BUILD_DIR}/coverage.json"
    -j "${JOBS}"
  )
  if [[ "${FAIL_UNDER_LINE}" != "0" ]]; then
    GCOVR_ARGS+=(--fail-under-line "${FAIL_UNDER_LINE}")
  fi
  "${GCOVR[@]}" "${GCOVR_ARGS[@]}" "${BUILD_DIR}"
  echo ""
  echo "HTML report:      ${HTML_DIR}/index.html"
  echo "Cobertura XML:    ${BUILD_DIR}/coverage.xml"
  echo "JSON (machine):   ${BUILD_DIR}/coverage.json"
elif command -v lcov >/dev/null 2>&1; then
  echo "==> gcovr unavailable; falling back to lcov/genhtml"
  lcov --capture --directory "${BUILD_DIR}" \
       --gcov-tool "${GCOV_TOOL}" \
       --output-file "${BUILD_DIR}/coverage.info" \
       --ignore-errors mismatch,negative 2>/dev/null \
    || lcov --capture --directory "${BUILD_DIR}" \
            --gcov-tool "${GCOV_TOOL}" \
            --output-file "${BUILD_DIR}/coverage.info"
  lcov --extract "${BUILD_DIR}/coverage.info" "${PROJECT_ROOT}/lib/*" \
       --output-file "${BUILD_DIR}/coverage.lib.info"
  genhtml "${BUILD_DIR}/coverage.lib.info" --output-directory "${HTML_DIR}"
  lcov --summary "${BUILD_DIR}/coverage.lib.info"
  echo "NOTE: lcov fallback produces no coverage.xml; install gcovr for the Cobertura report."
else
  echo "ERROR: neither gcovr nor lcov is available and gcovr could not be installed." >&2
  echo "       Install one of them (pip install gcovr | apt install gcovr lcov) and re-run." >&2
  exit 1
fi
