# Contributing to TimeStar

## Getting Started

1. Clone the repository with submodules:
   ```bash
   git clone --recursive https://github.com/yourusername/timestar.git
   cd timestar
   ```

2. Install dependencies (Ubuntu/Debian):
   ```bash
   sudo apt install cmake g++-14 libssl-dev \
     libboost-all-dev liblz4-dev libgnutls28-dev libsctp-dev libhwloc-dev \
     libnuma-dev libpciaccess-dev libcrypto++-dev libxml2-dev xfslibs-dev \
     systemtap-sdt-dev libyaml-cpp-dev libxxhash-dev ragel
   ```

3. Build:
   ```bash
   mkdir -p build && cd build
   cmake ..
   make -j$(nproc)
   ```

4. Run tests:
   ```bash
   ./test/timestar_unit_test
   ```

## Code Style

The project pins `clang-format` **21.1.6** (installed via `pip install clang-format==21.1.6` — the same pinned version runs in CI's Docker image, so results match exactly). Format your code before committing:

```bash
find lib bin -name '*.cpp' -o -name '*.hpp' | xargs clang-format -i
```

CI will reject PRs with formatting violations.

To catch violations before they reach CI, enable the repo's pre-push hook
(one-time setup; it runs the exact CI formatting gate on every push):

```bash
git config core.hooksPath scripts/git-hooks
```

Bypass in an emergency with `git push --no-verify` — CI still enforces the gate.

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with tests
3. Ensure all tests pass: `./test/timestar_unit_test`
4. Ensure formatting passes: `clang-format --dry-run --Werror <files>` (clang-format 21.1.6)
5. Open a PR against `main` with a clear description

## Project Structure

- `lib/` - Core library (storage, query, encoding, HTTP handlers)
- `bin/` - Executable sources (HTTP server, benchmarks)
- `test/` - Google Test suites
- `test_api/` - JavaScript API integration tests
- `docs/` - Documentation
- `frontend/` - React dashboard

## Testing

- Write Google Test cases for new functionality
- Place unit tests in `test/unit/` under the appropriate subdirectory
- All tests must pass before merge
- Target: no test regressions

## Coverage

Test coverage is measured with gcov/gcovr via the `coverage` CMake preset
(`build-coverage/`, gcc: `--coverage -O0 -g`, no `-DNDEBUG`; Seastar and
fetched third-party dependencies are not instrumented — see
`cmake/Coverage.cmake` for details, including why `Coverage` is not a
CMAKE_BUILD_TYPE here and why the ~10 Highway SIMD TUs stay at `-O2`).

```bash
# From the repo root: configure + build + run tests + generate reports
./scripts/run_coverage.sh
```

Reports land in:

- `build-coverage/coverage-html/index.html` — browsable per-line HTML
- `build-coverage/coverage.xml` — Cobertura XML (consumed by CI)
- console — per-file table + line/branch summary, filtered to `lib/`

Useful knobs (env vars): `COVERAGE_GTEST_FILTER` to narrow the test run,
`COVERAGE_SKIP_TESTS=1` to regenerate reports from existing counters,
`COVERAGE_FAIL_UNDER_LINE=<pct>` to enforce a threshold,
`COVERAGE_CMAKE_ARGS` for extra configure flags (CI passes ccache/Ninja here).

**Current baseline (Jul 2026):** `lib/` line coverage **55.8%**, branch
coverage **50.4%** (3175 unit tests). CI (`coverage` job in
`.github/workflows/ci.yml`) enforces a soft floor of **40% line coverage**
for `lib/`.

**Ratchet policy:** the 40% floor is a starting point, not the target — the
project goal is 100%. When you raise coverage meaningfully, bump
`COVERAGE_FAIL_UNDER_LINE` in the CI job to just below the new number.
Never lower the floor to get CI green.

## Running Benchmarks & Profiling

### Benchmark binaries

Built alongside the server (from the `build/` directory):

- `bin/timestar_benchmark` - General benchmark tool
- `bin/timestar_insert_bench` - Insert throughput benchmark
- `bin/timestar_query_bench` - Query throughput benchmark
- `bin/expression_benchmark` - Expression evaluation benchmark
- `bin/forecast_benchmark` - Forecasting benchmark

Canonical insert-bench invocation (start the server first with a fresh data directory):

```bash
./bin/timestar_insert_bench -c 4 --batches 1000 --batch-size 10000 --warmup 10 \
  --connections 8 --hosts 10 --racks 2
```

### Performance regression gates

Performance-sensitive changes should be checked against the perf gates in
`build/test/timestar_perf_test` (requires `-DTIMESTAR_BUILD_TESTS=ON`):

```bash
./test/timestar_perf_test --gtest_filter='OptimizationBaseline*'   # insert/query baselines
./test/timestar_perf_test --gtest_filter='QueryFoldBench*'         # aggregation fold kernels
./test/timestar_perf_test --gtest_filter='TsmReadPathBenchmark*'   # TSM read path / I/O amplification
```

### Measurement caveats

- For large query responses, `timestar_query_bench` wall-clock times are roughly 80%
  client-side JSON parsing. Use the `statistics.execution_time_ms` field from the query
  response to measure server-side compute.
- Seastar opens files with O_DIRECT, so the OS page cache never hides TSM read I/O in
  benchmarks - cold and warm runs measure real disk behavior.

### Static analysis

clang-tidy is configured for the project and is expected to be clean for new code.

## Releases

Releases are cut from a git tag; the tag is the single source of truth for the
version. **Do not hand-edit the `VERSION` file as part of a release** — the
release workflow stamps it for you.

To publish `1.2.3`:

```bash
git tag v1.2.3 && git push origin v1.2.3
# then publish a GitHub Release for that tag
```

Publishing the release triggers `.github/workflows/docker-publish.yml`, which:

1. Validates the tag is `MAJOR.MINOR.PATCH[-PRERELEASE]`, rejecting it otherwise.
2. Stamps the version and commit into the build via
   `-DTIMESTAR_VERSION_OVERRIDE` / `-DTIMESTAR_GIT_COMMIT_OVERRIDE`, so the
   compiled binary is versioned from the tag rather than from a checked-in file.
3. Builds the image locally and runs `timestar_http_server --version` inside it,
   **failing the deploy if the reported version does not match the tag.** Nothing
   is pushed to Docker Hub until this passes.
4. Pushes the image tags. Prereleases publish only their exact version — they do
   not move `latest`, `MAJOR`, or `MAJOR.MINOR`.
5. Commits `chore(release): X.Y.Z` back to the default branch so `VERSION` on
   main always reflects the last published release.

This exists because `VERSION` was previously bumped by hand: v1.2.2 shipped with
`1.2.1` compiled into the binary. Step 3 is the gate that makes that class of
mistake unshippable rather than merely discouraged.

`TIMESTAR_VERSION_OVERRIDE` is a release-automation hook. Local builds should
leave it unset and let cmake read `VERSION` and `git describe` as usual.

To re-publish or rehearse an existing tag, run the workflow manually
(`workflow_dispatch`) with that tag; tick **dry_run** to build and verify without
pushing or touching main.

## Seastar Notes

- All I/O returns `seastar::future<>` and uses `co_await`
- Shard-per-core model: avoid cross-shard data access
- Each shard has its own per-shard NativeIndex (custom LSM-tree) co-located with data
- Data is sharded by series key hash; metadata lives on the same shard as its data
