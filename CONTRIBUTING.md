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

The project uses `clang-format-22.1.1`. Format your code before committing:

```bash
find lib bin -name '*.cpp' -o -name '*.hpp' | xargs clang-format-22.1.1 -i
```

CI will reject PRs with formatting violations.

## Pull Request Process

1. Create a feature branch from `main`
2. Make your changes with tests
3. Ensure all tests pass: `./test/timestar_unit_test`
4. Ensure formatting passes: `clang-format-22.1.1 --dry-run --Werror <files>`
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

## Seastar Notes

- All I/O returns `seastar::future<>` and uses `co_await`
- Shard-per-core model: avoid cross-shard data access
- Each shard has its own per-shard NativeIndex (custom LSM-tree) co-located with data
- Data is sharded by series key hash; metadata lives on the same shard as its data
