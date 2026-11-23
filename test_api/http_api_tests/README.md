# HTTP API Test Suite

JavaScript integration tests for the TimeStar HTTP API. They run against a live server.

## Running

```bash
# 1. Start a server (from the build directory)
./bin/timestar_http_server --port 8086

# 2. From test_api/
npm install                 # first time only
npm test                    # Jest: 198 tests in 14 suites
npm run test:standalone     # 8 standalone scripts
npm run test:all            # both
```

Every test honours `TIMESTAR_HOST` and `TIMESTAR_PORT` (default `localhost:8086`):

```bash
TIMESTAR_PORT=18290 npm run test:all
```

`./run_tests.sh` (this directory) starts a server on `TIMESTAR_PORT` if none is listening there, runs
the Jest suites, and stops the server it started.

## What is here

- **`*.test.js`** — the Jest suites: queries (`comprehensive_query`), metadata, cardinality, delete and
  reinsert, last-write-wins overwrites, explicit field types, derived metrics and multi-series fan-out,
  transform functions, anomaly detection, forecasting, retention, subscriptions (SSE) and the protobuf API.
- **`test_*.js`** — standalone scripts run by `npm run test:standalone` (`../run-standalone-tests.js`):
  writes, strings, booleans, parser errors, error bodies, accumulation. A script passes when it exits 0.
- **`debug_*.js`** and `test_string_debug.js` — one-off investigation scripts. Not part of any suite.
- The `*.md` files other than this one are historical implementation notes.

## Notes

- The tests write to the server they are pointed at; use a scratch data directory.
- A separate, larger suite lives in the `timestar-nodejs` client repository (Vitest). Its correctness
  tests need a server started with `TIMESTAR_WAL_SIZE_THRESHOLD=2097152` and `TIMESTAR_DATA_DIR` set to
  that server's data directory, because they force WAL rollovers.
- `../equivalence_test.py` compares TimeStar with a running InfluxDB 2.7 and needs both.
