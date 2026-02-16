#!/usr/bin/env node
/**
 * ALP vs Gorilla Insert & Query Performance Benchmark
 *
 * Run against the TSDB HTTP server to measure end-to-end insert and query
 * performance. Execute once with ALP build and once with Gorilla build,
 * then compare results.
 *
 * Usage:
 *   node alp_vs_gorilla_bench.js [label]
 *
 * Example:
 *   node alp_vs_gorilla_bench.js ALP
 *   node alp_vs_gorilla_bench.js GORILLA
 */

const axios = require('axios');
const { performance } = require('perf_hooks');

const BASE_URL = process.env.TSDB_URL || 'http://localhost:8086';
const LABEL = process.argv[2] || 'UNKNOWN';

// ── Configuration ──────────────────────────────────────────────────
const MEASUREMENT = 'bench.sensor';
const NUM_HOSTS = 5;
const FIELDS = ['temperature', 'humidity', 'pressure', 'cpu_pct', 'mem_pct'];
const MINUTE_NS = 60_000_000_000n;

// Insert config
const INSERT_BATCH_SIZE   = 5000;   // timestamps per request
const INSERT_BATCHES      = 20;     // total requests (= 100K timestamps × 5 fields = 500K points)
const INSERT_CONCURRENCY  = 2;

// Query config
const QUERY_ITERATIONS = 20;

// ── Helpers ────────────────────────────────────────────────────────

function generateFieldValue(field) {
  switch (field) {
    case 'temperature': return +(20 + Math.sin(Math.random() * 6.28) * 5).toFixed(1);
    case 'humidity':    return +(40 + Math.random() * 30).toFixed(1);
    case 'pressure':    return +(1013 + Math.random() * 10).toFixed(2);
    case 'cpu_pct':     return +(Math.random() * 100).toFixed(1);
    case 'mem_pct':     return +(30 + Math.random() * 50).toFixed(1);
    default:            return Math.random() * 100;
  }
}

function generateArrayWrite(batchSize, hostId, startTs) {
  const timestamps = new Array(batchSize);
  const fields = {};
  for (const f of FIELDS) fields[f] = new Array(batchSize);

  for (let i = 0; i < batchSize; i++) {
    timestamps[i] = Number(startTs + BigInt(i) * MINUTE_NS);
    for (const f of FIELDS) {
      fields[f][i] = generateFieldValue(f);
    }
  }

  return {
    measurement: MEASUREMENT,
    tags: { host: `host-${String(hostId).padStart(2, '0')}` },
    timestamps,
    fields,
  };
}

function stats(times) {
  const sorted = [...times].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  const n = sorted.length;
  return {
    min:    sorted[0],
    max:    sorted[n - 1],
    avg:    sum / n,
    median: n % 2 === 0 ? (sorted[n/2 - 1] + sorted[n/2]) / 2 : sorted[Math.floor(n/2)],
    p95:    sorted[Math.min(Math.floor(n * 0.95), n - 1)],
  };
}

function fmt(ms) { return ms.toFixed(2); }

// ── Insert Benchmark ───────────────────────────────────────────────

async function benchmarkInserts() {
  const yearAgo = BigInt(Date.now()) * 1_000_000n - 525_600n * MINUTE_NS;
  const batchLatencies = [];
  let totalTimestamps = 0;

  // Build all payloads up-front so generation time doesn't count
  const payloads = [];
  for (let b = 0; b < INSERT_BATCHES; b++) {
    const hostId = (b % NUM_HOSTS) + 1;
    const startTs = yearAgo + BigInt(b * INSERT_BATCH_SIZE) * MINUTE_NS;
    payloads.push(generateArrayWrite(INSERT_BATCH_SIZE, hostId, startTs));
  }

  const overallStart = performance.now();
  let idx = 0;

  async function worker() {
    while (idx < payloads.length) {
      const i = idx++;
      const start = performance.now();
      await axios.post(`${BASE_URL}/write`, payloads[i]);
      const elapsed = performance.now() - start;
      batchLatencies.push(elapsed);
      totalTimestamps += INSERT_BATCH_SIZE;
    }
  }

  const workers = [];
  for (let c = 0; c < INSERT_CONCURRENCY; c++) workers.push(worker());
  await Promise.all(workers);

  const overallMs = performance.now() - overallStart;
  const totalDataPoints = totalTimestamps * FIELDS.length;
  const s = stats(batchLatencies);

  return {
    totalTimestamps,
    totalDataPoints,
    overallMs,
    throughputPps: (totalDataPoints / overallMs) * 1000,
    batchLatency: s,
  };
}

// ── Query Benchmark ────────────────────────────────────────────────

async function benchmarkQueries() {
  const now = BigInt(Date.now()) * 1_000_000n;
  const yearAgo = now - 525_600n * MINUTE_NS;
  const results = {};

  // 1) Single-host, all fields, no aggregation interval (raw decode)
  {
    const times = [];
    let totalPoints = 0;
    for (let i = 0; i < QUERY_ITERATIONS; i++) {
      const h = (i % NUM_HOSTS) + 1;
      const q = {
        query: `avg:${MEASUREMENT}(temperature,humidity){host:host-${String(h).padStart(2,'0')}}`,
        startTime: Number(yearAgo),
        endTime:   Number(now),
        aggregationInterval: '1h',
      };
      const start = performance.now();
      const resp = await axios.post(`${BASE_URL}/query`, q);
      times.push(performance.now() - start);
      totalPoints += resp.data.statistics?.point_count || 0;
    }
    results.singleHost1h = { ...stats(times), totalPoints };
  }

  // 2) All hosts, group by host, 12h buckets
  {
    const times = [];
    let totalPoints = 0;
    for (let i = 0; i < QUERY_ITERATIONS; i++) {
      const q = {
        query: `avg:${MEASUREMENT}(temperature,cpu_pct,mem_pct){} by {host}`,
        startTime: Number(yearAgo),
        endTime:   Number(now),
        aggregationInterval: '12h',
      };
      const start = performance.now();
      const resp = await axios.post(`${BASE_URL}/query`, q);
      times.push(performance.now() - start);
      totalPoints += resp.data.statistics?.point_count || 0;
    }
    results.groupByHost12h = { ...stats(times), totalPoints };
  }

  // 3) Latest value query (narrow time range)
  {
    const times = [];
    let totalPoints = 0;
    const recentStart = now - 60n * MINUTE_NS; // last hour
    for (let i = 0; i < QUERY_ITERATIONS; i++) {
      const h = (i % NUM_HOSTS) + 1;
      const q = {
        query: `latest:${MEASUREMENT}(temperature,humidity,pressure,cpu_pct,mem_pct){host:host-${String(h).padStart(2,'0')}}`,
        startTime: Number(recentStart),
        endTime:   Number(now),
      };
      const start = performance.now();
      const resp = await axios.post(`${BASE_URL}/query`, q);
      times.push(performance.now() - start);
      totalPoints += resp.data.statistics?.point_count || 0;
    }
    results.latestNarrow = { ...stats(times), totalPoints };
  }

  // 4) Wide scan - sum across all data, 1d buckets
  {
    const times = [];
    let totalPoints = 0;
    for (let i = 0; i < QUERY_ITERATIONS; i++) {
      const q = {
        query: `sum:${MEASUREMENT}(temperature,cpu_pct){}`,
        startTime: Number(yearAgo),
        endTime:   Number(now),
        aggregationInterval: '1d',
      };
      const start = performance.now();
      const resp = await axios.post(`${BASE_URL}/query`, q);
      times.push(performance.now() - start);
      totalPoints += resp.data.statistics?.point_count || 0;
    }
    results.wideScan1d = { ...stats(times), totalPoints };
  }

  return results;
}

// ── Main ───────────────────────────────────────────────────────────

async function main() {
  console.log('='.repeat(70));
  console.log(` TSDB Insert & Query Benchmark  [${LABEL}]`);
  console.log('='.repeat(70));
  console.log(`Server:       ${BASE_URL}`);
  console.log(`Measurement:  ${MEASUREMENT}`);
  console.log(`Hosts:        ${NUM_HOSTS}`);
  console.log(`Fields:       ${FIELDS.length} (${FIELDS.join(', ')})`);
  console.log(`Insert:       ${INSERT_BATCHES} batches × ${INSERT_BATCH_SIZE} ts × ${FIELDS.length} fields = ${INSERT_BATCHES * INSERT_BATCH_SIZE * FIELDS.length} points`);
  console.log(`Queries:      ${QUERY_ITERATIONS} iterations each`);
  console.log('='.repeat(70));

  // Health check
  try {
    await axios.get(`${BASE_URL}/health`);
    console.log('Server healthy.\n');
  } catch (e) {
    console.error('Server not reachable:', e.message);
    process.exit(1);
  }

  // ── INSERT ──
  console.log('--- INSERT BENCHMARK ---');
  const ins = await benchmarkInserts();
  console.log(`  Total:        ${ins.totalDataPoints.toLocaleString()} data points in ${fmt(ins.overallMs)} ms`);
  console.log(`  Throughput:   ${Math.round(ins.throughputPps).toLocaleString()} points/sec`);
  console.log(`  Batch latency (ms): avg=${fmt(ins.batchLatency.avg)}  med=${fmt(ins.batchLatency.median)}  p95=${fmt(ins.batchLatency.p95)}  min=${fmt(ins.batchLatency.min)}  max=${fmt(ins.batchLatency.max)}`);

  // Short pause for writes to settle
  await new Promise(r => setTimeout(r, 500));

  // ── QUERY ──
  console.log('\n--- QUERY BENCHMARK ---');
  const q = await benchmarkQueries();

  const printQ = (label, s) => {
    console.log(`  ${label.padEnd(25)} avg=${fmt(s.avg)}ms  med=${fmt(s.median)}ms  p95=${fmt(s.p95)}ms  min=${fmt(s.min)}ms  max=${fmt(s.max)}ms  pts=${s.totalPoints}`);
  };

  printQ('Single host, 1h buckets', q.singleHost1h);
  printQ('Group by host, 12h',      q.groupByHost12h);
  printQ('Latest (narrow range)',    q.latestNarrow);
  printQ('Wide scan, 1d buckets',   q.wideScan1d);

  // ── SUMMARY ──
  console.log('\n' + '='.repeat(70));
  console.log(` SUMMARY [${LABEL}]`);
  console.log('='.repeat(70));
  console.log(`  Insert throughput:   ${Math.round(ins.throughputPps).toLocaleString()} pts/sec`);
  console.log(`  Insert batch avg:    ${fmt(ins.batchLatency.avg)} ms`);
  console.log(`  Query single host:   ${fmt(q.singleHost1h.avg)} ms`);
  console.log(`  Query group-by-host: ${fmt(q.groupByHost12h.avg)} ms`);
  console.log(`  Query latest:        ${fmt(q.latestNarrow.avg)} ms`);
  console.log(`  Query wide scan:     ${fmt(q.wideScan1d.avg)} ms`);
  console.log('='.repeat(70));
}

main().catch(e => { console.error(e); process.exit(1); });
