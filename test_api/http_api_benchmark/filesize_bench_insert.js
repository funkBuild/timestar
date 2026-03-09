#!/usr/bin/env node
/**
 * Insert-only phase of the file size benchmark.
 * Called by filesize_bench.sh — do not run standalone.
 *
 * Usage: node filesize_bench_insert.js [label]
 */

const axios = require('axios');
const { performance } = require('perf_hooks');

const BASE_URL = process.env.TIMESTAR_URL || 'http://localhost:8086';
const LABEL = process.argv[2] || 'UNKNOWN';

const MEASUREMENT = 'sizing.sensor';
const NUM_HOSTS = 500;
const FIELDS = ['temperature', 'humidity', 'pressure', 'cpu_pct', 'mem_pct'];
const MINUTE_NS = 60_000_000_000n;

const MINUTES_TOTAL = 6 * 30 * 24 * 60; // 259,200
const BATCH_SIZE = 10000;
const CONCURRENCY = 8;

// Seeded PRNG for deterministic data across runs
function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function generateArrayWrite(batchSize, hostId, startTs, rng) {
  const timestamps = new Array(batchSize);
  const fields = {};
  for (const f of FIELDS) fields[f] = new Array(batchSize);

  for (let i = 0; i < batchSize; i++) {
    timestamps[i] = Number(startTs + BigInt(i) * MINUTE_NS);
    fields['temperature'][i] = +(20 + Math.sin(rng() * 6.28) * 5).toFixed(1);
    fields['humidity'][i]    = +(40 + rng() * 30).toFixed(1);
    fields['pressure'][i]    = +(1013 + rng() * 10).toFixed(2);
    fields['cpu_pct'][i]     = +(rng() * 100).toFixed(1);
    fields['mem_pct'][i]     = +(30 + rng() * 50).toFixed(1);
  }

  return {
    measurement: MEASUREMENT,
    tags: { host: `host-${String(hostId).padStart(3, '0')}`, rack: `rack-${((hostId - 1) % 3) + 1}` },
    timestamps,
    fields,
  };
}

async function main() {
  const totalDataPoints = NUM_HOSTS * MINUTES_TOTAL * FIELDS.length;
  console.log(`[${LABEL}] Inserting ${totalDataPoints.toLocaleString()} data points...`);
  console.log(`  ${NUM_HOSTS} hosts × ${MINUTES_TOTAL.toLocaleString()} min × ${FIELDS.length} fields`);

  // Health check
  try {
    await axios.get(`${BASE_URL}/health`);
  } catch (e) {
    console.error('Server not reachable:', e.message);
    process.exit(1);
  }

  const startNs = BigInt(Date.now()) * 1_000_000n - BigInt(MINUTES_TOTAL) * MINUTE_NS;
  const overallStart = performance.now();
  let idx = 0;
  let totalInserted = 0;
  let lastReport = overallStart;

  // Build work queue
  const workQueue = [];
  for (let hostId = 1; hostId <= NUM_HOSTS; hostId++) {
    for (let offset = 0; offset < MINUTES_TOTAL; offset += BATCH_SIZE) {
      const count = Math.min(BATCH_SIZE, MINUTES_TOTAL - offset);
      workQueue.push({ hostId, offset, count });
    }
  }

  async function worker() {
    const rng = mulberry32(12345 + idx);
    while (idx < workQueue.length) {
      const i = idx++;
      const { hostId, offset, count } = workQueue[i];
      const ts = startNs + BigInt(offset) * MINUTE_NS;
      const payload = generateArrayWrite(count, hostId, ts, rng);
      await axios.post(`${BASE_URL}/write`, payload);
      totalInserted += count * FIELDS.length;

      const now = performance.now();
      if (now - lastReport > 5000) {
        const pct = ((totalInserted / totalDataPoints) * 100).toFixed(1);
        const pps = Math.round((totalInserted / (now - overallStart)) * 1000);
        process.stdout.write(`\r  ${totalInserted.toLocaleString()}/${totalDataPoints.toLocaleString()} (${pct}%) ${pps.toLocaleString()} pts/sec  `);
        lastReport = now;
      }
    }
  }

  const workers = [];
  for (let c = 0; c < CONCURRENCY; c++) workers.push(worker());
  await Promise.all(workers);

  const elapsed = performance.now() - overallStart;
  const pps = Math.round((totalDataPoints / elapsed) * 1000);
  console.log(`\n  Done: ${totalDataPoints.toLocaleString()} points in ${(elapsed / 1000).toFixed(1)}s (${pps.toLocaleString()} pts/sec)`);
}

main().catch(e => { console.error(e.message || e); process.exit(1); });
