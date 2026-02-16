#!/usr/bin/env node
/**
 * Insert deterministic data, then report on-disk file sizes.
 * Run once with ALP build and once with Gorilla build.
 *
 * Usage: node filesize_bench.js [label]
 */

const axios = require('axios');
const { performance } = require('perf_hooks');
const { execSync } = require('child_process');

const BASE_URL = process.env.TSDB_URL || 'http://localhost:8086';
const LABEL = process.argv[2] || 'UNKNOWN';
const BUILD_DIR = process.argv[3] || '/home/matt/Desktop/source/tsdb/build';

const MEASUREMENT = 'sizing.sensor';
const NUM_HOSTS = 500;
const FIELDS = ['temperature', 'humidity', 'pressure', 'cpu_pct', 'mem_pct'];
const MINUTE_NS = 60_000_000_000n;

// 6 months of 1-minute data per host
// 200 hosts × 259,200 min × 5 fields = 259,200,000 data points
// ~500MB raw WAL per shard → many rollovers → TSM files created
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
    tags: { host: `host-${String(hostId).padStart(2, '0')}`, rack: `rack-${((hostId - 1) % 3) + 1}` },
    timestamps,
    fields,
  };
}

async function insertData() {
  const totalDataPoints = NUM_HOSTS * MINUTES_TOTAL * FIELDS.length;
  console.log(`Inserting ${totalDataPoints.toLocaleString()} data points (${NUM_HOSTS} hosts × ${MINUTES_TOTAL.toLocaleString()} min × ${FIELDS.length} fields)...`);

  const startNs = BigInt(Date.now()) * 1_000_000n - BigInt(MINUTES_TOTAL) * MINUTE_NS;
  const overallStart = performance.now();
  let idx = 0;
  let totalInserted = 0;
  let lastReport = overallStart;

  // Build work queue: { hostId, offset, count }
  const workQueue = [];
  for (let hostId = 1; hostId <= NUM_HOSTS; hostId++) {
    for (let offset = 0; offset < MINUTES_TOTAL; offset += BATCH_SIZE) {
      const count = Math.min(BATCH_SIZE, MINUTES_TOTAL - offset);
      workQueue.push({ hostId, offset, count });
    }
  }

  async function worker() {
    const rng = mulberry32(12345 + idx); // per-worker seed
    while (idx < workQueue.length) {
      const i = idx++;
      const { hostId, offset, count } = workQueue[i];
      const ts = startNs + BigInt(offset) * MINUTE_NS;
      const payload = generateArrayWrite(count, hostId, ts, rng);
      await axios.post(`${BASE_URL}/write`, payload);
      totalInserted += count * FIELDS.length;

      const now = performance.now();
      if (now - lastReport > 2000) {
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

  return totalDataPoints;
}

function measureDiskUsage() {
  // Gather per-type sizes across all shards
  const result = { wal: 0, tsm: 0, index: 0, total: 0, shardCount: 0 };

  try {
    const output = execSync(
      `find ${BUILD_DIR}/shard_* -type f -printf '%s %p\\n' 2>/dev/null`,
      { encoding: 'utf8', maxBuffer: 10 * 1024 * 1024 }
    );

    const shards = new Set();
    for (const line of output.trim().split('\n')) {
      if (!line) continue;
      const [sizeStr, path] = line.split(' ', 2);
      const size = parseInt(sizeStr, 10);
      result.total += size;

      // Extract shard
      const shardMatch = path.match(/shard_(\d+)/);
      if (shardMatch) shards.add(shardMatch[1]);

      if (path.endsWith('.wal'))          result.wal += size;
      else if (path.includes('/tsm/'))   result.tsm += size;
      else if (path.includes('/index/')) result.index += size;
    }
    result.shardCount = shards.size;
  } catch (e) {
    console.error('Failed to measure disk:', e.message);
  }

  return result;
}

function fmt(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

async function main() {
  console.log('='.repeat(60));
  console.log(` TSM File Size Benchmark  [${LABEL}]`);
  console.log('='.repeat(60));

  // Health check
  try {
    await axios.get(`${BASE_URL}/health`);
  } catch (e) {
    console.error('Server not reachable:', e.message);
    process.exit(1);
  }

  const totalPoints = await insertData();

  // Brief pause for any async flushes
  await new Promise(r => setTimeout(r, 2000));

  const disk = measureDiskUsage();
  const rawSize = totalPoints * 8; // 8 bytes per double

  console.log('');
  console.log(`--- ON-DISK SIZE [${LABEL}] ---`);
  console.log(`  WAL files:    ${fmt(disk.wal)}`);
  console.log(`  TSM files:    ${fmt(disk.tsm)}`);
  console.log(`  Index files:  ${fmt(disk.index)}`);
  console.log(`  Total:        ${fmt(disk.total)}`);
  console.log(`  Shards used:  ${disk.shardCount}`);
  console.log('');
  console.log(`  Raw value data (doubles only): ${fmt(rawSize)}`);
  console.log(`  Bytes per data point (total):  ${(disk.total / totalPoints).toFixed(2)}`);
  console.log(`  Bytes per data point (WAL):    ${(disk.wal / totalPoints).toFixed(2)}`);
  console.log(`  Bytes per data point (TSM):    ${disk.tsm > 0 ? (disk.tsm / totalPoints).toFixed(2) : 'N/A (not flushed)'}`);
  console.log(`  WAL compression ratio:         ${(rawSize / disk.wal).toFixed(2)}x`);
  if (disk.tsm > 0)
    console.log(`  TSM compression ratio:         ${(rawSize / disk.tsm).toFixed(2)}x`);
  console.log('='.repeat(60));
}

main().catch(e => { console.error(e); process.exit(1); });
