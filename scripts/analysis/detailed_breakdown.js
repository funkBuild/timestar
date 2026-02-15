#!/usr/bin/env node

const fs = require('fs');
const readline = require('readline');

async function analyzeDetailedBreakdown(logFile) {
  const fileStream = fs.createReadStream(logFile);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  const stats = {
    // HTTP-level timing
    http: {
      total: [],
      grouping: [],
      metadata: [],
      batchOps: []
    },
    // Memory store timing
    memory: {
      total: [],
      walInsert: [],
      inMemInsert: []
    },
    // Overall write timing
    write: {
      total: [],
      coalesce: [],
      compression: [],
      wal: [],
      points: [],
      walWrites: []
    }
  };

  for await (const line of rl) {
    // Parse HTTP breakdown: [PERF] [HTTP] processMultiWritePoint breakdown
    const httpMatch = line.match(/\[PERF\] \[HTTP\] processMultiWritePoint breakdown - Total: (\d+)μs, Grouping: (\d+)μs, Metadata: (\d+)μs, BatchOps: (\d+)μs/);
    if (httpMatch) {
      stats.http.total.push(parseInt(httpMatch[1]));
      stats.http.grouping.push(parseInt(httpMatch[2]));
      stats.http.metadata.push(parseInt(httpMatch[3]));
      stats.http.batchOps.push(parseInt(httpMatch[4]));
    }

    // Parse Memory store timing: [PERF] [MEMORY]
    const memTotalMatch = line.match(/\[PERF\] \[MEMORY\] Total batch insert: (\d+)μs/);
    if (memTotalMatch) {
      stats.memory.total.push(parseInt(memTotalMatch[1]));
    }

    const memWalMatch = line.match(/\[PERF\] \[MEMORY\] WAL batch insert: (\d+)μs/);
    if (memWalMatch) {
      stats.memory.walInsert.push(parseInt(memWalMatch[1]));
    }

    const memInMemMatch = line.match(/\[PERF\] \[MEMORY\] In-memory batch insert: (\d+)μs/);
    if (memInMemMatch) {
      stats.memory.inMemInsert.push(parseInt(memInMemMatch[1]));
    }

    // Parse overall write timing
    const writeMatch = line.match(/\[WRITE_TIMING\] Total: (\d+)μs, Coalesce: (\d+)μs, Compression: (\d+)μs, WAL: (\d+)μs, WAL_writes: (\d+), Points: (\d+)/);
    if (writeMatch) {
      stats.write.total.push(parseInt(writeMatch[1]));
      stats.write.coalesce.push(parseInt(writeMatch[2]));
      stats.write.compression.push(parseInt(writeMatch[3]));
      stats.write.wal.push(parseInt(writeMatch[4]));
      stats.write.walWrites.push(parseInt(writeMatch[5]));
      stats.write.points.push(parseInt(writeMatch[6]));
    }
  }

  return stats;
}

function calculateStats(arr) {
  if (arr.length === 0) return { sum: 0, avg: 0, min: 0, max: 0, count: 0 };
  const sum = arr.reduce((a, b) => a + b, 0);
  const avg = sum / arr.length;
  const min = Math.min(...arr);
  const max = Math.max(...arr);
  return { sum, avg, min, max, count: arr.length };
}

async function main() {
  const logFile = 'build/server.log';

  if (!fs.existsSync(logFile)) {
    console.error(`Log file not found: ${logFile}`);
    process.exit(1);
  }

  console.log('Analyzing detailed insert path breakdown...\n');

  const stats = await analyzeDetailedBreakdown(logFile);

  // Calculate aggregate stats
  const httpTotal = calculateStats(stats.http.total);
  const httpGrouping = calculateStats(stats.http.grouping);
  const httpMetadata = calculateStats(stats.http.metadata);
  const httpBatchOps = calculateStats(stats.http.batchOps);

  const memTotal = calculateStats(stats.memory.total);
  const memWal = calculateStats(stats.memory.walInsert);
  const memInMem = calculateStats(stats.memory.inMemInsert);

  const writeTotal = calculateStats(stats.write.total);
  const writeCoalesce = calculateStats(stats.write.coalesce);
  const writeWal = calculateStats(stats.write.wal);
  const writePoints = calculateStats(stats.write.points);

  console.log('=== DETAILED INSERT PATH BREAKDOWN ===\n');
  console.log(`Total Batches: ${writeTotal.count}`);
  console.log(`Total Points: ${writePoints.sum}\n`);

  console.log('--- HTTP Handler Level (per MultiWritePoint) ---\n');
  console.log(`HTTP Total:        Avg ${httpTotal.avg.toFixed(2)} µs  (Total: ${(httpTotal.sum/1000).toFixed(2)} ms)`);
  console.log(`  Grouping:        Avg ${httpGrouping.avg.toFixed(2)} µs  (Total: ${(httpGrouping.sum/1000).toFixed(2)} ms) [${((httpGrouping.sum/httpTotal.sum)*100).toFixed(1)}%]`);
  console.log(`  Metadata Index:  Avg ${httpMetadata.avg.toFixed(2)} µs  (Total: ${(httpMetadata.sum/1000).toFixed(2)} ms) [${((httpMetadata.sum/httpTotal.sum)*100).toFixed(1)}%]`);
  console.log(`  Batch Ops:       Avg ${httpBatchOps.avg.toFixed(2)} µs  (Total: ${(httpBatchOps.sum/1000).toFixed(2)} ms) [${((httpBatchOps.sum/httpTotal.sum)*100).toFixed(1)}%]`);
  console.log();

  console.log('--- Memory Store Level (per shard batch) ---\n');
  console.log(`Memory Total:      Avg ${memTotal.avg.toFixed(2)} µs  (Total: ${(memTotal.sum/1000).toFixed(2)} ms)`);
  console.log(`  WAL Insert:      Avg ${memWal.avg.toFixed(2)} µs  (Total: ${(memWal.sum/1000).toFixed(2)} ms) [${((memWal.sum/memTotal.sum)*100).toFixed(1)}%]`);
  console.log(`  In-Mem Insert:   Avg ${memInMem.avg.toFixed(2)} µs  (Total: ${(memInMem.sum/1000).toFixed(2)} ms) [${((memInMem.sum/memTotal.sum)*100).toFixed(1)}%]`);
  console.log();

  console.log('--- Overall Write Timing (end-to-end per batch) ---\n');
  console.log(`Write Total:       ${(writeTotal.sum/1000).toFixed(2)} ms`);
  console.log(`  Coalesce:        ${(writeCoalesce.sum/1000).toFixed(2)} ms [${((writeCoalesce.sum/writeTotal.sum)*100).toFixed(1)}%]`);
  console.log(`  WAL Reported:    ${(writeWal.sum/1000).toFixed(2)} ms [${((writeWal.sum/writeTotal.sum)*100).toFixed(1)}%]`);
  const otherTime = writeTotal.sum - writeCoalesce.sum - writeWal.sum;
  console.log(`  Other/Overhead:  ${(otherTime/1000).toFixed(2)} ms [${((otherTime/writeTotal.sum)*100).toFixed(1)}%]`);
  console.log();

  console.log('--- Detailed "Other/Overhead" Breakdown ---\n');
  console.log('The "Other/Overhead" (51%) includes:\n');

  // HTTP-level breakdown
  const httpPct = (httpTotal.sum / writeTotal.sum) * 100;
  const groupingPct = (httpGrouping.sum / writeTotal.sum) * 100;
  const metadataPct = (httpMetadata.sum / writeTotal.sum) * 100;
  const batchOpsPct = (httpBatchOps.sum / writeTotal.sum) * 100;

  console.log(`1. HTTP Request Processing: ${(httpTotal.sum/1000).toFixed(2)} ms [${httpPct.toFixed(1)}%]`);
  console.log(`   - Request grouping/parsing:       ${(httpGrouping.sum/1000).toFixed(2)} ms [${groupingPct.toFixed(1)}%]`);
  console.log(`   - Metadata indexing (LevelDB):    ${(httpMetadata.sum/1000).toFixed(2)} ms [${metadataPct.toFixed(1)}%]`);
  console.log(`   - Shard dispatch + coordination:  ${(httpBatchOps.sum/1000).toFixed(2)} ms [${batchOpsPct.toFixed(1)}%]`);
  console.log();

  // Memory store breakdown within BatchOps
  const memPct = (memTotal.sum / writeTotal.sum) * 100;
  const inMemPct = (memInMem.sum / writeTotal.sum) * 100;

  console.log(`2. Memory Store Operations: ${(memTotal.sum/1000).toFixed(2)} ms [${memPct.toFixed(1)}%]`);
  console.log(`   - WAL writes:                     ${(memWal.sum/1000).toFixed(2)} ms [${((memWal.sum/writeTotal.sum)*100).toFixed(1)}%]`);
  console.log(`   - In-memory data structure:       ${(memInMem.sum/1000).toFixed(2)} ms [${inMemPct.toFixed(1)}%]`);
  console.log();

  // Remainder
  const asyncOverhead = otherTime - httpTotal.sum;
  const asyncPct = (asyncOverhead / writeTotal.sum) * 100;
  console.log(`3. Async/Logging/Framework: ${(asyncOverhead/1000).toFixed(2)} ms [${asyncPct.toFixed(1)}%]`);
  console.log(`   (Seastar futures, logging, context switches)`);
  console.log();

  console.log('--- Performance Summary ---\n');
  console.log(`Total Processing Time: ${(writeTotal.sum/1000).toFixed(2)} ms`);
  console.log(`Throughput: ${Math.round(writePoints.sum / (writeTotal.sum/1000000))} points/second`);
  console.log(`Average Latency: ${writeTotal.avg.toFixed(2)} µs per batch`);
}

main().catch(console.error);
