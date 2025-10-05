#!/usr/bin/env node

const fs = require('fs');
const readline = require('readline');

async function analyzeDetailedLogs(logFile) {
  const fileStream = fs.createReadStream(logFile);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  const timings = {
    total: [],
    coalesce: [],
    compression: [],
    wal: [],
    walWrites: [],
    points: []
  };

  for await (const line of rl) {
    // Parse WRITE_TIMING logs
    const timingMatch = line.match(/\[WRITE_TIMING\] Total: (\d+)µs, Coalesce: (\d+)µs, Compression: (\d+)µs, WAL: (\d+)µs, WAL_writes: (\d+), Points: (\d+)/);
    if (timingMatch) {
      timings.total.push(parseInt(timingMatch[1]));
      timings.coalesce.push(parseInt(timingMatch[2]));
      timings.compression.push(parseInt(timingMatch[3]));
      timings.wal.push(parseInt(timingMatch[4]));
      timings.walWrites.push(parseInt(timingMatch[5]));
      timings.points.push(parseInt(timingMatch[6]));
    }
  }

  return timings;
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

  console.log('Analyzing detailed insert path performance...\n');

  const timings = await analyzeDetailedLogs(logFile);

  if (timings.total.length === 0) {
    console.log('No timing data found in logs.');
    return;
  }

  const totalStats = calculateStats(timings.total);
  const coalesceStats = calculateStats(timings.coalesce);
  const compressionStats = calculateStats(timings.compression);
  const walStats = calculateStats(timings.wal);
  const pointsStats = calculateStats(timings.points);

  console.log('=== INSERT PATH PERFORMANCE BREAKDOWN ===\n');
  console.log(`Total Batches Processed: ${totalStats.count}\n`);

  console.log('--- Timing Statistics (microseconds) ---\n');

  console.log('Total Request Time:');
  console.log(`  Sum:       ${(totalStats.sum / 1000).toFixed(2)} ms`);
  console.log(`  Average:   ${totalStats.avg.toFixed(2)} µs`);
  console.log(`  Min:       ${totalStats.min} µs`);
  console.log(`  Max:       ${totalStats.max} µs`);
  console.log();

  console.log('Coalesce Time:');
  console.log(`  Sum:       ${(coalesceStats.sum / 1000).toFixed(2)} ms`);
  console.log(`  Average:   ${coalesceStats.avg.toFixed(2)} µs`);
  console.log(`  Min:       ${coalesceStats.min} µs`);
  console.log(`  Max:       ${coalesceStats.max} µs`);
  console.log(`  % of Total: ${((coalesceStats.sum / totalStats.sum) * 100).toFixed(2)}%`);
  console.log();

  console.log('Compression Time:');
  console.log(`  Sum:       ${(compressionStats.sum / 1000).toFixed(2)} ms`);
  console.log(`  Average:   ${compressionStats.avg.toFixed(2)} µs`);
  console.log(`  % of Total: ${((compressionStats.sum / totalStats.sum) * 100).toFixed(2)}%`);
  console.log();

  console.log('WAL Time:');
  console.log(`  Sum:       ${(walStats.sum / 1000).toFixed(2)} ms`);
  console.log(`  Average:   ${walStats.avg.toFixed(2)} µs`);
  console.log(`  Min:       ${walStats.min} µs`);
  console.log(`  Max:       ${walStats.max} µs`);
  console.log(`  % of Total: ${((walStats.sum / totalStats.sum) * 100).toFixed(2)}%`);
  console.log();

  // Calculate "Other" time (overhead, logging, etc.)
  const accountedTime = coalesceStats.sum + compressionStats.sum + walStats.sum;
  const otherTime = totalStats.sum - accountedTime;
  console.log('Other Time (Memory Store, Overhead, etc.):');
  console.log(`  Sum:       ${(otherTime / 1000).toFixed(2)} ms`);
  console.log(`  Average:   ${(otherTime / totalStats.count).toFixed(2)} µs`);
  console.log(`  % of Total: ${((otherTime / totalStats.sum) * 100).toFixed(2)}%`);
  console.log();

  console.log('--- Data Statistics ---\n');
  console.log(`Total Points Inserted: ${pointsStats.sum}`);
  console.log(`Average Points per Batch: ${pointsStats.avg.toFixed(2)}`);
  console.log();

  console.log('--- Performance Metrics ---\n');
  const totalTimeSeconds = totalStats.sum / 1000000;
  const throughput = pointsStats.sum / totalTimeSeconds;
  console.log(`Total Processing Time: ${(totalTimeSeconds * 1000).toFixed(2)} ms`);
  console.log(`Throughput: ${throughput.toFixed(0)} points/second`);
  console.log(`Average Latency per Batch: ${totalStats.avg.toFixed(2)} µs`);
  console.log();

  console.log('--- Time Distribution ---\n');
  const components = [
    { name: 'Coalesce', time: coalesceStats.sum, pct: (coalesceStats.sum / totalStats.sum) * 100 },
    { name: 'Compression', time: compressionStats.sum, pct: (compressionStats.sum / totalStats.sum) * 100 },
    { name: 'WAL Writes', time: walStats.sum, pct: (walStats.sum / totalStats.sum) * 100 },
    { name: 'Other (Memory Store + Overhead)', time: otherTime, pct: (otherTime / totalStats.sum) * 100 }
  ];

  components.sort((a, b) => b.time - a.time);

  for (const comp of components) {
    const bar = '█'.repeat(Math.round(comp.pct / 2));
    console.log(`${comp.name.padEnd(35)} ${bar} ${comp.pct.toFixed(1)}%`);
  }
}

main().catch(console.error);
