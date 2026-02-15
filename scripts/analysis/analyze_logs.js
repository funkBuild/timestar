#!/usr/bin/env node

const fs = require('fs');
const readline = require('readline');

async function analyzeLogs(logFile) {
  const fileStream = fs.createReadStream(logFile);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  const stats = {
    walInserts: 0,
    walFlushes: 0,
    writeProcessing: 0,
    walFlushTimes: [],
    timestampStart: null,
    timestampEnd: null
  };

  for await (const line of rl) {
    // Track timestamp range
    const tsMatch = line.match(/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+)/);
    if (tsMatch) {
      const ts = tsMatch[1];
      if (!stats.timestampStart) stats.timestampStart = ts;
      stats.timestampEnd = ts;
    }

    // Count WAL inserts
    if (line.includes('WAL::insert')) {
      stats.walInserts++;
    }

    // Count WAL flushes and extract timing
    if (line.includes('WAL stream flush')) {
      stats.walFlushes++;
      const timeMatch = line.match(/took=(\d+)ms/);
      if (timeMatch) {
        stats.walFlushTimes.push(parseInt(timeMatch[1]));
      }
    }

    // Count write processing
    if (line.includes('[WRITE] Processing MultiWritePoint')) {
      stats.writeProcessing++;
    }
  }

  return stats;
}

async function main() {
  const logFile = 'build/server.log';

  if (!fs.existsSync(logFile)) {
    console.error(`Log file not found: ${logFile}`);
    process.exit(1);
  }

  console.log('Analyzing insert path logs...\n');

  const stats = await analyzeLogs(logFile);

  console.log('=== Insert Path Performance Breakdown ===\n');

  console.log(`Time Range: ${stats.timestampStart} to ${stats.timestampEnd}\n`);

  console.log('Operation Counts:');
  console.log(`  WAL Inserts:          ${stats.walInserts}`);
  console.log(`  WAL Flushes:          ${stats.walFlushes}`);
  console.log(`  Write Processings:    ${stats.writeProcessing}`);
  console.log();

  if (stats.walFlushTimes.length > 0) {
    const totalFlushTime = stats.walFlushTimes.reduce((a, b) => a + b, 0);
    const avgFlushTime = totalFlushTime / stats.walFlushTimes.length;
    const maxFlushTime = Math.max(...stats.walFlushTimes);
    const minFlushTime = Math.min(...stats.walFlushTimes);

    console.log('WAL Flush Timing (milliseconds):');
    console.log(`  Total:     ${totalFlushTime.toFixed(2)} ms`);
    console.log(`  Average:   ${avgFlushTime.toFixed(2)} ms`);
    console.log(`  Min:       ${minFlushTime.toFixed(2)} ms`);
    console.log(`  Max:       ${maxFlushTime.toFixed(2)} ms`);
    console.log();
  }

  // Calculate approximate processing time
  if (stats.timestampStart && stats.timestampEnd) {
    const start = new Date(stats.timestampStart.replace(',', '.'));
    const end = new Date(stats.timestampEnd.replace(',', '.'));
    const durationMs = end - start;
    const durationSec = durationMs / 1000;

    console.log('Overall Metrics:');
    console.log(`  Total Duration:   ${durationSec.toFixed(3)} seconds`);
    if (stats.walInserts > 0) {
      const insertsPerSec = stats.walInserts / durationSec;
      console.log(`  WAL Insert Rate:  ${insertsPerSec.toFixed(0)} inserts/sec`);
    }
  }
}

main().catch(console.error);
