#!/usr/bin/env node

const http = require('http');

const NUM_POINTS = 10000;
const BATCH_SIZE = 100;
const PORT = 8086;
const HOST = 'localhost';

async function sendBatch(writes) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify({ writes });

    const options = {
      hostname: HOST,
      port: PORT,
      path: '/write',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': data.length
      }
    };

    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', (chunk) => body += chunk);
      res.on('end', () => {
        if (res.statusCode === 200) {
          resolve(JSON.parse(body));
        } else {
          reject(new Error(`HTTP ${res.statusCode}: ${body}`));
        }
      });
    });

    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

async function main() {
  console.log(`Inserting ${NUM_POINTS} points in batches of ${BATCH_SIZE}...`);
  const startTime = Date.now();

  const baseTimestamp = 1704067200000000000n; // Jan 1, 2024 in nanoseconds

  for (let batchNum = 0; batchNum < NUM_POINTS / BATCH_SIZE; batchNum++) {
    const writes = [];

    for (let i = 0; i < BATCH_SIZE; i++) {
      const pointNum = batchNum * BATCH_SIZE + i;
      const timestamp = baseTimestamp + BigInt(pointNum) * 1000000000n; // 1 second apart

      writes.push({
        measurement: 'temperature',
        tags: {
          location: `loc_${pointNum % 10}`,
          sensor: `sensor_${pointNum % 5}`
        },
        fields: {
          value: 20 + Math.random() * 10,
          humidity: 40 + Math.random() * 30
        },
        timestamp: timestamp.toString()
      });
    }

    try {
      await sendBatch(writes);
      if ((batchNum + 1) % 10 === 0) {
        process.stdout.write(`\rProgress: ${(batchNum + 1) * BATCH_SIZE}/${NUM_POINTS} points`);
      }
    } catch (error) {
      console.error(`\nError sending batch ${batchNum}:`, error.message);
      process.exit(1);
    }
  }

  const endTime = Date.now();
  const totalTime = (endTime - startTime) / 1000;
  const throughput = NUM_POINTS / totalTime;

  console.log(`\n\nCompleted!`);
  console.log(`Total time: ${totalTime.toFixed(2)} seconds`);
  console.log(`Throughput: ${throughput.toFixed(0)} points/second`);
}

main().catch(console.error);
