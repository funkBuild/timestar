const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function runTest(label, query) {
  const times = [];
  for (let i = 0; i < 3; i++) {
    const start = Date.now();
    await axios.post(`${BASE_URL}/query`, query);
    const end = Date.now();
    times.push(end - start);
  }
  const avg = times.reduce((a, b) => a + b, 0) / times.length;
  console.log(`${label}: ${avg.toFixed(2)}ms (${times.join(', ')})`);
}

async function main() {
  const now = Date.now() * 1000000;
  const yearAgo = now - (525600 * 60 * 1000000000);

  console.log('Testing aggregation performance...\n');

  await runTest('Baseline - Group by rack', {
    query: 'avg:server.metrics(cpu_usage,memory_usage,network_in,network_out){} by {rack}',
    startTime: yearAgo,
    endTime: now,
    aggregationInterval: '12h'
  });
}

main().catch(console.error);
