const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function runQuickTest() {
  const now = Date.now() * 1000000;
  const yearAgo = now - (525600 * 60 * 1000000000);

  // Test group-by query (the slow one)
  const query = {
    query: 'avg:server.metrics(cpu_usage,memory_usage,network_in,network_out){} by {rack}',
    startTime: yearAgo,
    endTime: now,
    aggregationInterval: "12h"
  };

  console.log('Running group-by query test...');
  const start = Date.now();
  const response = await axios.post(`${BASE_URL}/query`, query);
  const end = Date.now();

  console.log(`\nQuery completed in ${end - start}ms`);
  console.log(`Series count: ${response.data.statistics?.series_count || 0}`);
  console.log(`Point count: ${response.data.statistics?.point_count || 0}`);
}

runQuickTest().catch(console.error);
