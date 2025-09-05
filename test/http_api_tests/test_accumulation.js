const axios = require('axios');
const BASE_URL = 'http://localhost:8087';

async function test() {
  // First write
  console.log('First write...');
  let writes = [1, 2].map(ts => ({
    measurement: 'accumtest',
    tags: { test: 'acc' },
    fields: { value: 100 },
    timestamp: ts
  }));
  await axios.post(`${BASE_URL}/write`, { writes });
  
  // Query
  let response = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:accumtest(value){test:acc}',
    startTime: 0,
    endTime: 100
  });
  console.log('After first write:', response.data.series[0]?.fields?.value?.values?.length, 'values');
  
  // Second write with same timestamps
  console.log('\nSecond write with same timestamps...');
  writes = [1, 2].map(ts => ({
    measurement: 'accumtest',
    tags: { test: 'acc' },
    fields: { value: 200 },
    timestamp: ts
  }));
  await axios.post(`${BASE_URL}/write`, { writes });
  
  // Query again
  response = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:accumtest(value){test:acc}',
    startTime: 0,
    endTime: 100
  });
  console.log('After second write:', response.data.series[0]?.fields?.value?.values?.length, 'values');
  console.log('Values:', response.data.series[0]?.fields?.value?.values);
}

test().catch(console.error);
