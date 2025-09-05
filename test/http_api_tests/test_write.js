const axios = require('axios');
const BASE_URL = 'http://localhost:8087';

async function testWrite() {
  // Clear and write fresh data
  const writes = [1, 2, 3, 4, 5, 6].map(ts => ({
    measurement: 'test_single',
    tags: { device: 'test' },
    fields: { 
      field1: ts * 10
    },
    timestamp: ts
  }));
  
  console.log('Writing:', JSON.stringify(writes, null, 2));
  
  await axios.post(`${BASE_URL}/write`, { writes });
  
  // Query back
  const response = await axios.post(`${BASE_URL}/query`, {
    query: 'avg:test_single(field1){device:test}',
    startTime: 0,
    endTime: 100
  });
  
  console.log('\nResponse:', JSON.stringify(response.data, null, 2));
  console.log('Number of values:', response.data.series[0]?.fields?.field1?.values?.length);
}

testWrite().catch(console.error);
