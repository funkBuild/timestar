const axios = require('axios');
const BASE_URL = 'http://localhost:8087';

async function test() {
  // Write string data
  const writes = [1, 2].map((ts, i) => ({
    measurement: 'test_strings',
    tags: { device: 'camera' },
    fields: { 
      image: `ref::image${i+1}::s3://bucket/image${i+1}.jpeg`
    },
    timestamp: ts
  }));
  
  console.log('Writing:', JSON.stringify(writes, null, 2));
  await axios.post(`${BASE_URL}/write`, { writes });
  
  // Query
  const response = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:test_strings(){device:camera}',
    startTime: 0,
    endTime: 100
  });
  
  console.log('\nResponse:', JSON.stringify(response.data, null, 2));
}

test().catch(console.error);
