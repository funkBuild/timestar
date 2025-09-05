const axios = require('axios');
const BASE_URL = 'http://localhost:8087';

async function test() {
  console.log('=== Testing String Write and Query ===');
  
  // Write a simple string value
  const write = {
    measurement: 'test_str',
    tags: { device: 'cam1' },
    fields: { 
      image: 'test_image.jpg'
    },
    timestamp: 100
  };
  
  console.log('\nWriting:', JSON.stringify(write, null, 2));
  const writeResp = await axios.post(`${BASE_URL}/write`, write);
  console.log('Write response:', writeResp.data);
  
  // Query it back
  console.log('\nQuerying for string field...');
  const queryResp = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:test_str(image){device:cam1}',
    startTime: 0,
    endTime: 1000
  });
  
  console.log('Query response:', JSON.stringify(queryResp.data, null, 2));
}

test().catch(console.error);
