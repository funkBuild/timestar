const axios = require('axios');
const BASE_URL = 'http://localhost:8088';

async function test() {
  // Write string data
  const writes = [1, 2].map((ts, i) => ({
    measurement: 'test_strings_debug',
    tags: { device: 'camera' },
    fields: { 
      image: `ref::image${i+1}::s3://bucket/image${i+1}.jpeg`,
      count: i + 1  // Add a numeric field too
    },
    timestamp: ts
  }));
  
  console.log('Writing:', JSON.stringify(writes, null, 2));
  await axios.post(`${BASE_URL}/write`, { writes });
  
  // Query for numeric field first
  console.log('\nQuerying numeric field...');
  let response = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:test_strings_debug(count){device:camera}',
    startTime: 0,
    endTime: 100
  });
  console.log('Numeric query response:', JSON.stringify(response.data, null, 2));
  
  // Now query for string field
  console.log('\nQuerying string field...');
  response = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:test_strings_debug(image){device:camera}',
    startTime: 0,
    endTime: 100
  });
  console.log('String query response:', JSON.stringify(response.data, null, 2));
  
  // Query for all fields
  console.log('\nQuerying all fields...');
  response = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:test_strings_debug(){device:camera}',
    startTime: 0,
    endTime: 100
  });
  console.log('All fields response:', JSON.stringify(response.data, null, 2));
}

test().catch(console.error);
