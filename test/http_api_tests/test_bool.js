const axios = require('axios');
const BASE_URL = 'http://localhost:8087';

async function test() {
  // Write boolean data
  const write = {
    measurement: 'test_bool',
    tags: { sensor: 'door1' },
    fields: { 
      is_open: true
    },
    timestamp: 200
  };
  
  console.log('Writing:', JSON.stringify(write, null, 2));
  await axios.post(`${BASE_URL}/write`, write);
  
  // Query it back
  const queryResp = await axios.post(`${BASE_URL}/query`, {
    query: 'latest:test_bool(is_open){sensor:door1}',
    startTime: 0,
    endTime: 1000
  });
  
  console.log('Query response:', JSON.stringify(queryResp.data, null, 2));
}

test().catch(console.error);
