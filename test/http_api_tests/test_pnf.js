const axios = require('axios');
const BASE_URL = 'http://localhost:8087';

async function testPnf() {
  // Write test data
  const writes = [1, 2, 3, 4, 5, 6].map(ts => ({
    measurement: 'lid_data',
    tags: { meter_id: '33616' },
    fields: { 
      pnf: 0,
      pnf_status: 1
    },
    timestamp: ts
  }));
  
  await axios.post(`${BASE_URL}/write`, { writes });
  
  // Query
  const response = await axios.post(`${BASE_URL}/query`, {
    query: 'avg:lid_data(pnf){meter_id:33616}',
    startTime: 0,
    endTime: 1000
  });
  
  console.log('Response:', JSON.stringify(response.data, null, 2));
  console.log('Number of values:', response.data.series[0]?.fields?.pnf?.values?.length);
}

testPnf().catch(console.error);
