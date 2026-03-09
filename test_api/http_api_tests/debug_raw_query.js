const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const testMeasurement = `test_raw_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000;

async function testRawQuery() {
  console.log('=== Testing Raw Query Without Aggregation ===\n');
  
  // Write mixed data types
  console.log('--- Writing Mixed Data ---');
  const writes = [
    {
      measurement: testMeasurement,
      tags: { type: 'numeric' },
      fields: { value: 42.5 },
      timestamp: now - 2000000000
    },
    {
      measurement: testMeasurement,
      tags: { type: 'boolean' },
      fields: { flag: true },
      timestamp: now - 1500000000
    },
    {
      measurement: testMeasurement,
      tags: { type: 'string' },
      fields: { message: 'Hello World' },
      timestamp: now - 1000000000
    }
  ];
  
  const writeResp = await axios.post(`${BASE_URL}/write`, { writes });
  console.log('Write response:', writeResp.data);
  
  // Try different queries
  console.log('\n--- Query 1: AVG aggregation (numeric) ---');
  try {
    const q1 = await axios.post(`${BASE_URL}/query`, {
      query: `avg:${testMeasurement}(value){type:numeric}`,
      startTime: now - 3000000000,
      endTime: now
    });
    console.log('Result:', JSON.stringify(q1.data, null, 2));
  } catch (e) {
    console.log('Error:', e.response?.data || e.message);
  }
  
  console.log('\n--- Query 2: LATEST aggregation (boolean) ---');
  try {
    const q2 = await axios.post(`${BASE_URL}/query`, {
      query: `latest:${testMeasurement}(flag){type:boolean}`,
      startTime: now - 3000000000,
      endTime: now
    });
    console.log('Result:', JSON.stringify(q2.data, null, 2));
  } catch (e) {
    console.log('Error:', e.response?.data || e.message);
  }
  
  console.log('\n--- Query 3: LATEST aggregation (string) ---');
  try {
    const q3 = await axios.post(`${BASE_URL}/query`, {
      query: `latest:${testMeasurement}(message){type:string}`,
      startTime: now - 3000000000,
      endTime: now
    });
    console.log('Result:', JSON.stringify(q3.data, null, 2));
  } catch (e) {
    console.log('Error:', e.response?.data || e.message);
  }
  
  console.log('\n--- Query 4: All fields without filter ---');
  try {
    const q4 = await axios.post(`${BASE_URL}/query`, {
      query: `latest:${testMeasurement}(){}`,
      startTime: now - 3000000000,
      endTime: now
    });
    console.log('Result:', JSON.stringify(q4.data, null, 2));
  } catch (e) {
    console.log('Error:', e.response?.data || e.message);
  }
}

testRawQuery().catch(console.error);