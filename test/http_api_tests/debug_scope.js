const axios = require('axios');

const HOST = process.env.TSDB_HOST || 'localhost';
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const testMeasurement = `test_scope_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000;

async function testScopeFiltering() {
  console.log('=== Testing Scope Filtering ===\n');
  
  // Create test data
  const timestamps = [];
  for (let i = 0; i < 5; i++) {
    timestamps.push(now - (1000000000 * (5 - i)));
  }
  
  // Write data for devices in different locations
  const devices = [
    { id: 'device1', location: 'room1', values: [10, 20, 30, 40, 50] },
    { id: 'device2', location: 'room1', values: [15, 25, 35, 45, 55] },
    { id: 'device3', location: 'room2', values: [100, 200, 300, 400, 500] }
  ];
  
  for (const device of devices) {
    const writes = timestamps.map((ts, i) => ({
      measurement: testMeasurement,
      tags: { deviceId: device.id, location: device.location },
      fields: { temperature: device.values[i] },
      timestamp: ts
    }));
    
    await axios.post(`${BASE_URL}/write`, { writes });
    console.log(`✓ Written data for ${device.id} in ${device.location}`);
  }
  
  console.log('\n--- Test 1: Query without scope (all data) ---');
  const result1 = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${testMeasurement}(temperature){}`,
    startTime: timestamps[0] - 1000000,
    endTime: timestamps[timestamps.length - 1] + 1000000
  });
  
  console.log('Result:');
  console.log('  Series count:', result1.data.series.length);
  console.log('  Values:', result1.data.series[0]?.fields.temperature?.values);
  console.log('  Expected: Average of all devices at each timestamp');
  console.log('  Expected values: [41.67, 81.67, 121.67, 161.67, 201.67]');
  
  console.log('\n--- Test 2: Query with scope filter ---');
  const result2 = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${testMeasurement}(temperature){location:room1}`,
    startTime: timestamps[0] - 1000000,
    endTime: timestamps[timestamps.length - 1] + 1000000
  });
  
  console.log('Result:');
  console.log('  Series count:', result2.data.series.length);
  console.log('  Values:', result2.data.series[0]?.fields.temperature?.values);
  console.log('  Scopes in response:', result2.data.scopes);
  console.log('  Expected: Average of room1 devices only');
  console.log('  Expected values: [12.5, 22.5, 32.5, 42.5, 52.5]');
  
  console.log('\n--- Test 3: Query with non-matching scope ---');
  const result3 = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${testMeasurement}(temperature){location:room99}`,
    startTime: timestamps[0] - 1000000,
    endTime: timestamps[timestamps.length - 1] + 1000000
  });
  
  console.log('Result:');
  console.log('  Series count:', result3.data.series.length);
  console.log('  Expected: 0 series (no matching data)');
}

testScopeFiltering().catch(console.error);