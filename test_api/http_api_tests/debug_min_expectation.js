const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const testMeasurement = `test_min_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000;

async function testMinBehavior() {
  console.log('=== Testing MIN Aggregation Behavior ===\n');
  
  // Create timestamps
  const timestamps = [];
  for (let i = 0; i < 5; i++) {
    timestamps.push(now - (1000000000 * (5 - i))); // 5 timestamps, 1 second apart
  }
  
  // Write data for 3 devices with different values at each timestamp
  const devices = [
    { id: 'device1', values: [10, 20, 30, 40, 50] },  // Lowest at each timestamp
    { id: 'device2', values: [15, 25, 35, 45, 55] },  // Middle values
    { id: 'device3', values: [20, 30, 40, 50, 60] }   // Highest values
  ];
  
  console.log('Test data:');
  for (const device of devices) {
    const writes = timestamps.map((ts, i) => ({
      measurement: testMeasurement,
      tags: { deviceId: device.id },
      fields: { temperature: device.values[i] },
      timestamp: ts
    }));
    
    await axios.post(`${BASE_URL}/write`, { writes });
    console.log(`  ${device.id}: ${device.values}`);
  }
  
  console.log('\n--- Testing MIN aggregation ---');
  console.log('Query: min:' + testMeasurement + '(temperature){}');
  
  const result = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){}`,
    startTime: timestamps[0] - 1000000,
    endTime: timestamps[timestamps.length - 1] + 1000000
  });
  
  console.log('\nExpected behavior for MIN across all devices:');
  console.log('  Option A: Single MIN value across all data points = 10');
  console.log('  Option B: MIN at each timestamp = [10, 20, 30, 40, 50]');
  
  console.log('\nActual result:');
  console.log('  Series count:', result.data.series.length);
  if (result.data.series.length > 0) {
    const series = result.data.series[0];
    const values = series.fields.temperature?.values;
    const timestamps = series.fields.temperature?.timestamps;
    console.log('  Values returned:', values);
    console.log('  Number of values:', values?.length);
    if (timestamps && timestamps.length > 1) {
      console.log('  Timestamps returned:', timestamps.length, 'timestamps');
    }
  }
  
  // Test with aggregation interval to see behavior
  console.log('\n--- Testing MIN with time intervals ---');
  console.log('Query: min:' + testMeasurement + '(temperature){} with 2s interval');
  
  const result2 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){}`,
    startTime: timestamps[0] - 1000000,
    endTime: timestamps[timestamps.length - 1] + 1000000,
    aggregationInterval: 2000000000  // 2 seconds
  });
  
  console.log('\nWith time intervals:');
  console.log('  Series count:', result2.data.series.length);
  if (result2.data.series.length > 0) {
    const series = result2.data.series[0];
    const values = series.fields.temperature?.values;
    console.log('  Values returned:', values);
    console.log('  Number of buckets:', values?.length);
  }
  
  console.log('\n=== Analysis ===');
  console.log('The test expects Option B behavior: MIN at each timestamp');
  console.log('Current implementation provides Option A: single MIN across all data');
  console.log('This is why the test expects 100 values but gets 1');
}

testMinBehavior().catch(console.error);