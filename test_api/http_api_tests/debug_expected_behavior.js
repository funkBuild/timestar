const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const testMeasurement = `test_expected_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000;

async function testExpectedBehavior() {
  console.log('=== EXPECTED BEHAVIOR TEST ===\n');
  
  // Create test data
  const timestamps = [];
  for (let i = 0; i < 5; i++) {
    timestamps.push(now - (1000000000 * (5 - i - 1))); 
  }
  
  // Write data for 2 devices
  const devices = [
    { id: 'sensor1', location: 'room1', values: [10, 20, 30, 40, 50] },
    { id: 'sensor2', location: 'room1', values: [15, 25, 35, 45, 55] }
  ];
  
  for (const device of devices) {
    const writes = timestamps.map((ts, i) => ({
      measurement: testMeasurement,
      tags: { sensorId: device.id, location: device.location },
      fields: { temperature: device.values[i] },
      timestamp: ts
    }));
    
    await axios.post(`${BASE_URL}/write`, { writes });
    console.log(`✓ Written data for ${device.id}:`, device.values);
  }
  
  console.log('\n--- CASE 1: MIN aggregation without group by ---');
  console.log('Query: min:' + testMeasurement + '(temperature){}');
  console.log('Expected behavior:');
  console.log('  - Should return 1 series (not 2)');
  console.log('  - Should have MIN across all data: [10, 20, 30, 40, 50]');
  console.log('  - Tags should be empty or omitted since we\'re aggregating across all series\n');
  
  const result1 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){}`,
    startTime: now - 10000000000,
    endTime: now
  });
  
  console.log('Actual result:');
  console.log('  - Series count:', result1.data.series.length);
  result1.data.series.forEach((s, i) => {
    console.log(`  - Series ${i}: tags=${JSON.stringify(s.tags)}, values=${s.fields.temperature?.values}`);
  });
  
  console.log('\n--- CASE 2: MIN aggregation WITH group by location ---');
  console.log('Query: min:' + testMeasurement + '(temperature){} by {location}');
  console.log('Expected behavior:');
  console.log('  - Should return 1 series (both sensors in same location)');
  console.log('  - Should have MIN values: [10, 20, 30, 40, 50]');
  console.log('  - Tags should show {location: "room1"}\n');
  
  const result2 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){} by {location}`,
    startTime: now - 10000000000,
    endTime: now
  });
  
  console.log('Actual result:');
  console.log('  - Series count:', result2.data.series.length);
  result2.data.series.forEach((s, i) => {
    console.log(`  - Series ${i}: tags=${JSON.stringify(s.tags)}, values=${s.fields.temperature?.values}`);
  });
  
  console.log('\n--- CASE 3: MIN aggregation WITH group by sensorId ---');
  console.log('Query: min:' + testMeasurement + '(temperature){} by {sensorId}');
  console.log('Expected behavior:');
  console.log('  - Should return 2 series (one per sensor)');
  console.log('  - sensor1 should have: [10, 20, 30, 40, 50]');
  console.log('  - sensor2 should have: [15, 25, 35, 45, 55]\n');
  
  const result3 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){} by {sensorId}`,
    startTime: now - 10000000000,
    endTime: now
  });
  
  console.log('Actual result:');
  console.log('  - Series count:', result3.data.series.length);
  result3.data.series.forEach((s, i) => {
    console.log(`  - Series ${i}: tags=${JSON.stringify(s.tags)}, values=${s.fields.temperature?.values}`);
  });
  
  console.log('\n=== SUMMARY ===');
  console.log('The key issue is that without "group by", aggregations should merge ALL series into one.');
  console.log('With "group by", series should be grouped by the specified tags before aggregation.');
}

testExpectedBehavior().catch(console.error);