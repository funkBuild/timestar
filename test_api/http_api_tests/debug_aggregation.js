const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const testMeasurement = `test_debug_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000;

const createTestData = async () => {
  const timestamps = [];
  for (let i = 0; i < 10; i++) {
    timestamps.push(now - (1000000000 * (10 - i - 1))); 
  }
  
  // Write data for 3 devices with different values
  const devices = [
    { id: 'device_a', paddock: 'north', multiplier: 1 },
    { id: 'device_b', paddock: 'north', multiplier: 10 },
    { id: 'device_c', paddock: 'south', multiplier: 100 }
  ];
  
  for (const device of devices) {
    const writes = timestamps.map((ts, i) => ({
      measurement: `${testMeasurement}`,
      tags: { 
        deviceId: device.id, 
        paddock: device.paddock 
      },
      fields: { 
        temperature: i * device.multiplier 
      },
      timestamp: ts
    }));
    
    const response = await axios.post(`${BASE_URL}/write`, { writes });
    console.log(`✓ Written data for ${device.id}`);
  }
};

const testQuery = async () => {
  console.log('\n=== Testing MIN aggregation ===\n');
  
  // Test 1: Simple MIN without any filters
  console.log('Query 1: min:' + testMeasurement + '(){}');
  const result1 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(){}`,
    startTime: now - 20000000000,
    endTime: now
  });
  
  console.log('Result series count:', result1.data.series.length);
  console.log('Series returned:');
  result1.data.series.forEach((s, i) => {
    console.log(`  Series ${i}:`, {
      measurement: s.measurement,
      tags: s.tags,
      fields: Object.keys(s.fields),
      temperature_values: s.fields.temperature?.values?.slice(0, 3) || 'N/A'
    });
  });
  
  console.log('\n=== Testing with specific field ===\n');
  
  // Test 2: MIN with specific field
  console.log('Query 2: min:' + testMeasurement + '(temperature){}');
  const result2 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){}`,
    startTime: now - 20000000000,
    endTime: now
  });
  
  console.log('Result series count:', result2.data.series.length);
  console.log('Expected: 1 series with aggregated MIN values across all devices');
  console.log('Actual series:');
  result2.data.series.forEach((s, i) => {
    console.log(`  Series ${i}:`, {
      tags: s.tags,
      temperature_count: s.fields.temperature?.values?.length || 0,
      temperature_values: s.fields.temperature?.values || 'N/A'
    });
  });
  
  console.log('\n=== Testing with tag filter ===\n');
  
  // Test 3: MIN with tag filter  
  console.log('Query 3: min:' + testMeasurement + '(temperature){paddock:north}');
  const result3 = await axios.post(`${BASE_URL}/query`, {
    query: `min:${testMeasurement}(temperature){paddock:north}`,
    startTime: now - 20000000000,
    endTime: now
  });
  
  console.log('Result series count:', result3.data.series.length);
  console.log('Expected: 1 series with MIN from north paddock devices only');
  console.log('Actual series:');
  result3.data.series.forEach((s, i) => {
    console.log(`  Series ${i}:`, {
      tags: s.tags,
      values: s.fields.temperature?.values || 'N/A'
    });
  });
};

const main = async () => {
  try {
    // Check server health
    await axios.get(`${BASE_URL}/health`);
    console.log('Server is healthy\n');
    
    await createTestData();
    await testQuery();
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
};

main();