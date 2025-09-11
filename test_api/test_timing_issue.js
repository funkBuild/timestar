const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testWithDelay(delayMs = 0) {
  try {
    console.log(`=== Testing with ${delayMs}ms delay ===`);
    
    // Insert data
    const insertData = {
      measurement: 'timing_test',
      tags: { device: 'test-device' },
      fields: { temperature: 25.0, humidity: 60.0 },
      timestamp: 1704067200000000000
    };
    
    await axios.post(`${BASE_URL}/write`, insertData);
    console.log('Data inserted');
    
    // Delete temperature field
    const deleteRequest = {
      measurement: 'timing_test',
      tags: { device: 'test-device' },
      fields: ['temperature'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000000
    };
    
    await axios.post(`${BASE_URL}/delete`, deleteRequest);
    console.log('Delete request completed');
    
    // Add delay if specified
    if (delayMs > 0) {
      console.log(`Waiting ${delayMs}ms...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
    
    // Query immediately after delete
    const queryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:timing_test(temperature,humidity){device:test-device}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    
    const series = queryResponse.data.series?.[0];
    const hasTemperature = !!series?.fields?.temperature;
    const tempCount = series?.fields?.temperature?.timestamps?.length || 0;
    const humidityCount = series?.fields?.humidity?.timestamps?.length || 0;
    
    console.log(`Temperature present: ${hasTemperature}, count: ${tempCount}`);
    console.log(`Humidity count: ${humidityCount}`);
    
    return {
      delayMs,
      temperatureStillPresent: hasTemperature && tempCount > 0,
      humidityPreserved: humidityCount > 0
    };
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
    return { delayMs, error: true };
  }
}

async function runTimingTests() {
  console.log('Testing delete/query timing coordination...\n');
  
  // Test with different delays
  const delays = [0, 10, 50, 100, 500];
  const results = [];
  
  for (const delay of delays) {
    const result = await testWithDelay(delay);
    results.push(result);
    
    // Clear state between tests
    await new Promise(resolve => setTimeout(resolve, 100));
    console.log('');
  }
  
  console.log('Summary:');
  for (const result of results) {
    if (result.error) {
      console.log(`${result.delayMs}ms: ERROR`);
    } else {
      const status = result.temperatureStillPresent ? '❌ FAIL' : '✅ PASS';
      console.log(`${result.delayMs}ms: ${status} (temp=${result.temperatureStillPresent}, humidity=${result.humidityPreserved})`);
    }
  }
}

runTimingTests();