const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

async function testErrorResponses() {
  console.log('=== Testing Error Response Format ===\n');
  
  // Test 1: Invalid aggregation method
  console.log('Test 1: Invalid aggregation method');
  try {
    await axios.post(`${BASE_URL}/query`, {
      query: 'invalid:test_measurement()',
      startTime: 0,
      endTime: 100
    });
    console.log('  ERROR: Should have thrown an error');
  } catch (error) {
    console.log('  Status:', error.response?.status);
    console.log('  Response data:', JSON.stringify(error.response?.data, null, 2));
    console.log('  Type of error field:', typeof error.response?.data?.error);
  }
  
  // Test 2: Missing measurement
  console.log('\nTest 2: Missing measurement');
  try {
    await axios.post(`${BASE_URL}/query`, {
      query: 'avg:(field)',
      startTime: 0,
      endTime: 100
    });
    console.log('  ERROR: Should have thrown an error');
  } catch (error) {
    console.log('  Status:', error.response?.status);
    console.log('  Response data:', JSON.stringify(error.response?.data, null, 2));
  }
  
  // Test 3: Missing field parentheses
  console.log('\nTest 3: Missing field parentheses');
  try {
    await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test_measurement{}',
      startTime: 0,
      endTime: 100
    });
    console.log('  ERROR: Should have thrown an error');
  } catch (error) {
    console.log('  Status:', error.response?.status);
    console.log('  Response data:', JSON.stringify(error.response?.data, null, 2));
  }
  
  console.log('\n=== Analysis ===');
  console.log('The tests expect error.response.data.error to be a string');
  console.log('But our implementation returns it as an object with {code, message}');
  console.log('This is why .toContain() is failing');
}

testErrorResponses().catch(console.error);