const axios = require('axios');
const BASE_URL = 'http://localhost:8086';

async function test() {
  console.log('Testing parser validation...\n');
  
  // Test 1: Query without parentheses (should fail)
  console.log('1. Query without parentheses:');
  try {
    await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test.measurement{tag:value}',
      startTime: 0,
      endTime: 100
    });
    console.log('   ❌ FAILED: Should have thrown error');
  } catch (error) {
    if (error.response && error.response.status === 400) {
      console.log('   ✅ PASS: Got 400 error');
      console.log('   Error message:', error.response.data.error || error.response.data);
    } else {
      console.log('   ❌ FAILED: Wrong error:', error.message);
    }
  }
  
  // Test 2: Query with empty parentheses (should work)
  console.log('\n2. Query with empty parentheses:');
  try {
    const resp = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test.measurement(){tag:value}',
      startTime: 0,
      endTime: 100
    });
    console.log('   ✅ PASS: Query accepted');
  } catch (error) {
    console.log('   ❌ FAILED: Should not throw error');
    console.log('   Error:', error.response?.data || error.message);
  }
  
  // Test 3: Query with fields in parentheses (should work)
  console.log('\n3. Query with fields in parentheses:');
  try {
    const resp = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test.measurement(field1,field2){tag:value}',
      startTime: 0,
      endTime: 100  
    });
    console.log('   ✅ PASS: Query accepted');
  } catch (error) {
    console.log('   ❌ FAILED: Should not throw error');
    console.log('   Error:', error.response?.data || error.message);
  }
}

test().catch(console.error);
