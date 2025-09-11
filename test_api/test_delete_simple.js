const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testDelete() {
  try {
    console.log('1. Writing test data...');
    const writeData = {
      measurement: 'test_delete',
      tags: { location: 'test' },
      fields: { value: 100.0 },
      timestamp: 1704067200000000000
    };
    
    const writeResponse = await axios.post(`${BASE_URL}/write`, writeData);
    console.log('Write response:', writeResponse.data);
    
    console.log('\n2. Querying data before delete...');
    const queryBefore = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test_delete(value){location:test}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    console.log('Query before delete:', JSON.stringify(queryBefore.data, null, 2));
    
    console.log('\n3. Deleting data...');
    const deleteRequest = {
      measurement: 'test_delete',
      tags: { location: 'test' },
      fields: ['value'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    };
    console.log('Delete request:', JSON.stringify(deleteRequest, null, 2));
    
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, deleteRequest);
    console.log('Delete response:', deleteResponse.data);
    
    console.log('\n4. Querying data after delete...');
    const queryAfter = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test_delete(value){location:test}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    console.log('Query after delete:', JSON.stringify(queryAfter.data, null, 2));
    
    // Check if deletion worked
    const beforeCount = queryBefore.data.series?.[0]?.fields?.value?.timestamps?.length || 0;
    const afterCount = queryAfter.data.series?.[0]?.fields?.value?.timestamps?.length || 0;
    
    console.log(`\nResult: Before delete: ${beforeCount} points, After delete: ${afterCount} points`);
    if (afterCount === 0 && beforeCount === 1) {
      console.log('✅ DELETE WORKS!');
    } else {
      console.log('❌ DELETE DOES NOT WORK!');
    }
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
}

testDelete();