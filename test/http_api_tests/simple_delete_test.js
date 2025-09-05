const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function runTest() {
  try {
    console.log('Starting simple delete test...');
    
    // Generate test data
    const testData = [];
    const startTime = 1704067200000000000; // Jan 1, 2024
    const hourInNanos = 3600000000000;
    
    // Create 10 data points
    for (let i = 0; i < 10; i++) {
      testData.push({
        measurement: 'test_delete',
        tags: {
          location: 'test'
        },
        fields: {
          value: i * 10
        },
        timestamp: startTime + (i * hourInNanos)
      });
    }
    
    console.log('1. Inserting initial data...');
    const insertResponse = await axios.post(`${BASE_URL}/write`, {
      writes: testData
    });
    console.log('   Insert response:', insertResponse.data);
    
    console.log('2. Querying all data...');
    const queryAllResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test_delete(value){location:test}',
      startTime: startTime,
      endTime: startTime + (10 * hourInNanos)
    });
    console.log('   Initial data points:', queryAllResponse.data.series[0].fields.value.timestamps.length);
    
    console.log('3. Deleting middle segment (items 3-6)...');
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: 'test_delete',
      tags: {
        location: 'test'
      },
      fields: ['value'],
      startTime: startTime + (3 * hourInNanos),
      endTime: startTime + (6 * hourInNanos)
    });
    console.log('   Delete response:', deleteResponse.data);
    
    console.log('4. Querying after delete...');
    const queryAfterDeleteResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test_delete(value){location:test}',
      startTime: startTime,
      endTime: startTime + (10 * hourInNanos)
    });
    const remainingPoints = queryAfterDeleteResponse.data.series[0].fields.value.timestamps.length;
    console.log('   Remaining data points:', remainingPoints);
    
    // Check if deletion worked
    if (remainingPoints === 10) {
      console.log('   ❌ DELETE NOT WORKING: Still have all 10 points');
    } else if (remainingPoints === 6) {
      console.log('   ✓ DELETE WORKING: Have 6 points as expected (deleted 4)');
    } else {
      console.log('   ? Unexpected result: Have', remainingPoints, 'points');
    }
    
    console.log('5. Re-inserting deleted data...');
    const reinsertData = testData.slice(3, 7); // Items that were deleted
    const reinsertResponse = await axios.post(`${BASE_URL}/write`, {
      writes: reinsertData  
    });
    console.log('   Reinsert response:', reinsertResponse.data);
    
    console.log('6. Final query...');
    const finalQueryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test_delete(value){location:test}',
      startTime: startTime,
      endTime: startTime + (10 * hourInNanos)
    });
    const finalPoints = finalQueryResponse.data.series[0].fields.value.timestamps.length;
    console.log('   Final data points:', finalPoints);
    
    if (finalPoints === 10) {
      console.log('   ✓ REINSERT WORKING: Have all 10 points again');
    } else {
      console.log('   ❌ REINSERT NOT WORKING: Have', finalPoints, 'points instead of 10');
    }
    
    console.log('\nTest complete!');
    
  } catch (error) {
    console.error('Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
  }
}

runTest();