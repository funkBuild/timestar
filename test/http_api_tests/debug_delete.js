const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTest() {
  try {
    console.log('=== Debug Delete Test ===\n');
    
    // Generate simple test data
    const testData = [];
    const startTime = 1000000000; // Simple timestamp
    
    // Create 5 data points
    for (let i = 0; i < 5; i++) {
      testData.push({
        measurement: 'debug_delete',
        tags: {
          test: 'delete'
        },
        fields: {
          value: i * 10
        },
        timestamp: startTime + i
      });
    }
    
    console.log('1. Inserting data:');
    console.log('   Timestamps:', testData.map(d => d.timestamp));
    console.log('   Values:', testData.map(d => d.fields.value));
    
    const insertResponse = await axios.post(`${BASE_URL}/write`, {
      writes: testData
    });
    console.log('   Response:', insertResponse.data);
    
    // Give it a moment
    await sleep(100);
    
    console.log('\n2. Query all data:');
    const queryAllResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:debug_delete(value){test:delete}',
      startTime: startTime - 1,
      endTime: startTime + 10
    });
    
    if (queryAllResponse.data.series && queryAllResponse.data.series.length > 0) {
      const series = queryAllResponse.data.series[0];
      console.log('   Found timestamps:', series.fields.value.timestamps);
      console.log('   Found values:', series.fields.value.values);
    } else {
      console.log('   No data found!');
    }
    
    console.log('\n3. Delete middle points (timestamps 1000000001-1000000003):');
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: 'debug_delete',
      tags: {
        test: 'delete'
      },
      fields: ['value'],
      startTime: startTime + 1,  // Delete points 1, 2, 3
      endTime: startTime + 3
    });
    console.log('   Delete response:', deleteResponse.data);
    
    // Give it a moment
    await sleep(100);
    
    console.log('\n4. Query after delete:');
    const queryAfterResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:debug_delete(value){test:delete}',
      startTime: startTime - 1,
      endTime: startTime + 10
    });
    
    if (queryAfterResponse.data.series && queryAfterResponse.data.series.length > 0) {
      const series = queryAfterResponse.data.series[0];
      console.log('   Found timestamps:', series.fields.value.timestamps);
      console.log('   Found values:', series.fields.value.values);
      
      // Check what we have
      const remainingTimestamps = series.fields.value.timestamps;
      const expectedRemaining = [startTime, startTime + 4]; // Should only have 0 and 4
      
      console.log('\n   Expected remaining:', expectedRemaining);
      console.log('   Actually remaining:', remainingTimestamps);
      
      if (remainingTimestamps.length === 2 && 
          remainingTimestamps[0] === expectedRemaining[0] &&
          remainingTimestamps[1] === expectedRemaining[1]) {
        console.log('   ✅ DELETE WORKING! Middle points removed correctly.');
      } else {
        console.log('   ❌ DELETE NOT WORKING as expected');
      }
    } else {
      console.log('   No data found!');
    }
    
  } catch (error) {
    console.error('Error:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
  }
}

runTest();