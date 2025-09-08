const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function debugTimeBucket() {
  console.log('=== Debug Time Bucket Test ===\n');
  
  const measurement = `test_bucket_${Math.floor(Math.random() * 10000)}`;
  
  // Create 100 data points at 1-second intervals
  const now = Date.now() * 1000000; // nanoseconds
  const testData = { writes: [] };
  
  for (let i = 0; i < 100; i++) {
    testData.writes.push({
      measurement,
      tags: { device: 'A' },
      fields: { value: i * 0.1 },
      timestamp: now - (1000000000 * (99 - i)) // 1 second intervals backwards
    });
  }
  
  console.log('1. Writing 100 test points at 1-second intervals');
  console.log(`   First timestamp: ${testData.writes[0].timestamp}`);
  console.log(`   Last timestamp: ${testData.writes[99].timestamp}`);
  console.log(`   Total time span: ${(testData.writes[99].timestamp - testData.writes[0].timestamp) / 1000000000} seconds`);
  
  const writeResp = await axios.post(`${BASE_URL}/write`, testData);
  console.log('   Write response:', writeResp.data);
  
  // Query with 10-second buckets
  console.log('\n2. Query with 10-second buckets:');
  const query = {
    query: `avg:${measurement}(value){}`,
    startTime: testData.writes[0].timestamp,
    endTime: testData.writes[99].timestamp,
    aggregationInterval: 10000000000  // 10 seconds in nanoseconds
  };
  console.log('   Query:', JSON.stringify(query, null, 2));
  
  const resp = await axios.post(`${BASE_URL}/query`, query);
  console.log('   Response:', JSON.stringify(resp.data, null, 2));
  
  if (resp.data.series && resp.data.series.length > 0) {
    const timestamps = resp.data.series[0].fields.value.timestamps;
    const values = resp.data.series[0].fields.value.values;
    
    console.log(`\n3. Bucket Analysis:`);
    console.log(`   Number of buckets: ${timestamps.length}`);
    console.log(`   Expected buckets: 10 (100 points / 10 seconds)`);
    
    if (timestamps.length !== 10) {
      console.log('   ❌ INCORRECT number of buckets!');
      console.log('\n   Bucket details:');
      for (let i = 0; i < timestamps.length; i++) {
        const bucketStart = timestamps[i];
        const bucketEnd = i < timestamps.length - 1 ? timestamps[i + 1] : testData.writes[99].timestamp;
        const bucketDuration = (bucketEnd - bucketStart) / 1000000000;
        console.log(`   Bucket ${i + 1}: timestamp=${bucketStart}, value=${values[i].toFixed(2)}, duration=${bucketDuration}s`);
      }
      
      // Check if there's an extra bucket at the end
      const lastBucketTime = timestamps[timestamps.length - 1];
      const lastDataTime = testData.writes[99].timestamp;
      console.log(`\n   Last bucket timestamp: ${lastBucketTime}`);
      console.log(`   Last data timestamp: ${lastDataTime}`);
      console.log(`   Difference: ${(lastDataTime - lastBucketTime) / 1000000000} seconds`);
    } else {
      console.log('   ✅ CORRECT number of buckets!');
    }
  }
}

debugTimeBucket().catch(console.error);