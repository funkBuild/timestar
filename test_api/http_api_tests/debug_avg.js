const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function debugAvgAggregation() {
  console.log('=== Debug AVG Aggregation Test ===\n');
  
  const now = Date.now() * 1000000; // nanoseconds
  const measurement = `debug_avg_${Math.floor(Math.random() * 10000)}`;
  
  // Create simple test data
  const testData = {
    writes: [
      // Device A - values 10, 20, 30
      { measurement, tags: { device: 'A', location: 'room1' }, fields: { temp: 10 }, timestamp: now - 2000000000 },
      { measurement, tags: { device: 'A', location: 'room1' }, fields: { temp: 20 }, timestamp: now - 1000000000 },
      { measurement, tags: { device: 'A', location: 'room1' }, fields: { temp: 30 }, timestamp: now },
      
      // Device B - values 40, 50, 60  
      { measurement, tags: { device: 'B', location: 'room1' }, fields: { temp: 40 }, timestamp: now - 2000000000 },
      { measurement, tags: { device: 'B', location: 'room1' }, fields: { temp: 50 }, timestamp: now - 1000000000 },
      { measurement, tags: { device: 'B', location: 'room1' }, fields: { temp: 60 }, timestamp: now },
      
      // Device C - values 70, 80, 90 (different location)
      { measurement, tags: { device: 'C', location: 'room2' }, fields: { temp: 70 }, timestamp: now - 2000000000 },
      { measurement, tags: { device: 'C', location: 'room2' }, fields: { temp: 80 }, timestamp: now - 1000000000 },
      { measurement, tags: { device: 'C', location: 'room2' }, fields: { temp: 90 }, timestamp: now }
    ]
  };
  
  console.log('1. Writing test data:');
  console.log('   Device A (room1): 10, 20, 30 -> avg = 20');
  console.log('   Device B (room1): 40, 50, 60 -> avg = 50');
  console.log('   Device C (room2): 70, 80, 90 -> avg = 80');
  
  const writeResp = await axios.post(`${BASE_URL}/write`, testData);
  console.log('   Write response:', writeResp.data);
  
  // Test 1: Query all devices
  console.log('\n2. Query AVG for all devices:');
  const query1 = {
    query: `avg:${measurement}(temp){}`,
    startTime: now - 3000000000,
    endTime: now + 1000000000
  };
  console.log('   Query:', JSON.stringify(query1, null, 2));
  
  const resp1 = await axios.post(`${BASE_URL}/query`, query1);
  console.log('   Response:', JSON.stringify(resp1.data, null, 2));
  
  if (resp1.data.series && resp1.data.series.length > 0) {
    const avgValue = resp1.data.series[0].fields.temp.values[0];
    const expectedAvg = (10+20+30+40+50+60+70+80+90) / 9; // 50
    console.log(`   Got AVG: ${avgValue}, Expected: ${expectedAvg}`);
    if (Math.abs(avgValue - expectedAvg) < 0.01) {
      console.log('   ✅ CORRECT!');
    } else {
      console.log('   ❌ INCORRECT!');
    }
  }
  
  // Test 2: Query with location filter
  console.log('\n3. Query AVG for room1 only:');
  const query2 = {
    query: `avg:${measurement}(temp){location:room1}`,
    startTime: now - 3000000000,
    endTime: now + 1000000000
  };
  console.log('   Query:', JSON.stringify(query2, null, 2));
  
  const resp2 = await axios.post(`${BASE_URL}/query`, query2);
  console.log('   Response:', JSON.stringify(resp2.data, null, 2));
  
  if (resp2.data.series && resp2.data.series.length > 0) {
    const avgValue = resp2.data.series[0].fields.temp.values[0];
    const expectedAvg = (10+20+30+40+50+60) / 6; // 35
    console.log(`   Got AVG: ${avgValue}, Expected: ${expectedAvg}`);
    if (Math.abs(avgValue - expectedAvg) < 0.01) {
      console.log('   ✅ CORRECT!');
    } else {
      console.log('   ❌ INCORRECT!');
      console.log('   This is the issue - scope filtering is not working correctly');
    }
  }
  
  // Test 3: Query raw data to understand what's happening
  console.log('\n4. Query raw data without aggregation:');
  const query3 = {
    query: `latest:${measurement}(temp){location:room1}`,
    startTime: now - 3000000000,
    endTime: now + 1000000000
  };
  
  const resp3 = await axios.post(`${BASE_URL}/query`, query3);
  console.log('   Raw data response:', JSON.stringify(resp3.data, null, 2));
}

debugAvgAggregation().catch(console.error);