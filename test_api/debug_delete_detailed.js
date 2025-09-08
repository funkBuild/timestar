const axios = require('axios');
const BASE_URL = 'http://localhost:8086';

async function testDelete() {
  console.log('=== Debug Delete and Reinsert Test ===\n');
  
  const measurement = 'debug_del_' + Date.now();
  
  // Create test data with 10 points
  const data = [];
  for (let i = 0; i < 10; i++) {
    data.push({
      measurement,
      tags: { device: 'test' },
      fields: { value: i * 10 },
      timestamp: 1000000000 + i
    });
  }
  
  // Insert data
  console.log('1. Inserting 10 data points...');
  const insertResp = await axios.post(`${BASE_URL}/write`, { writes: data });
  console.log('   Insert response:', insertResp.data);
  
  // Query with LATEST (no aggregation)
  console.log('\n2. Query with LATEST (no aggregation):');
  const query1 = await axios.post(`${BASE_URL}/query`, {
    query: `latest:${measurement}(value){device:test}`,
    startTime: 1000000000,
    endTime: 1000000010
  });
  console.log('   Points found:', query1.data.series[0].fields.value.timestamps.length);
  console.log('   Timestamps:', query1.data.series[0].fields.value.timestamps);
  
  // Query with AVG (aggregation)
  console.log('\n3. Query with AVG (aggregation):');
  const query2 = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${measurement}(value){device:test}`,
    startTime: 1000000000,
    endTime: 1000000010
  });
  console.log('   Points found:', query2.data.series[0].fields.value.timestamps.length);
  console.log('   Timestamps:', query2.data.series[0].fields.value.timestamps);
  console.log('   Values:', query2.data.series[0].fields.value.values);
  
  // Delete middle 4 points
  console.log('\n4. Deleting middle 4 points (timestamps 3-6)...');
  const deleteResp = await axios.post(`${BASE_URL}/delete`, {
    measurement,
    tags: { device: 'test' },
    fields: ['value'],
    startTime: 1000000003,
    endTime: 1000000006
  });
  console.log('   Delete response:', deleteResp.data);
  
  // Query after delete with AVG
  console.log('\n5. Query after delete with AVG:');
  const query3 = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${measurement}(value){device:test}`,
    startTime: 1000000000,
    endTime: 1000000010
  });
  console.log('   Points found:', query3.data.series[0].fields.value.timestamps.length);
  console.log('   Timestamps:', query3.data.series[0].fields.value.timestamps);
  console.log('   Values:', query3.data.series[0].fields.value.values);
  
  // Reinsert deleted data
  console.log('\n6. Reinserting deleted data...');
  const reinsertData = data.slice(3, 7); // The 4 deleted points
  const reinsertResp = await axios.post(`${BASE_URL}/write`, { writes: reinsertData });
  console.log('   Reinsert response:', reinsertResp.data);
  
  // Final query with AVG
  console.log('\n7. Final query after reinsertion with AVG:');
  const query4 = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${measurement}(value){device:test}`,
    startTime: 1000000000,
    endTime: 1000000010
  });
  console.log('   Points found:', query4.data.series[0].fields.value.timestamps.length);
  console.log('   Timestamps:', query4.data.series[0].fields.value.timestamps);
  console.log('   Values:', query4.data.series[0].fields.value.values);
  
  if (query4.data.series[0].fields.value.timestamps.length === 10) {
    console.log('\n✅ SUCCESS: All 10 points present after reinsertion');
  } else {
    console.log('\n❌ FAILURE: Expected 10 points but got', query4.data.series[0].fields.value.timestamps.length);
  }
}

testDelete().catch(console.error);