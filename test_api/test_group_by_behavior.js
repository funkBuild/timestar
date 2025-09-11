const axios = require('axios');

const BASE_URL = 'http://localhost:8086';
const now = Date.now() * 1000000; // Convert to nanoseconds

async function testGroupByBehavior() {
  console.log('=== Testing Group By Current Behavior ===\n');

  // Insert test data with multiple devices and paddocks like your example
  const measurement = 'soil.moisture.test';
  
  const insertData = [
    {
      measurement,
      tags: { deviceId: 'aaaaa', paddock: 'back-paddock' },
      fields: { value1: 1.0, value2: 2.0, value3: 3.0 },
      timestamp: now
    },
    {
      measurement,
      tags: { deviceId: 'bbbbb', paddock: 'back-paddock' },
      fields: { value1: 4.0, value2: 5.0, value3: 6.0 },
      timestamp: now
    },
    {
      measurement,
      tags: { deviceId: 'ccccc', paddock: 'front-paddock' },
      fields: { value1: 7.0, value2: 8.0, value3: 9.0 },
      timestamp: now
    }
  ];

  // Insert the data
  for (const data of insertData) {
    await axios.post(`${BASE_URL}/write`, data);
  }

  console.log('Inserted test data for 3 devices\n');

  // Test query with group by like in your example
  const queryResponse = await axios.post(`${BASE_URL}/query`, {
    query: `avg:${measurement}(value1,value2,value3){paddock:back-paddock} by {deviceId}`,
    startTime: now - 1000000000,
    endTime: now + 1000000000
  });

  console.log('Query: avg:soil.moisture.test(value1,value2,value3){paddock:back-paddock} by {deviceId}');
  console.log('Current Response Structure:');
  console.log(JSON.stringify(queryResponse.data, null, 2));

  console.log('\n=== Analysis vs Expected Format ===');
  console.log('Expected format from your example:');
  console.log('- queryResult.scope: [{ name: "paddock", value: "back-paddock" }]');
  console.log('- queryResult.series.length: 2 (aaaaa and bbbbb)');
  console.log('- series[0].groupTags: ["deviceId=aaaaa"]');
  console.log('- series[0].fields.value1: [array of values]');
  
  console.log('\nActual current format:');
  console.log('- response.scopes:', queryResponse.data.scopes);
  console.log('- response.series.length:', queryResponse.data.series.length);
  if (queryResponse.data.series.length > 0) {
    console.log('- series[0].tags:', queryResponse.data.series[0].tags);
    console.log('- series[0].fields:', Object.keys(queryResponse.data.series[0].fields));
  }
}

testGroupByBehavior().catch(console.error);