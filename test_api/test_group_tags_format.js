const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

async function testGroupTagsFormat() {
  console.log('=== Testing Expected Group Tags Format ===\n');

  // Create test data with multiple tags for complex grouping
  const measurement = 'metrics.test';
  const now = Date.now() * 1000000;
  
  // Insert data with multiple tag dimensions
  const insertData = [
    {
      measurement,
      tags: { region: 'us-west', env: 'prod', host: 'server1' },
      fields: { cpu: 45.2, memory: 78.5 },
      timestamp: now
    },
    {
      measurement,
      tags: { region: 'us-west', env: 'prod', host: 'server2' },
      fields: { cpu: 52.1, memory: 82.3 },
      timestamp: now
    },
    {
      measurement,
      tags: { region: 'us-west', env: 'dev', host: 'server3' },
      fields: { cpu: 23.4, memory: 45.6 },
      timestamp: now
    },
    {
      measurement,
      tags: { region: 'us-east', env: 'prod', host: 'server4' },
      fields: { cpu: 67.8, memory: 91.2 },
      timestamp: now
    },
    {
      measurement,
      tags: { region: 'us-east', env: 'dev', host: 'server5' },
      fields: { cpu: 34.5, memory: 56.7 },
      timestamp: now
    }
  ];

  // Insert the data
  for (const data of insertData) {
    await axios.post(`${BASE_URL}/write`, data);
  }

  console.log('Inserted test data for 5 servers across regions and environments\n');

  // Test 1: Single group-by key
  console.log('--- TEST 1: Group by single key (region) ---');
  const query1 = `avg:${measurement}(cpu,memory){} by {region}`;
  const response1 = await axios.post(`${BASE_URL}/query`, {
    query: query1,
    startTime: now - 1000000000,
    endTime: now + 1000000000
  });

  console.log('Query:', query1);
  console.log('Current Response:');
  console.log(JSON.stringify(response1.data, null, 2));
  
  console.log('\nExpected format:');
  console.log('- Each series should have groupTags: ["region=us-west"] or ["region=us-east"]');
  console.log('- No top-level scopes array\n');

  // Test 2: Multiple group-by keys
  console.log('--- TEST 2: Group by multiple keys (region, env) ---');
  const query2 = `avg:${measurement}(cpu,memory){} by {region,env}`;
  const response2 = await axios.post(`${BASE_URL}/query`, {
    query: query2,
    startTime: now - 1000000000,
    endTime: now + 1000000000
  });

  console.log('Query:', query2);
  console.log('Current Response:');
  console.log(JSON.stringify(response2.data, null, 2));
  
  console.log('\nExpected format:');
  console.log('- Series 1: groupTags: ["region=us-west", "env=prod"]');
  console.log('- Series 2: groupTags: ["region=us-west", "env=dev"]');
  console.log('- Series 3: groupTags: ["region=us-east", "env=prod"]');
  console.log('- Series 4: groupTags: ["region=us-east", "env=dev"]\n');

  // Test 3: Group by with filter scope
  console.log('--- TEST 3: Group by with filter scope ---');
  const query3 = `avg:${measurement}(cpu,memory){region:us-west} by {env,host}`;
  const response3 = await axios.post(`${BASE_URL}/query`, {
    query: query3,
    startTime: now - 1000000000,
    endTime: now + 1000000000
  });

  console.log('Query:', query3);
  console.log('Current Response:');
  console.log(JSON.stringify(response3.data, null, 2));
  
  console.log('\nExpected format:');
  console.log('- Only us-west servers should be included');
  console.log('- Each series should have groupTags with env and host values');
  console.log('- Series 1: groupTags: ["env=prod", "host=server1"]');
  console.log('- Series 2: groupTags: ["env=prod", "host=server2"]');
  console.log('- Series 3: groupTags: ["env=dev", "host=server3"]\n');

  // Analyze the response structure
  console.log('=== ANALYSIS ===');
  console.log('Current implementation uses:');
  if (response2.data.series && response2.data.series.length > 0) {
    console.log('- series[].tags object:', JSON.stringify(response2.data.series[0].tags));
    if (response2.data.series[0].scopes) {
      console.log('- series[].scopes object:', JSON.stringify(response2.data.series[0].scopes));
    }
  }
  if (response2.data.scopes) {
    console.log('- Top-level scopes array:', JSON.stringify(response2.data.scopes));
  }
  
  console.log('\nDesired implementation should use:');
  console.log('- series[].groupTags array: ["key1=value1", "key2=value2", ...]');
  console.log('- No top-level scopes array');
  console.log('- Filter scopes should be separate from groupTags\n');

  // Show what the transformed response would look like
  console.log('=== PROPOSED TRANSFORMATION ===');
  if (response2.data.series && response2.data.series.length > 0) {
    const transformedSeries = response2.data.series.map(series => {
      // Convert tags object to groupTags array
      const groupTags = Object.entries(series.tags || {})
        .map(([key, value]) => `${key}=${value}`)
        .sort(); // Sort for consistent ordering
      
      // Create transformed series object
      return {
        measurement: series.measurement,
        groupTags: groupTags,
        fields: series.fields
        // Don't include scopes at series level
      };
    });

    const transformedResponse = {
      status: response2.data.status,
      series: transformedSeries,
      statistics: response2.data.statistics
      // No top-level scopes
    };

    console.log('Transformed response for TEST 2:');
    console.log(JSON.stringify(transformedResponse, null, 2));
  }
}

testGroupTagsFormat().catch(console.error);