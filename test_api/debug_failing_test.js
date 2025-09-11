const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testFailingScenario() {
  try {
    console.log('=== Testing Exact Failing Test Scenario ===\n');
    
    // Step 1: Insert multi-field data (EXACTLY like failing test)
    console.log('1. Writing multi-field data...');
    const multiFieldData = {
      measurement: 'multi_sensor',
      tags: {
        device: 'sensor-02'
      },
      fields: {
        temperature: 22.5,
        humidity: 55.0,
        pressure: 1013.25
      },
      timestamp: 1704067200000000000
    };
    
    const insertResponse = await axios.post(`${BASE_URL}/write`, multiFieldData);
    console.log('Insert response:', insertResponse.data);
    
    // Step 2: Delete only temperature field (EXACTLY like failing test)
    console.log('\n2. Deleting temperature field...');
    const deleteRequest = {
      measurement: 'multi_sensor',
      tags: {
        device: 'sensor-02'
      },
      fields: ['temperature'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000000
    };
    
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, deleteRequest);
    console.log('Delete response:', deleteResponse.data);
    
    // Step 3: Query and verify (EXACTLY like failing test)
    console.log('\n3. Querying after delete...');
    const queryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    
    console.log('Query response:', JSON.stringify(queryResponse.data, null, 2));
    
    // Step 4: Analyze results
    console.log('\n4. Analysis:');
    const series = queryResponse.data.series[0];
    
    console.log('Temperature field present:', !!series.fields.temperature);
    if (series.fields.temperature) {
      console.log('Temperature timestamps:', series.fields.temperature.timestamps.length);
      console.log('Temperature values:', series.fields.temperature.values);
    }
    
    console.log('Humidity timestamps:', series.fields.humidity.timestamps.length);
    console.log('Pressure timestamps:', series.fields.pressure.timestamps.length);
    
    // This should match the Jest test expectation
    if (series.fields.temperature && series.fields.temperature.timestamps.length > 0) {
      console.log('❌ FAILURE: Temperature field still has timestamps (should be 0)');
      console.log('This matches the Jest test failure');
    } else {
      console.log('✅ SUCCESS: Temperature field properly deleted');
    }
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
}

testFailingScenario();