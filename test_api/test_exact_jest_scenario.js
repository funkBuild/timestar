const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testExactJestScenario() {
  try {
    console.log('=== Testing Exact Jest Scenario ===');
    
    // Use EXACT same data as Jest test
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
    
    console.log('1. Inserting data...');
    const insertResponse = await axios.post(`${BASE_URL}/write`, multiFieldData);
    console.log('Insert response:', insertResponse.data);
    
    console.log('\n2. Deleting temperature field...');
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: 'multi_sensor',
      tags: {
        device: 'sensor-02'
      },
      fields: ['temperature'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000000
    });
    console.log('Delete response:', deleteResponse.data);
    
    console.log('\n3. Querying after delete...');
    const queryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    
    console.log('Query response:', JSON.stringify(queryResponse.data, null, 2));
    
    const series = queryResponse.data.series[0];
    
    console.log('\n4. Analysis:');
    console.log('Temperature field present:', !!series.fields.temperature);
    if (series.fields.temperature) {
      console.log('Temperature timestamps:', series.fields.temperature.timestamps.length);
      console.log('Temperature values:', series.fields.temperature.values);
      console.log('❌ FAIL: This matches the Jest test failure!');
    } else {
      console.log('✅ SUCCESS: Temperature field properly deleted');
    }
    
    console.log('Humidity timestamps:', series.fields.humidity.timestamps.length);
    console.log('Pressure timestamps:', series.fields.pressure.timestamps.length);
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
}

async function testTwiceInARow() {
  console.log('\n=== Running Test Twice to Check State Pollution ===\n');
  
  console.log('First run:');
  await testExactJestScenario();
  
  console.log('\n' + '='.repeat(50) + '\n');
  
  console.log('Second run (same measurement):');
  await testExactJestScenario();
}

testTwiceInARow();