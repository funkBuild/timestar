const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testPartialDelete() {
  try {
    console.log('1. Writing multi-field data...');
    const writeData = {
      measurement: 'multi_sensor',
      tags: { device: 'sensor-02' },
      fields: {
        temperature: 22.5,
        humidity: 55.0,
        pressure: 1013.25
      },
      timestamp: 1704067200000000000
    };
    
    const writeResponse = await axios.post(`${BASE_URL}/write`, writeData);
    console.log('Write response:', writeResponse.data);
    
    console.log('\n2. Querying all fields before delete...');
    const queryBefore = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    console.log('Query before delete:', JSON.stringify(queryBefore.data, null, 2));
    
    console.log('\n3. Deleting only temperature field...');
    const deleteRequest = {
      measurement: 'multi_sensor',
      tags: { device: 'sensor-02' },
      fields: ['temperature'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    };
    console.log('Delete request:', JSON.stringify(deleteRequest, null, 2));
    
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, deleteRequest);
    console.log('Delete response:', deleteResponse.data);
    
    console.log('\n4. Querying all fields after delete...');
    const queryAfter = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    console.log('Query after delete:', JSON.stringify(queryAfter.data, null, 2));
    
    // Check results
    console.log('\n5. Checking results...');
    const series = queryAfter.data.series?.[0];
    if (series) {
      if (series.fields.temperature) {
        const tempCount = series.fields.temperature.timestamps?.length || 0;
        console.log(`Temperature field: ${tempCount} points (should be 0)`);
        if (tempCount > 0) {
          console.log('❌ FAIL: Temperature should be deleted!');
        }
      } else {
        console.log('Temperature field: missing (correct!)');
      }
      
      const humidityCount = series.fields.humidity?.timestamps?.length || 0;
      const pressureCount = series.fields.pressure?.timestamps?.length || 0;
      console.log(`Humidity field: ${humidityCount} points (should be 1)`);
      console.log(`Pressure field: ${pressureCount} points (should be 1)`);
      
      if (humidityCount === 1 && pressureCount === 1 && 
          (!series.fields.temperature || series.fields.temperature.timestamps?.length === 0)) {
        console.log('✅ PARTIAL DELETE WORKS!');
      } else {
        console.log('❌ PARTIAL DELETE FAILED!');
      }
    } else {
      console.log('❌ No series returned after delete!');
    }
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
}

testPartialDelete();