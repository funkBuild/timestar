const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testZeroWidthRange() {
  try {
    console.log('=== Testing Zero-Width Range Deletion ===\n');
    
    // Insert multi-field data (exactly like failing test)
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
    
    console.log('\n2. Querying before delete...');
    const queryBefore = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    console.log('Before delete:', JSON.stringify(queryBefore.data, null, 2));
    
    // Delete with ZERO-WIDTH range (startTime == endTime) - exactly like failing test
    console.log('\n3. Deleting with zero-width range...');
    const deleteRequest = {
      measurement: 'multi_sensor',
      tags: {
        device: 'sensor-02'
      },
      fields: ['temperature'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000000  // SAME VALUE = zero-width range
    };
    console.log('Delete request:', JSON.stringify(deleteRequest, null, 2));
    
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, deleteRequest);
    console.log('Delete response:', deleteResponse.data);
    
    console.log('\n4. Querying after delete...');
    const queryAfter = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    console.log('After delete:', JSON.stringify(queryAfter.data, null, 2));
    
    // Analyze results
    console.log('\n5. Analysis:');
    const series = queryAfter.data.series?.[0];
    if (series) {
      const tempTimestamps = series.fields.temperature?.timestamps || [];
      const humTimestamps = series.fields.humidity?.timestamps || [];
      const pressureTimestamps = series.fields.pressure?.timestamps || [];
      
      console.log(`Temperature timestamps: ${tempTimestamps.length}`);
      console.log(`Humidity timestamps: ${humTimestamps.length}`);
      console.log(`Pressure timestamps: ${pressureTimestamps.length}`);
      
      if (tempTimestamps.length === 0) {
        console.log('✅ Zero-width range DOES delete the exact timestamp');
      } else {
        console.log('❌ Zero-width range does NOT delete the exact timestamp');
        console.log('Temperature still present with values:', series.fields.temperature?.values);
      }
    } else {
      console.log('No series returned');
    }
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
}

testZeroWidthRange();