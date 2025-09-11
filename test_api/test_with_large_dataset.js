const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function insertLargeDataset() {
  console.log('=== Inserting Large Dataset (like Jest test) ===');
  
  // Generate the same test data as Jest beforeAll
  const startTime = 1704067200000000000; // Jan 1, 2024 00:00:00 in nanoseconds
  const hourInNanos = 3600000000000; // 1 hour in nanoseconds
  
  const testData = [];
  for (let i = 0; i < 72; i++) { // 72 hours = 3 days
    testData.push({
      measurement: 'sensor_data',
      tags: {
        location: 'warehouse-1',
        sensor_id: 'temp-sensor-01'
      },
      fields: {
        temperature: 20.0 + Math.sin(i / 12 * Math.PI) * 5, // Sine wave between 15-25°C
        humidity: 50.0 + Math.cos(i / 12 * Math.PI) * 10 // Cosine wave between 40-60%
      },
      timestamp: startTime + (i * hourInNanos)
    });
  }
  
  console.log('Inserting', testData.length, 'data points...');
  
  const insertResponse = await axios.post(`${BASE_URL}/write`, {
    writes: testData
  });
  
  console.log('Insert response:', insertResponse.data);
  return testData;
}

async function testPartialDeletionAfterLargeInsert() {
  try {
    // First, insert the large dataset (like Jest does)
    const largeDataset = await insertLargeDataset();
    
    console.log('\n=== Now Testing Partial Deletion ===');
    
    // Insert the multi-field data for partial deletion test
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
    
    console.log('1. Inserting multi-field data...');
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
    
    const series = queryResponse.data.series[0];
    
    console.log('\n4. Analysis:');
    console.log('Temperature field present:', !!series.fields.temperature);
    if (series.fields.temperature) {
      console.log('Temperature timestamps:', series.fields.temperature.timestamps.length);
      console.log('❌ FAIL: Large dataset caused the issue!');
    } else {
      console.log('✅ SUCCESS: Temperature field properly deleted even with large dataset');
    }
    
    console.log('Humidity timestamps:', series.fields.humidity?.timestamps?.length || 0);
    console.log('Pressure timestamps:', series.fields.pressure?.timestamps?.length || 0);
    
  } catch (error) {
    console.error('Error:', error.response?.data || error.message);
  }
}

testPartialDeletionAfterLargeInsert();