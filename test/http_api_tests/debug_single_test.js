const axios = require('axios');

// Test configuration
const HOST = 'localhost';
const PORT = 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const writeMultiFieldData = async (measurement, tags, fields, timestamps) => {
  const writes = timestamps.map((ts, i) => {
    const fieldData = {};
    Object.keys(fields).forEach(field => {
      fieldData[field] = fields[field][i];
    });
    return {
      measurement,
      tags,
      fields: fieldData,
      timestamp: ts
    };
  });
  
  const response = await axios.post(`${BASE_URL}/write`, { writes });
  return response.data;
};

const query = async (queryStr, startTime = 0, endTime = 1000) => {
  const request = {
    query: queryStr,
    startTime,
    endTime
  };
  
  const response = await axios.post(`${BASE_URL}/query`, request);
  return response.data;
};

async function debugTest() {
  try {
    console.log('=== Debug Test: Field Prefix Issue ===');
    
    // Insert data with similar field names
    const timestamps = [1, 2, 3, 4, 5, 6];
    console.log('Writing data with timestamps:', timestamps);
    console.log('pnf values:', [0, 0, 0, 0, 0, 0]);
    console.log('pnf_status values:', [1, 1, 1, 1, 1, 1]);
    
    const writeResult = await writeMultiFieldData(
      'lid_data',
      { meter_id: '33616' },
      {
        pnf: [0, 0, 0, 0, 0, 0],
        pnf_status: [1, 1, 1, 1, 1, 1]
      },
      timestamps
    );
    console.log('Write result:', writeResult);
    
    // Query only pnf field
    console.log('\nQuerying: avg:lid_data(pnf){meter_id:33616}');
    const result = await query(`avg:lid_data(pnf){meter_id:33616}`, 0, 1000);
    
    console.log('\n=== Query Result ===');
    console.log('Status:', result.status);
    console.log('Series count:', result.series?.length || 0);
    console.log('Statistics:', result.statistics);
    
    if (result.series && result.series.length > 0) {
      result.series.forEach((series, i) => {
        console.log(`\nSeries ${i}:`);
        console.log('  Measurement:', series.measurement);
        console.log('  Tags:', series.tags);
        console.log('  Fields available:', Object.keys(series.fields));
        
        if (series.fields.pnf) {
          console.log('  pnf timestamps:', series.fields.pnf.timestamps);
          console.log('  pnf values:', series.fields.pnf.values);
          console.log('  pnf value count:', series.fields.pnf.values.length);
        }
        
        if (series.fields.pnf_status) {
          console.log('  pnf_status timestamps:', series.fields.pnf_status.timestamps);
          console.log('  pnf_status values:', series.fields.pnf_status.values);
        }
      });
    }
    
    if (result.error) {
      console.log('Error:', result.error);
    }
    
  } catch (error) {
    console.error('Test failed:', error.message);
    if (error.response) {
      console.error('Response data:', error.response.data);
    }
  }
}

debugTest();