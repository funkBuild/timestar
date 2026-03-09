const axios = require('axios');

const HOST = process.env.TIMESTAR_HOST || 'localhost';
const PORT = process.env.TIMESTAR_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

const testMeasurement = `test_types_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000;

async function testDataTypes() {
  console.log('=== Testing Boolean and String Data Types ===\n');
  
  // Test Boolean data
  console.log('--- Writing Boolean Data ---');
  const boolWrites = [
    {
      measurement: `${testMeasurement}.boolean`,
      tags: { deviceId: 'sensor' },
      fields: { value: true },
      timestamp: now - 2000000000
    },
    {
      measurement: `${testMeasurement}.boolean`,
      tags: { deviceId: 'sensor' },
      fields: { value: false },
      timestamp: now - 1000000000
    }
  ];
  
  try {
    const boolResp = await axios.post(`${BASE_URL}/write`, { writes: boolWrites });
    console.log('Boolean write response:', boolResp.data);
  } catch (e) {
    console.log('Boolean write error:', e.response?.data || e.message);
  }
  
  // Test String/Image data
  console.log('\n--- Writing String/Image Data ---');
  const stringWrites = [
    {
      measurement: `${testMeasurement}.images`,
      tags: { deviceId: 'camera' },
      fields: { image: 'ref::image1::s3://bucket/image1.jpeg' },
      timestamp: now - 2000000000
    },
    {
      measurement: `${testMeasurement}.images`,
      tags: { deviceId: 'camera' },
      fields: { image: 'ref::image2::s3://bucket/image2.jpeg' },
      timestamp: now - 1000000000
    }
  ];
  
  try {
    const strResp = await axios.post(`${BASE_URL}/write`, { writes: stringWrites });
    console.log('String write response:', strResp.data);
  } catch (e) {
    console.log('String write error:', e.response?.data || e.message);
  }
  
  // Query Boolean data
  console.log('\n--- Querying Boolean Data ---');
  try {
    const boolQuery = await axios.post(`${BASE_URL}/query`, {
      query: `latest:${testMeasurement}.boolean(){}`,
      startTime: now - 3000000000,
      endTime: now
    });
    
    console.log('Boolean query response:');
    console.log('  Status:', boolQuery.data.status);
    console.log('  Series count:', boolQuery.data.series?.length);
    
    if (boolQuery.data.series?.length > 0) {
      const series = boolQuery.data.series[0];
      console.log('  Fields:', Object.keys(series.fields || {}));
      console.log('  Value field:', series.fields?.value);
      
      if (series.fields?.value) {
        console.log('    Timestamps:', series.fields.value.timestamps);
        console.log('    Values:', series.fields.value.values);
        console.log('    Value types:', series.fields.value.values?.map(v => typeof v));
      }
    }
  } catch (e) {
    console.log('Boolean query error:', e.response?.data || e.message);
  }
  
  // Query String data
  console.log('\n--- Querying String/Image Data ---');
  try {
    const strQuery = await axios.post(`${BASE_URL}/query`, {
      query: `latest:${testMeasurement}.images(){}`,
      startTime: now - 3000000000,
      endTime: now
    });
    
    console.log('String query response:');
    console.log('  Status:', strQuery.data.status);
    console.log('  Series count:', strQuery.data.series?.length);
    
    if (strQuery.data.series?.length > 0) {
      const series = strQuery.data.series[0];
      console.log('  Fields:', Object.keys(series.fields || {}));
      console.log('  String fields:', Object.keys(series.string_fields || {}));
      console.log('  Image field:', series.fields?.image);
      
      if (series.fields?.image) {
        console.log('    Timestamps:', series.fields.image.timestamps);
        console.log('    Values:', series.fields.image.values);
      }
    }
  } catch (e) {
    console.log('String query error:', e.response?.data || e.message);
  }
}

testDataTypes().catch(console.error);