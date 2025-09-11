const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testArrayFormat() {
  console.log('Testing array format...');
  
  // Test the array format that benchmark uses
  const arrayFormatData = {
    measurement: "test_measurement",
    tags: {
      host: "host-01",
      rack: "rack-1"
    },
    fields: {
      cpu_usage: [20.5, 25.3, 30.1],
      memory_usage: [40.2, 45.8, 50.4]
    },
    timestamps: [1000000000, 1000001000, 1000002000]
  };

  try {
    console.log('Sending array format request...');
    const response = await axios.post(`${BASE_URL}/write`, arrayFormatData);
    console.log('✓ Array format succeeded:', response.data);
  } catch (error) {
    console.error('✗ Array format failed:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
  }
}

async function testBatchFormat() {
  console.log('\nTesting batch format...');
  
  // Test the batch format that works
  const batchFormatData = {
    writes: [
      {
        measurement: "test_measurement",
        tags: { host: "host-01", rack: "rack-1" },
        fields: { cpu_usage: 20.5, memory_usage: 40.2 },
        timestamp: 1000000000
      },
      {
        measurement: "test_measurement", 
        tags: { host: "host-01", rack: "rack-1" },
        fields: { cpu_usage: 25.3, memory_usage: 45.8 },
        timestamp: 1000001000
      },
      {
        measurement: "test_measurement",
        tags: { host: "host-01", rack: "rack-1" }, 
        fields: { cpu_usage: 30.1, memory_usage: 50.4 },
        timestamp: 1000002000
      }
    ]
  };

  try {
    console.log('Sending batch format request...');
    const response = await axios.post(`${BASE_URL}/write`, batchFormatData);
    console.log('✓ Batch format succeeded:', response.data);
  } catch (error) {
    console.error('✗ Batch format failed:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
  }
}

async function main() {
  // Test server health first
  try {
    await axios.get(`${BASE_URL}/health`);
    console.log('✓ Server is healthy\n');
  } catch (error) {
    console.error('✗ Server health check failed:', error.message);
    process.exit(1);
  }

  await testArrayFormat();
  await testBatchFormat();
}

main().catch(console.error);