const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testLargeArrayFormat() {
  console.log('Testing large array format (1000 points)...');
  
  const size = 1000;
  const fields = {};
  const timestamps = [];
  
  // Create arrays for 10 fields (like the benchmark)
  const fieldNames = [
    "cpu_usage", "memory_usage", "disk_io_read", "disk_io_write",
    "network_in", "network_out", "load_avg_1m", "load_avg_5m", 
    "load_avg_15m", "temperature"
  ];
  
  fieldNames.forEach(field => {
    fields[field] = [];
  });
  
  // Generate 1000 data points
  for (let i = 0; i < size; i++) {
    timestamps.push(1000000000 + i * 1000000); // 1 second intervals
    
    fieldNames.forEach(field => {
      let value;
      switch (field) {
        case "cpu_usage":
          value = 20 + Math.random() * 60;
          break;
        case "memory_usage":
          value = 30 + Math.random() * 50;
          break;
        case "disk_io_read":
        case "disk_io_write":
          value = Math.random() * 100;
          break;
        case "network_in":
        case "network_out":
          value = Math.random() * 1000;
          break;
        case "load_avg_1m":
        case "load_avg_5m":
        case "load_avg_15m":
          value = Math.random() * 4;
          break;
        case "temperature":
          value = 50 + Math.random() * 30;
          break;
        default:
          value = Math.random() * 100;
      }
      fields[field].push(value);
    });
  }
  
  const arrayFormatData = {
    measurement: "server.metrics",
    tags: {
      host: "host-01",
      rack: "rack-1"
    },
    fields: fields,
    timestamps: timestamps
  };

  try {
    console.log('Sending large array format request...');
    const start = Date.now();
    const response = await axios.post(`${BASE_URL}/write`, arrayFormatData);
    const end = Date.now();
    console.log(`✓ Large array format succeeded in ${end - start}ms:`, response.data);
  } catch (error) {
    console.error('✗ Large array format failed:', error.message);
    if (error.response) {
      console.error('Response status:', error.response.status);
      console.error('Response data:', error.response.data);
    }
    process.exit(1);
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

  await testLargeArrayFormat();
}

main().catch(console.error);