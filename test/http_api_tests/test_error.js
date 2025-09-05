const axios = require('axios');
const BASE_URL = 'http://localhost:8086';

async function test() {
  try {
    const response = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:test.measurement{tag:value}',  // Missing field parentheses
      startTime: 0,
      endTime: 100
    });
    console.log('Unexpected success:', response.data);
  } catch (error) {
    if (error.response) {
      console.log('Error status:', error.response.status);
      console.log('Error data:', error.response.data);
    } else {
      console.log('Network error:', error.message);
    }
  }
}

test().catch(console.error);
