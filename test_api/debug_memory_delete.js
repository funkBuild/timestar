const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testMemoryDelete() {
    try {
        console.log('=== Testing Memory Store Delete Implementation ===');
        
        // Insert multi-field data
        const insertData = {
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
        
        console.log('1. Inserting test data...');
        const insertResponse = await axios.post(`${BASE_URL}/write`, insertData);
        console.log('Insert response:', insertResponse.status, insertResponse.data);
        
        console.log('2. Verifying initial data...');
        const initialQuery = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Initial query response:', initialQuery.status, JSON.stringify(initialQuery.data, null, 2));
        
        console.log('3. Deleting temperature field...');
        const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
            measurement: 'multi_sensor',
            tags: {
                device: 'sensor-02'
            },
            fields: ['temperature'],
            startTime: 1704067200000000000,
            endTime: 1704067200000000000
        });
        console.log('Delete response:', deleteResponse.status, deleteResponse.data);
        
        console.log('4. Querying after deletion (only remaining fields)...');
        const queryResponse = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:multi_sensor(humidity,pressure){device:sensor-02}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Query after deletion (remaining fields):', queryResponse.status, JSON.stringify(queryResponse.data, null, 2));
        
        console.log('5. Trying to query deleted field (should fail)...');
        try {
            const tempQueryResponse = await axios.post(`${BASE_URL}/query`, {
                query: 'avg:multi_sensor(temperature){device:sensor-02}',
                startTime: 1704067200000000000,
                endTime: 1704067200000000001
            });
            console.log('Query deleted temperature field:', tempQueryResponse.status, JSON.stringify(tempQueryResponse.data, null, 2));
        } catch (error) {
            console.log('Query deleted temperature field failed (expected):', error.response?.status, error.response?.data);
        }
        
        console.log('6. Trying to query all fields including deleted (should only return existing)...');
        try {
            const allFieldsResponse = await axios.post(`${BASE_URL}/query`, {
                query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
                startTime: 1704067200000000000,
                endTime: 1704067200000000001
            });
            console.log('Query all fields:', allFieldsResponse.status, JSON.stringify(allFieldsResponse.data, null, 2));
        } catch (error) {
            console.log('Query all fields failed:', error.response?.status, error.response?.data);
        }
        
    } catch (error) {
        console.error('Error details:');
        console.error('Status:', error.response?.status);
        console.error('Data:', error.response?.data);
        console.error('Message:', error.message);
        if (error.response?.data) {
            console.error('Full error response:', JSON.stringify(error.response.data, null, 2));
        }
        process.exit(1);
    }
}

testMemoryDelete();