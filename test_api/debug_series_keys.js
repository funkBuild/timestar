const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function debugSeriesKeys() {
    try {
        console.log('=== Debug Series Keys After Deletion ===');
        
        // Insert data with 3 fields 
        const insertData = {
            measurement: 'debug_keys',
            tags: {
                location: 'lab-01',
                sensor: 'temp-sensor'
            },
            fields: {
                temperature: 25.5,
                humidity: 60.0,
                pressure: 1013.25
            },
            timestamp: 1704067400000000000
        };
        
        console.log('1. Inserting test data with 3 fields...');
        const insertResponse = await axios.post(`${BASE_URL}/write`, insertData);
        console.log('Insert response:', insertResponse.status, insertResponse.data);
        
        console.log('2. Testing individual field queries BEFORE deletion...');
        
        // Test each field individually
        const fields = ['temperature', 'humidity', 'pressure'];
        for (const field of fields) {
            try {
                const response = await axios.post(`${BASE_URL}/query`, {
                    query: `avg:debug_keys(${field}){location:lab-01}`,
                    startTime: 1704067400000000000,
                    endTime: 1704067400000000001
                });
                console.log(`  ${field}: SUCCESS (${response.status})`);
            } catch (error) {
                console.log(`  ${field}: FAILED (${error.response?.status})`);
            }
        }
        
        console.log('3. Deleting temperature field...');
        const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
            measurement: 'debug_keys',
            tags: {
                location: 'lab-01',
                sensor: 'temp-sensor'
            },
            fields: ['temperature'],
            startTime: 1704067400000000000,
            endTime: 1704067400000000000
        });
        console.log('Delete response:', deleteResponse.status, deleteResponse.data);
        
        console.log('4. Testing individual field queries AFTER deletion...');
        
        // Test each field individually after deletion
        for (const field of fields) {
            try {
                const response = await axios.post(`${BASE_URL}/query`, {
                    query: `avg:debug_keys(${field}){location:lab-01}`,
                    startTime: 1704067400000000000,
                    endTime: 1704067400000000001
                });
                console.log(`  ${field}: SUCCESS (${response.status}) - ${response.data.statistics.point_count} points`);
            } catch (error) {
                console.log(`  ${field}: FAILED (${error.response?.status}) - ${error.response?.data?.message}`);
            }
        }
        
        console.log('5. Testing multi-field query after deletion...');
        try {
            const response = await axios.post(`${BASE_URL}/query`, {
                query: 'avg:debug_keys(temperature,humidity,pressure){location:lab-01}',
                startTime: 1704067400000000000,
                endTime: 1704067400000000001
            });
            console.log('Multi-field query: SUCCESS', response.status);
            console.log('Fields returned:', Object.keys(response.data.series[0]?.fields || {}));
        } catch (error) {
            console.log('Multi-field query: FAILED', error.response?.status, error.response?.data?.message);
        }
        
    } catch (error) {
        console.error('Test error:', error.message);
        process.exit(1);
    }
}

debugSeriesKeys();