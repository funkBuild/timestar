const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testPartialFieldQuery() {
    try {
        console.log('=== Testing Partial Field Query (2 existing + 1 missing) ===');
        
        // Insert data with only 2 fields (temperature and humidity)
        const insertData = {
            measurement: 'weather_partial',
            tags: {
                location: 'test-site',
                sensor: 'sensor-123'
            },
            fields: {
                temperature: 25.5,
                humidity: 60.0
                // Note: no pressure field inserted
            },
            timestamp: 1704067200000000000
        };
        
        console.log('1. Inserting test data with 2 fields (temperature, humidity)...');
        const insertResponse = await axios.post(`${BASE_URL}/write`, insertData);
        console.log('Insert response:', insertResponse.status, insertResponse.data);
        
        console.log('2. Verifying initial data exists...');
        const initialQuery = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:weather_partial(temperature,humidity){location:test-site}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Initial query response:', initialQuery.status, JSON.stringify(initialQuery.data, null, 2));
        
        console.log('3. Testing partial field query (2 existing + 1 missing field)...');
        // This should return only temperature and humidity, not fail with "Series not found"
        try {
            const partialQueryResponse = await axios.post(`${BASE_URL}/query`, {
                query: 'avg:weather_partial(temperature,humidity,pressure){location:test-site}',
                startTime: 1704067200000000000,
                endTime: 1704067200000000001
            });
            console.log('SUCCESS: Partial field query response:', partialQueryResponse.status, JSON.stringify(partialQueryResponse.data, null, 2));
            
            // Verify the response contains only the existing fields
            const series = partialQueryResponse.data.series;
            if (series && series.length > 0) {
                const fields = series[0].fields;
                const fieldNames = Object.keys(fields);
                console.log('Returned field names:', fieldNames);
                
                // Check that we got temperature and humidity but not pressure
                if (fieldNames.includes('temperature') && fieldNames.includes('humidity')) {
                    if (fieldNames.includes('pressure')) {
                        console.log('ERROR: Response incorrectly includes missing pressure field');
                    } else {
                        console.log('SUCCESS: Response correctly includes only existing fields (temperature, humidity)');
                    }
                } else {
                    console.log('ERROR: Response missing expected fields (temperature, humidity)');
                }
            } else {
                console.log('ERROR: Response has no series data');
            }
            
        } catch (error) {
            console.log('FAILED: Partial field query failed (this is the bug we want to fix)');
            console.log('Error status:', error.response?.status);
            console.log('Error data:', error.response?.data);
            console.log('Expected behavior: Should return existing fields (temperature, humidity) and ignore missing field (pressure)');
        }
        
        console.log('4. Control test - querying only existing fields should work...');
        const controlQuery = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:weather_partial(temperature,humidity){location:test-site}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Control query response:', controlQuery.status, JSON.stringify(controlQuery.data, null, 2));
        
    } catch (error) {
        console.error('Test setup error:');
        console.error('Status:', error.response?.status);
        console.error('Data:', error.response?.data);
        console.error('Message:', error.message);
        if (error.response?.data) {
            console.error('Full error response:', JSON.stringify(error.response.data, null, 2));
        }
        process.exit(1);
    }
}

testPartialFieldQuery();