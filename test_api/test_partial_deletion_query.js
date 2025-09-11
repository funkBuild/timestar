const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testPartialDeletionQuery() {
    try {
        console.log('=== Testing Partial Deletion Query (Insert 3, Delete 1, Query All 3) ===');
        
        // Insert data with 3 fields 
        const insertData = {
            measurement: 'weather_delete',
            tags: {
                location: 'test-site',
                sensor: 'sensor-456'
            },
            fields: {
                temperature: 25.5,
                humidity: 60.0,
                pressure: 1013.25
            },
            timestamp: 1704067200000000000
        };
        
        console.log('1. Inserting test data with 3 fields (temperature, humidity, pressure)...');
        const insertResponse = await axios.post(`${BASE_URL}/write`, insertData);
        console.log('Insert response:', insertResponse.status, insertResponse.data);
        
        console.log('2. Verifying all 3 fields exist...');
        const initialQuery = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:weather_delete(temperature,humidity,pressure){location:test-site}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Initial query response:', initialQuery.status, JSON.stringify(initialQuery.data, null, 2));
        
        console.log('3. Deleting temperature field...');
        const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
            measurement: 'weather_delete',
            tags: {
                location: 'test-site',
                sensor: 'sensor-456'
            },
            fields: ['temperature'],
            startTime: 1704067200000000000,
            endTime: 1704067200000000000
        });
        console.log('Delete response:', deleteResponse.status, deleteResponse.data);
        
        console.log('4. Verifying temperature field was deleted (should fail or return empty)...');
        try {
            const tempQuery = await axios.post(`${BASE_URL}/query`, {
                query: 'avg:weather_delete(temperature){location:test-site}',
                startTime: 1704067200000000000,
                endTime: 1704067200000000001
            });
            console.log('Temperature query response:', tempQuery.status, JSON.stringify(tempQuery.data, null, 2));
            if (tempQuery.data.series && tempQuery.data.series.length > 0) {
                console.log('ERROR: Temperature field still exists after deletion');
            } else {
                console.log('SUCCESS: Temperature field correctly deleted (no series returned)');
            }
        } catch (error) {
            console.log('SUCCESS: Temperature field correctly deleted (query failed):', error.response?.status, error.response?.data);
        }
        
        console.log('5. Testing partial field query after deletion (query all 3, expect only 2)...');
        // This is the test case that should fail with current implementation
        try {
            const partialQueryResponse = await axios.post(`${BASE_URL}/query`, {
                query: 'avg:weather_delete(temperature,humidity,pressure){location:test-site}',
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
                
                // Check that we got humidity and pressure but not temperature
                const hasHumidity = fieldNames.includes('humidity');
                const hasPressure = fieldNames.includes('pressure');
                const hasTemperature = fieldNames.includes('temperature');
                
                if (hasHumidity && hasPressure && !hasTemperature) {
                    console.log('SUCCESS: Response correctly includes only existing fields (humidity, pressure)');
                } else if (hasTemperature) {
                    console.log('ERROR: Response incorrectly includes deleted temperature field');
                } else {
                    console.log('ERROR: Response missing expected fields. Has humidity:', hasHumidity, ', has pressure:', hasPressure);
                }
            } else {
                console.log('ERROR: Response has no series data - this is the bug!');
            }
            
        } catch (error) {
            console.log('FAILED: Partial field query failed with "Series not found" error - this is the bug we need to fix!');
            console.log('Error status:', error.response?.status);
            console.log('Error data:', error.response?.data);
            console.log('Expected behavior: Should return existing fields (humidity, pressure) and ignore deleted field (temperature)');
        }
        
        console.log('6. Control test - querying only existing fields should work...');
        const controlQuery = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:weather_delete(humidity,pressure){location:test-site}',
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

testPartialDeletionQuery();