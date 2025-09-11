const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function debugMemoryStores() {
    try {
        console.log('=== Debug Memory Store Behavior ===');
        
        // Test with completely separate measurements to rule out cross-contamination
        console.log('1. Inserting data into measurement A...');
        await axios.post(`${BASE_URL}/write`, {
            measurement: 'measurement_a',
            tags: { location: 'room1' },
            fields: { field1: 10.0, field2: 20.0 },
            timestamp: 1704067500000000000
        });
        
        console.log('2. Inserting data into measurement B...');
        await axios.post(`${BASE_URL}/write`, {
            measurement: 'measurement_b', 
            tags: { location: 'room2' },
            fields: { field1: 30.0, field2: 40.0 },
            timestamp: 1704067500000000000
        });
        
        console.log('3. Testing queries before any deletion...');
        const testQueries = [
            'avg:measurement_a(field1){location:room1}',
            'avg:measurement_a(field2){location:room1}', 
            'avg:measurement_b(field1){location:room2}',
            'avg:measurement_b(field2){location:room2}'
        ];
        
        for (const query of testQueries) {
            try {
                await axios.post(`${BASE_URL}/query`, {
                    query: query,
                    startTime: 1704067500000000000,
                    endTime: 1704067500000000001
                });
                console.log(`  ${query}: SUCCESS`);
            } catch (error) {
                console.log(`  ${query}: FAILED (${error.response?.status})`);
            }
        }
        
        console.log('4. Deleting ONE field from measurement A...');
        await axios.post(`${BASE_URL}/delete`, {
            measurement: 'measurement_a',
            tags: { location: 'room1' },
            fields: ['field1'],
            startTime: 1704067500000000000,
            endTime: 1704067500000000000
        });
        
        console.log('5. Testing all queries after deletion...');
        for (const query of testQueries) {
            try {
                await axios.post(`${BASE_URL}/query`, {
                    query: query,
                    startTime: 1704067500000000000,
                    endTime: 1704067500000000001
                });
                console.log(`  ${query}: SUCCESS`);
            } catch (error) {
                console.log(`  ${query}: FAILED (${error.response?.status})`);
            }
        }
        
        console.log('\nExpected results:');
        console.log('  measurement_a field1: FAILED (deleted)');
        console.log('  measurement_a field2: SUCCESS (should remain)');
        console.log('  measurement_b field1: SUCCESS (different measurement)');
        console.log('  measurement_b field2: SUCCESS (different measurement)');
        
    } catch (error) {
        console.error('Test error:', error.message);
        process.exit(1);
    }
}

debugMemoryStores();