const crypto = require('crypto');
const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

function calculateShard(seriesKey, numShards = 32) {
    // Simulate the same hash function used by the C++ code
    const hash = crypto.createHash('md5').update(seriesKey).digest();
    const hashValue = hash.readUInt32BE(0);
    return hashValue % numShards;
}

async function debugSharding() {
    try {
        console.log('=== Debug Sharding and Series Keys ===');
        
        const seriesKeys = [
            'measurement_a,location=room1 field1',
            'measurement_a,location=room1 field2',
            'measurement_b,location=room2 field1', 
            'measurement_b,location=room2 field2'
        ];
        
        console.log('1. Series key to shard mapping:');
        for (const key of seriesKeys) {
            const shard = calculateShard(key);
            console.log(`  "${key}" -> Shard ${shard}`);
        }
        
        console.log('\n2. Testing if the issue is shard-specific...');
        
        // Insert data
        await axios.post(`${BASE_URL}/write`, {
            measurement: 'shard_test',
            tags: { zone: 'test' },
            fields: { 
                alpha: 10.0, 
                beta: 20.0,
                gamma: 30.0  
            },
            timestamp: 1704067600000000000
        });
        
        const testSeriesKeys = [
            'shard_test,zone=test alpha',
            'shard_test,zone=test beta', 
            'shard_test,zone=test gamma'
        ];
        
        console.log('\n3. Series keys and their shards:');
        for (const key of testSeriesKeys) {
            const shard = calculateShard(key);
            console.log(`  "${key}" -> Shard ${shard}`);
        }
        
        console.log('\n4. Testing queries before deletion...');
        for (const field of ['alpha', 'beta', 'gamma']) {
            try {
                await axios.post(`${BASE_URL}/query`, {
                    query: `avg:shard_test(${field}){zone:test}`,
                    startTime: 1704067600000000000,
                    endTime: 1704067600000000001
                });
                console.log(`  ${field}: SUCCESS`);
            } catch (error) {
                console.log(`  ${field}: FAILED (${error.response?.status})`);
            }
        }
        
        // Delete just alpha
        console.log('\n5. Deleting ONLY alpha field...');
        await axios.post(`${BASE_URL}/delete`, {
            measurement: 'shard_test',
            tags: { zone: 'test' },
            fields: ['alpha'],
            startTime: 1704067600000000000,
            endTime: 1704067600000000000
        });
        
        console.log('\n6. Testing queries after deletion...');
        for (const field of ['alpha', 'beta', 'gamma']) {
            try {
                await axios.post(`${BASE_URL}/query`, {
                    query: `avg:shard_test(${field}){zone:test}`,
                    startTime: 1704067600000000000,
                    endTime: 1704067600000000001
                });
                console.log(`  ${field}: SUCCESS`);
            } catch (error) {
                console.log(`  ${field}: FAILED (${error.response?.status})`);
            }
        }
        
    } catch (error) {
        console.error('Test error:', error.message);
        process.exit(1);
    }
}

debugSharding();