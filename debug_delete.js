#!/usr/bin/env node

const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function testDeleteOperation() {
    console.log('Testing delete operation...');
    
    try {
        // Test 1: Insert initial data
        console.log('Step 1: Inserting test data...');
        const insertResponse = await axios.post(`${BASE_URL}/write`, {
            measurement: 'debug_test',
            tags: { device: 'sensor1' },
            fields: { temp: 25.0 },
            timestamp: 1704067200000000000
        });
        console.log('Insert response:', insertResponse.data);
        
        // Test 2: Query to verify data exists
        console.log('\nStep 2: Querying data...');
        const queryResponse = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:debug_test(temp){device:sensor1}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Query response:', JSON.stringify(queryResponse.data, null, 2));
        
        // Test 3: Try to delete data
        console.log('\nStep 3: Attempting delete...');
        const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
            measurement: 'debug_test',
            tags: { device: 'sensor1' },
            field: 'temp',
            startTime: 1704067200000000000,
            endTime: 1704067200000000000
        });
        console.log('Delete response:', deleteResponse.data);
        
        // Test 4: Query after delete to verify
        console.log('\nStep 4: Querying after delete...');
        const queryAfterDeleteResponse = await axios.post(`${BASE_URL}/query`, {
            query: 'avg:debug_test(temp){device:sensor1}',
            startTime: 1704067200000000000,
            endTime: 1704067200000000001
        });
        console.log('Query after delete response:', JSON.stringify(queryAfterDeleteResponse.data, null, 2));
        
    } catch (error) {
        console.error('Error:', error.response ? error.response.data : error.message);
    }
}

testDeleteOperation();