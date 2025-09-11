const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

async function debugExactIssue() {
    try {
        console.log('=== Debug Exact Issue - Minimal Reproduction ===');
        
        console.log('1. Testing very simple case: 2 fields, 1 measurement...');
        
        // Insert exactly 2 fields
        await axios.post(`${BASE_URL}/write`, {
            measurement: 'simple',
            tags: { id: 'test1' },
            fields: { 
                fieldA: 100.0,
                fieldB: 200.0
            },
            timestamp: 1704067700000000000
        });
        
        console.log('2. Test both fields work before deletion...');
        try {
            await axios.post(`${BASE_URL}/query`, {
                query: 'avg:simple(fieldA){id:test1}',
                startTime: 1704067700000000000,
                endTime: 1704067700000000001
            });
            console.log('  fieldA query: SUCCESS');
        } catch (e) {
            console.log('  fieldA query: FAILED');
        }
        
        try {
            await axios.post(`${BASE_URL}/query`, {
                query: 'avg:simple(fieldB){id:test1}',
                startTime: 1704067700000000000,
                endTime: 1704067700000000001
            });
            console.log('  fieldB query: SUCCESS');
        } catch (e) {
            console.log('  fieldB query: FAILED');
        }
        
        console.log('3. Delete ONLY fieldA...');
        await axios.post(`${BASE_URL}/delete`, {
            measurement: 'simple',
            tags: { id: 'test1' },
            fields: ['fieldA'],
            startTime: 1704067700000000000,
            endTime: 1704067700000000000
        });
        
        console.log('4. Test both fields after deleting fieldA...');
        try {
            await axios.post(`${BASE_URL}/query`, {
                query: 'avg:simple(fieldA){id:test1}',
                startTime: 1704067700000000000,
                endTime: 1704067700000000001
            });
            console.log('  fieldA query: SUCCESS (unexpected!)');
        } catch (e) {
            console.log('  fieldA query: FAILED (expected)');
        }
        
        try {
            await axios.post(`${BASE_URL}/query`, {
                query: 'avg:simple(fieldB){id:test1}',
                startTime: 1704067700000000000,
                endTime: 1704067700000000001
            });
            console.log('  fieldB query: SUCCESS (this is what we want!)');
        } catch (e) {
            console.log('  fieldB query: FAILED (this is the bug!)');
        }
        
        // Let's also try inserting a COMPLETELY NEW measurement to make sure the corruption isn't global
        console.log('5. Testing completely separate measurement...');
        await axios.post(`${BASE_URL}/write`, {
            measurement: 'separate',
            tags: { id: 'other' },
            fields: { fieldX: 300.0 },
            timestamp: 1704067700000000000
        });
        
        try {
            await axios.post(`${BASE_URL}/query`, {
                query: 'avg:separate(fieldX){id:other}',
                startTime: 1704067700000000000,
                endTime: 1704067700000000001
            });
            console.log('  separate measurement: SUCCESS (corruption is not global)');
        } catch (e) {
            console.log('  separate measurement: FAILED (corruption is global - very bad!)');
        }
        
    } catch (error) {
        console.error('Test error:', error.message);
        process.exit(1);
    }
}

debugExactIssue();