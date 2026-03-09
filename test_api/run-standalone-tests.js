#!/usr/bin/env node

const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

// Find all standalone test files (test_*.js but not *.test.js)
const testDir = path.join(__dirname, 'http_api_tests');
const files = fs.readdirSync(testDir);

const standaloneTests = files.filter(file => 
  file.startsWith('test_') && 
  file.endsWith('.js') && 
  !file.endsWith('.test.js') &&
  !file.includes('debug')
);

console.log('Running standalone tests...\n');
console.log(`Found ${standaloneTests.length} standalone test files:\n`);

let currentIndex = 0;
let passed = 0;
let failed = 0;

function runNextTest() {
  if (currentIndex >= standaloneTests.length) {
    console.log('\n========================================');
    console.log('Standalone Test Results:');
    console.log(`✓ Passed: ${passed}`);
    console.log(`✗ Failed: ${failed}`);
    console.log(`Total: ${standaloneTests.length}`);
    console.log('========================================\n');
    process.exit(failed > 0 ? 1 : 0);
    return;
  }

  const testFile = standaloneTests[currentIndex];
  const testPath = path.join(testDir, testFile);
  
  console.log(`\n[${currentIndex + 1}/${standaloneTests.length}] Running ${testFile}...`);
  console.log('----------------------------------------');

  exec(`node ${testPath}`, (error, stdout, stderr) => {
    if (error) {
      console.error(`✗ FAILED: ${testFile}`);
      console.error('Error:', error.message);
      if (stderr) console.error('Stderr:', stderr);
      if (stdout) console.log('Output:', stdout);
      failed++;
    } else {
      console.log(`✓ PASSED: ${testFile}`);
      if (stdout) {
        // Show condensed output
        const lines = stdout.split('\n');
        if (lines.length > 10) {
          console.log('Output (first 10 lines):');
          console.log(lines.slice(0, 10).join('\n'));
          console.log(`... (${lines.length - 10} more lines)`);
        } else {
          console.log('Output:', stdout);
        }
      }
      passed++;
    }
    
    currentIndex++;
    runNextTest();
  });
}

// Check if server is running
const http = require('http');

const options = {
  hostname: 'localhost',
  port: 8086,
  path: '/health',
  method: 'GET'
};

console.log('Checking if TimeStar server is running...');

const req = http.request(options, (res) => {
  if (res.statusCode === 200) {
    console.log('✓ Server is running on port 8086\n');
    runNextTest();
  } else {
    console.error('✗ Server returned unexpected status:', res.statusCode);
    process.exit(1);
  }
});

req.on('error', (e) => {
  console.error('✗ Cannot connect to server on port 8086');
  console.error('Please start the TimeStar server first:');
  console.error('  cd build && ./bin/timestar_http_server');
  process.exit(1);
});

req.end();