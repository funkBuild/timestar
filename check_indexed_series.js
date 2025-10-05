const http = require('http');

function makeRequest(path) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 8086,
      path: path,
      method: 'GET'
    };

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => { data += chunk; });
      res.on('end', () => {
        resolve(data ? JSON.parse(data) : {});
      });
    });

    req.on('error', reject);
    req.end();
  });
}

async function main() {
  console.log('Checking indexed series count...\n');
  
  // This would need a metadata endpoint to check
  // For now, let's just insert a test point and see if it works
  
  const testWrite = {
    measurement: 'test',
    tags: { location: 'test-A', sensor: 'sensor-1' },
    fields: { value: 100 },
    timestamp: Date.now() * 1000000
  };
  
  const testWrite2 = {
    measurement: 'test',
    tags: { location: 'test-B', sensor: 'sensor-2' },
    fields: { value: 200 },
    timestamp: Date.now() * 1000000 + 1
  };
  
  console.log('Writing two points with different tags but same measurement+field...');
  
  const postData = JSON.stringify({ writes: [testWrite, testWrite2] });
  
  await new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port: 8086,
      path: '/write',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData)
      }
    };

    const req = http.request(options, (res) => {
      res.on('data', () => {});
      res.on('end', () => {
        console.log('Write completed with status:', res.statusCode);
        resolve();
      });
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

main().catch(console.error);
