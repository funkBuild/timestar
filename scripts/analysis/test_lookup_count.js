const http = require('http');

async function writePoints(points) {
  return new Promise((resolve, reject) => {
    const postData = JSON.stringify({ writes: points });
    
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
      res.on('end', () => resolve());
    });

    req.on('error', reject);
    req.write(postData);
    req.end();
  });
}

async function main() {
  console.log('Testing metadata lookup patterns...\n');
  
  const points = [];
  const baseTime = Date.now() * 1000000;
  
  for (let i = 0; i < 10; i++) {
    points.push({
      measurement: 'test_lookup',
      tags: {
        location: 'loc-' + (i % 5),
        sensor: 'sensor-01'
      },
      fields: {
        value: Math.random() * 100,
        humidity: Math.random() * 50
      },
      timestamp: baseTime + i
    });
  }
  
  console.log('Sending batch of ' + points.length + ' points');
  console.log('Unique series: 5 locations × 1 sensor × 2 fields = 10 unique series');
  console.log('Expected lookups if every point calls indexMetadata: 20');
  console.log('Expected lookups if properly deduped: 10\n');
  
  await writePoints(points);
  
  console.log('Write completed. Checking logs for actual count...');
}

main().catch(console.error);
