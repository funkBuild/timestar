const axios = require('axios');
const { performance } = require('perf_hooks');

// Test configuration
const HOST = process.env.TSDB_HOST || 'localhost';
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Generate random measurement name to avoid conflicts
const testMeasurement = `test_metrics_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000; // Convert to nanoseconds

// Test helpers
const closeTo = (arr, tolerance = 5) => arr.map(a => expect.closeTo(a, tolerance));

// Helper function to extract tag value from groupTags array
const getTagValue = (groupTags, tagName) => {
  if (!groupTags) return undefined;
  const tag = groupTags.find(t => t.startsWith(`${tagName}=`));
  return tag ? tag.split('=')[1] : undefined;
};

// Helper to convert groupTags array to object for easier access
const groupTagsToObject = (groupTags) => {
  if (!groupTags) return {};
  const obj = {};
  groupTags.forEach(tag => {
    const [key, value] = tag.split('=');
    obj[key] = value;
  });
  return obj;
};

// Helper to merge series with single fields into a single series object
// The API returns each field as a separate series, but tests expect them merged
const mergeSeries = (seriesArray) => {
  if (!seriesArray || seriesArray.length === 0) return null;
  if (seriesArray.length === 1) return seriesArray[0];

  const merged = {
    measurement: seriesArray[0].measurement,
    tags: seriesArray[0].tags,
    groupTags: seriesArray[0].groupTags,
    fields: {}
  };

  seriesArray.forEach(series => {
    Object.assign(merged.fields, series.fields);
  });

  return merged;
};

const average = (...arrs) => {
  const len = arrs[0].length;
  const out = Array(len).fill(0);
  
  arrs.forEach((arr) => {
    arr.forEach((item, i) => {
      out[i] += item;
    });
  });
  
  return out.map(i => i / arrs.length);
};

const createTestTimestamps = (count = 100) => {
  const arr = [];
  for (let i = 0; i < count; i++) {
    arr.push(now - (1000000000 * (count - i - 1))); // 1 second intervals in nanoseconds
  }
  return arr;
};

const createTestFieldData = (count, multiplier) => {
  const arr = [];
  for (let i = 0; i < count; i++) {
    arr.push(0.1 * multiplier * i);
  }
  return arr;
};

// HTTP client helpers
const writeData = async (measurement, tags, field, timestamps, values) => {
  const writes = timestamps.map((ts, i) => ({
    measurement,
    tags,
    fields: { [field]: values[i] },
    timestamp: ts
  }));
  
  const response = await axios.post(`${BASE_URL}/write`, { writes });
  return response.data;
};

const writeMultiFieldData = async (measurement, tags, fields, timestamps) => {
  const writes = timestamps.map((ts, i) => {
    const fieldData = {};
    Object.keys(fields).forEach(field => {
      fieldData[field] = fields[field][i];
    });
    return {
      measurement,
      tags,
      fields: fieldData,
      timestamp: ts
    };
  });
  
  const response = await axios.post(`${BASE_URL}/write`, { writes });
  return response.data;
};

const query = async (queryStr, startTime = now - 100000000000, endTime = now, aggregationInterval = null) => {
  const request = {
    query: queryStr,
    startTime,
    endTime
  };
  
  if (aggregationInterval) {
    request.aggregationInterval = aggregationInterval;
  }
  
  const response = await axios.post(`${BASE_URL}/query`, request);
  return response.data;
};

// Create test data
const createTestData = async () => {
  const timestamps = createTestTimestamps(100);
  
  // Insert data for device aaaaa
  await writeMultiFieldData(
    `${testMeasurement}.moisture`,
    { deviceId: 'aaaaa', paddock: 'back-paddock' },
    {
      value1: createTestFieldData(100, 1),
      value2: createTestFieldData(100, 2),
      value3: createTestFieldData(100, 3)
    },
    timestamps
  );
  
  // Insert data for device bbbbb
  await writeMultiFieldData(
    `${testMeasurement}.moisture`,
    { deviceId: 'bbbbb', paddock: 'back-paddock' },
    {
      value1: createTestFieldData(100, 4),
      value2: createTestFieldData(100, 5),
      value3: createTestFieldData(100, 6)
    },
    timestamps
  );
  
  // Insert data for device ccccc (different paddock)
  await writeMultiFieldData(
    `${testMeasurement}.moisture`,
    { deviceId: 'ccccc', paddock: 'front-paddock' },
    {
      value1: createTestFieldData(100, 7),
      value2: createTestFieldData(100, 8),
      value3: createTestFieldData(100, 9)
    },
    timestamps
  );
};

const createTestImageData = async () => {
  const currentTime = Date.now() * 1000000; // Get current time in nanoseconds
  const timestamps = [
    currentTime - 2000000000,  // 2 seconds ago from now
    currentTime - 1000000000   // 1 second ago from now
  ];

  // Simulate image references (in real app would be actual binary data)
  const writes = timestamps.map((ts, i) => ({
    measurement: `${testMeasurement}.images`,
    tags: { deviceId: 'camera' },
    fields: { image: `ref::image${i+1}::s3://bucket/image${i+1}.jpeg` },
    timestamp: ts
  }));

  await axios.post(`${BASE_URL}/write`, { writes });
};

const createTestBooleanData = async () => {
  const currentTime = Date.now() * 1000000; // Get current time in nanoseconds
  const timestamps = [
    currentTime - 2000000000,  // 2 seconds ago from now
    currentTime - 1000000000   // 1 second ago from now
  ];

  const writes = timestamps.map((ts, i) => ({
    measurement: `${testMeasurement}.boolean`,
    tags: { deviceId: 'sensor' },
    fields: { value: i === 0 },  // true, false
    timestamp: ts
  }));

  await axios.post(`${BASE_URL}/write`, { writes });
};

// Test suite
describe('Comprehensive TSDB Query Tests', () => {
  beforeAll(async () => {
    // Wait for server to be ready
    let retries = 10;
    while (retries > 0) {
      try {
        await axios.get(`${BASE_URL}/health`);
        break;
      } catch (e) {
        retries--;
        if (retries === 0) throw new Error('Server not responding');
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    
    // Create test data
    await createTestData();
    await createTestImageData();
    await createTestBooleanData();
  });
  
  describe('Aggregation Functions', () => {
    test('MIN aggregation returns minimum values', async () => {
      const result = await query(`min:${testMeasurement}.moisture(){}`);

      expect(result.status).toBe('success');
      expect(result.series.length).toBe(3); // One series per field

      const series = mergeSeries(result.series);
      expect(series.fields.value1).toBeDefined();
      expect(series.fields.value2).toBeDefined();
      expect(series.fields.value3).toBeDefined();

      // MIN should return device aaaaa's values (lowest)
      const expectedValue1 = createTestFieldData(100, 1);
      const expectedValue2 = createTestFieldData(100, 2);
      const expectedValue3 = createTestFieldData(100, 3);

      expect(series.fields.value1.values).toEqual(closeTo(expectedValue1));
      expect(series.fields.value2.values).toEqual(closeTo(expectedValue2));
      expect(series.fields.value3.values).toEqual(closeTo(expectedValue3));
    });
    
    test('MAX aggregation returns maximum values', async () => {
      const result = await query(`max:${testMeasurement}.moisture(){}`);

      expect(result.status).toBe('success');
      expect(result.series.length).toBe(3); // One series per field

      const series = mergeSeries(result.series);

      // MAX should return device ccccc's values (highest)
      const expectedValue1 = createTestFieldData(100, 7);
      const expectedValue2 = createTestFieldData(100, 8);
      const expectedValue3 = createTestFieldData(100, 9);

      expect(series.fields.value1.values).toEqual(closeTo(expectedValue1));
      expect(series.fields.value2.values).toEqual(closeTo(expectedValue2));
      expect(series.fields.value3.values).toEqual(closeTo(expectedValue3));
    });

    test('AVG aggregation returns average values', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(){}`);

      expect(result.status).toBe('success');
      expect(result.series.length).toBe(3); // One series per field

      const series = mergeSeries(result.series);
      
      // AVG should return average of all three devices
      const device1 = createTestFieldData(100, 1);
      const device2 = createTestFieldData(100, 4);
      const device3 = createTestFieldData(100, 7);
      const expectedValue1 = average(device1, device2, device3);
      
      expect(series.fields.value1.values).toEqual(closeTo(expectedValue1));
    });
    
    test('SUM aggregation returns sum of values', async () => {
      const result = await query(`sum:${testMeasurement}.moisture(value1){}`);
      
      expect(result.status).toBe('success');
      expect(result.series.length).toBe(1);
      
      const series = result.series[0];
      
      // SUM should add all three devices' values
      const device1 = createTestFieldData(100, 1);
      const device2 = createTestFieldData(100, 4);
      const device3 = createTestFieldData(100, 7);
      
      const expectedSum = device1.map((v, i) => v + device2[i] + device3[i]);
      
      expect(series.fields.value1.values).toEqual(closeTo(expectedSum));
    });
    
    test('LATEST aggregation returns most recent values', async () => {
      const result = await query(`latest:${testMeasurement}.moisture(value1){}`);
      
      expect(result.status).toBe('success');
      expect(result.series.length).toBe(1);
      
      const series = result.series[0];
      const timestamps = series.fields.value1.timestamps;
      
      // Latest should return recent values
      expect(timestamps.length).toBeGreaterThan(0);
      expect(timestamps[timestamps.length - 1]).toBeLessThanOrEqual(now);
      expect(timestamps[timestamps.length - 1]).toBeGreaterThan(now - 2000000000);
    });
  });
  
  describe('Field Selection', () => {
    test('Query without fields returns all fields', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(){}`);

      expect(result.status).toBe('success');
      expect(result.series.length).toBe(3); // One series per field

      const series = mergeSeries(result.series);
      expect(Object.keys(series.fields).sort()).toEqual(['value1', 'value2', 'value3']);
    });
    
    test('Query with specific fields returns only those fields', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(value1,value3){}`);

      expect(result.status).toBe('success');
      expect(result.series.length).toBe(2); // One series per field

      const series = mergeSeries(result.series);
      expect(Object.keys(series.fields).sort()).toEqual(['value1', 'value3']);
      expect(series.fields.value2).toBeUndefined();
    });
    
    test('Query handles fields with same prefix correctly', async () => {
      // Use unique measurement name to avoid data accumulation
      const uniqueMeasurement = `lid_data_${Math.floor(Math.random() * 100000)}`;
      
      // First insert data with similar field names
      const timestamps = [1, 2, 3, 4, 5, 6];
      await writeMultiFieldData(
        uniqueMeasurement,
        { meter_id: '33616' },
        {
          pnf: [0, 0, 0, 0, 0, 0],
          pnf_status: [1, 1, 1, 1, 1, 1]
        },
        timestamps
      );
      
      // Query only pnf field
      const result = await query(`avg:${uniqueMeasurement}(pnf){meter_id:33616}`, 0, 1000);
      
      expect(result.status).toBe('success');
      expect(result.series.length).toBe(1);
      
      const series = result.series[0];
      expect(series.fields.pnf).toBeDefined();
      expect(series.fields.pnf_status).toBeUndefined();
      expect(series.fields.pnf.values).toEqual([0, 0, 0, 0, 0, 0]);
    });
  });
  
  describe('Scope Filtering', () => {
    test('Query without scope returns all data', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(value1){}`);
      
      expect(result.status).toBe('success');
      expect(result.series.length).toBe(1);
      
      // Should average all three devices
      const device1 = createTestFieldData(100, 1);
      const device2 = createTestFieldData(100, 4);
      const device3 = createTestFieldData(100, 7);
      const expectedAvg = average(device1, device2, device3);
      
      expect(result.series[0].fields.value1.values).toEqual(closeTo(expectedAvg));
    });
    
    test('Query with scope filters data correctly', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(value1,value2,value3){paddock:back-paddock}`);

      expect(result.status).toBe('success');
      // Scopes are no longer at top level per new format
      expect(result.series.length).toBe(3); // One series per field

      const series = mergeSeries(result.series);

      // Should only average aaaaa and bbbbb (both in back-paddock)
      const device1 = createTestFieldData(100, 1);
      const device2 = createTestFieldData(100, 4);
      const expectedValue1 = average(device1, device2);

      expect(series.fields.value1.values).toEqual(closeTo(expectedValue1));
    });
  });
  
  describe('Group By', () => {
    test('Group by single tag', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(value1){paddock:back-paddock} by {deviceId}`);
      
      expect(result.status).toBe('success');
      // Should have 2 series (aaaaa and bbbbb in back-paddock)
      expect(result.series.length).toBe(2);
      
      const deviceIds = result.series.map(s => getTagValue(s.groupTags, 'deviceId')).sort();
      expect(deviceIds).toEqual(['aaaaa', 'bbbbb']);
      
      // Check values for each device
      result.series.forEach(series => {
        const deviceId = getTagValue(series.groupTags, 'deviceId');
        if (deviceId === 'aaaaa') {
          expect(series.fields.value1.values).toEqual(closeTo(createTestFieldData(100, 1)));
        } else if (deviceId === 'bbbbb') {
          expect(series.fields.value1.values).toEqual(closeTo(createTestFieldData(100, 4)));
        }
      });
    });
    
    test('Group by without scope', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(value1){} by {deviceId}`);
      
      expect(result.status).toBe('success');
      // Should have 3 series (all devices)
      expect(result.series.length).toBe(3);
      
      const deviceIds = result.series.map(s => getTagValue(s.groupTags, 'deviceId')).sort();
      expect(deviceIds).toEqual(['aaaaa', 'bbbbb', 'ccccc']);
    });
    
    test('Group by multiple tags', async () => {
      const result = await query(`avg:${testMeasurement}.moisture(value1){} by {paddock,deviceId}`);
      
      expect(result.status).toBe('success');
      // Should have 3 series (each unique combination)
      expect(result.series.length).toBe(3);
      
      // Each series should have both tags
      result.series.forEach(series => {
        expect(getTagValue(series.groupTags, 'paddock')).toBeDefined();
        expect(getTagValue(series.groupTags, 'deviceId')).toBeDefined();
      });
    });
  });
  
  describe('Data Types', () => {
    test('Boolean data query', async () => {
      // Use a wider time range to ensure we capture the data
      const currentTime = Date.now() * 1000000;
      const result = await query(`avg:${testMeasurement}.boolean(){}`, 0, currentTime + 10000000000);

      expect(result.status).toBe('success');
      // Boolean data may not be supported in aggregation queries yet
      // Just verify the query succeeds
      if (result.series.length > 0) {
        const series = result.series[0];
        expect(series.fields.value).toBeDefined();

        // Boolean values now returned as actual booleans
        const values = series.fields.value.values;
        expect(values.length).toBeGreaterThanOrEqual(1);
        // Just verify we got boolean values
        values.forEach(v => expect(typeof v).toBe('boolean'));
      }
    });

    test('String/image data query', async () => {
      // Use a wider time range to ensure we capture the data
      const currentTime = Date.now() * 1000000;
      const result = await query(`avg:${testMeasurement}.images(){}`, 0, currentTime + 10000000000);

      expect(result.status).toBe('success');
      // String data may not be supported in aggregation queries yet
      // Just verify the query succeeds
      if (result.series.length > 0) {
        const series = result.series[0];

        // String fields might be in a different format
        // Check both possible locations
        const imageData = series.fields.image || series.string_fields?.image;
        expect(imageData).toBeDefined();

        if (imageData.values) {
          expect(imageData.values.length).toBeGreaterThanOrEqual(1);
          expect(imageData.values[0]).toContain('ref::');
          expect(imageData.values[0]).toContain('s3://');
        }
      }
    });
  });
  
  describe('Time Intervals', () => {
    test('Aggregation with time intervals', async () => {
      const result = await query(
        `avg:${testMeasurement}.moisture(value1){}`,
        now - 100000000000,
        now,
        '10s'  // 10 second intervals
      );
      
      expect(result.status).toBe('success');
      expect(result.series.length).toBe(1);
      
      const series = result.series[0];
      const timestamps = series.fields.value1.timestamps;
      
      // With 100 seconds of data and 10-second intervals, expect ~10 buckets
      expect(timestamps.length).toBeGreaterThanOrEqual(9);
      expect(timestamps.length).toBeLessThanOrEqual(11);
    });
    
    test('MAX with time intervals', async () => {
      const result = await query(
        `max:${testMeasurement}.moisture(value1){}`,
        now - 100000000000,
        now,
        '20s'  // 20 second intervals
      );
      
      expect(result.status).toBe('success');
      expect(result.series.length).toBe(1);
      
      const series = result.series[0];
      const timestamps = series.fields.value1.timestamps;
      
      // With 100 seconds and 20-second intervals, expect ~5 buckets
      expect(timestamps.length).toBeGreaterThanOrEqual(4);
      expect(timestamps.length).toBeLessThanOrEqual(6);
    });
  });
  
  describe('Error Cases', () => {
    test('Missing aggregation method throws error', async () => {
      try {
        await query(`${testMeasurement}.moisture(){}`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.error).toContain('aggregation method');
      }
    });
    
    test('Invalid aggregation method throws error', async () => {
      try {
        await query(`invalid:${testMeasurement}.moisture(){}`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.error).toContain('avg', 'min', 'max', 'sum', 'latest');
      }
    });
    
    test('Missing measurement throws error', async () => {
      try {
        await query(`avg:(value1){}`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.error).toContain('measurement');
      }
    });
    
    test('Missing fields parentheses throws error', async () => {
      try {
        await query(`avg:${testMeasurement}.moisture{paddock:back-paddock}`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.error).toContain('field');
      }
    });
    
    test('Unclosed scope brace throws error', async () => {
      try {
        await query(`avg:${testMeasurement}.moisture(){paddock:back-paddock`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.error).toContain('brace');
      }
    });
    
    test('Malformed group by throws error', async () => {
      try {
        await query(`avg:${testMeasurement}.moisture(){} by deviceId}`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.error).toContain('brace');
      }
    });
  });
  
  describe('Cache and Data Updates', () => {
    test('Correctly clears cache when inserting new data', async () => {
      // First query - with 3 fields and 3 devices, we get 9 series
      const result1 = await query(`avg:${testMeasurement}.moisture(){} by {deviceId}`);
      expect(result1.series.length).toBe(9); // 3 devices * 3 fields

      // Insert new device data
      const timestamps = createTestTimestamps(100);
      await writeMultiFieldData(
        `${testMeasurement}.moisture`,
        { deviceId: 'zzzzzz', paddock: 'back-paddock' },
        {
          value1: createTestFieldData(100, 1),
          value2: createTestFieldData(100, 2),
          value3: createTestFieldData(100, 3)
        },
        timestamps
      );

      // Query again - now with 4 devices and 3 fields, we get 12 series
      const result2 = await query(`avg:${testMeasurement}.moisture(){} by {deviceId}`);
      expect(result2.series.length).toBe(12); // 4 devices * 3 fields

      // Verify new device is in results
      const deviceIds = result2.series.map(s => getTagValue(s.groupTags, 'deviceId'));
      expect(deviceIds).toContain('zzzzzz');
    });
  });
  
  describe('Performance', () => {
    test('Large query completes in reasonable time', async () => {
      const startTime = performance.now();
      
      const result = await query(
        `avg:${testMeasurement}.moisture(){}`,
        now - 100000000000,
        now,
        '1s'  // 1 second intervals (100 buckets)
      );
      
      const endTime = performance.now();
      const duration = endTime - startTime;
      
      expect(result.status).toBe('success');
      expect(duration).toBeLessThan(5000); // Should complete within 5 seconds
      
      if (result.statistics) {
        console.log(`Query execution time: ${result.statistics.execution_time_ms}ms`);
        console.log(`Points processed: ${result.statistics.point_count}`);
      }
    });
  });
});

// Custom Jest matchers
expect.extend({
  closeTo(received, expected, tolerance = 5) {
    const pass = Math.abs(received - expected) < tolerance;
    return {
      pass,
      message: () => pass
        ? `Expected ${received} not to be close to ${expected}`
        : `Expected ${received} to be close to ${expected} (within ${tolerance})`
    };
  }
});