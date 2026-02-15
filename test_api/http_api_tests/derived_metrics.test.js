const axios = require('axios');

// Test configuration
const HOST = process.env.TSDB_HOST || 'localhost';
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Generate random measurement name to avoid conflicts
const testMeasurement = `derived_test_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000; // Convert to nanoseconds

// Helper function to create test timestamps
const createTestTimestamps = (count, intervalNs = 1000000000) => {
  const arr = [];
  for (let i = 0; i < count; i++) {
    arr.push(now + (intervalNs * i));
  }
  return arr;
};

// Test data setup
let testData;

describe('Derived Metrics API', () => {
  // Before all tests, write test data
  beforeAll(async () => {
    // Create 10 data points for each of two metrics
    const timestamps = createTestTimestamps(10);

    // Metric A: values 10, 20, 30, ... 100
    const valuesA = Array.from({length: 10}, (_, i) => (i + 1) * 10);

    // Metric B: values 5, 10, 15, ... 50
    const valuesB = Array.from({length: 10}, (_, i) => (i + 1) * 5);

    // Write metric A
    for (let i = 0; i < 10; i++) {
      await axios.post(`${BASE_URL}/write`, {
        measurement: testMeasurement,
        tags: { metric: 'cpu' },
        fields: { value: valuesA[i] },
        timestamp: timestamps[i]
      });
    }

    // Write metric B
    for (let i = 0; i < 10; i++) {
      await axios.post(`${BASE_URL}/write`, {
        measurement: testMeasurement,
        tags: { metric: 'memory' },
        fields: { value: valuesB[i] },
        timestamp: timestamps[i]
      });
    }

    testData = { timestamps, valuesA, valuesB };

    // Small delay to ensure data is flushed
    await new Promise(resolve => setTimeout(resolve, 500));
  }, 30000);

  describe('Basic Formula Operations', () => {
    test('should add two series', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: 'a + b',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.timestamps).toBeDefined();
      expect(response.data.values).toBeDefined();
      expect(response.data.formula).toBe('a + b');

      // Each value should be A + B: (10+5), (20+10), (30+15), ...
      // With aggregation, we expect aggregated values
      expect(response.data.statistics).toBeDefined();
      expect(response.data.statistics.sub_queries_executed).toBe(2);
    });

    test('should subtract two series', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: 'a - b',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.formula).toBe('a - b');
    });

    test('should multiply two series', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: 'a * b',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.formula).toBe('a * b');
    });

    test('should divide two series', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: 'a / b',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.formula).toBe('a / b');

      // CPU values are 2x memory values, so ratio should be ~2
      if (response.data.values.length > 0) {
        response.data.values.forEach(v => {
          expect(v).toBeCloseTo(2.0, 1);
        });
      }
    });
  });

  describe('Complex Formulas', () => {
    test('should handle percentage calculation: (a / (a + b)) * 100', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: '(a / (a + b)) * 100',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.formula).toBe('(a / (a + b)) * 100');

      // CPU is 2x memory, so percentage should be ~66.67%
      if (response.data.values.length > 0) {
        response.data.values.forEach(v => {
          expect(v).toBeCloseTo(66.67, 0);
        });
      }
    });

    test('should handle formulas with scalar operations', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`
        },
        formula: 'a * 2 + 10',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.formula).toBe('a * 2 + 10');
    });

    test('should handle nested parentheses', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: '((a + b) * 2) / (a - b + 1)',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
    });
  });

  describe('Single Query Reference', () => {
    test('should handle single query with identity formula', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          x: `avg:${testMeasurement}(value){metric:cpu}`
        },
        formula: 'x',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.statistics.sub_queries_executed).toBe(1);
    });

    test('should apply negation to series', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`
        },
        formula: '-a',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      // All values should be negative
      if (response.data.values.length > 0) {
        response.data.values.forEach(v => {
          expect(v).toBeLessThanOrEqual(0);
        });
      }
    });
  });

  describe('Aggregation Interval', () => {
    test('should support aggregationInterval parameter', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: 'a + b',
        startTime: now - 1000000000,
        endTime: now + 11000000000,
        aggregationInterval: '5s'
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
    });
  });

  describe('Error Handling', () => {
    test('should return error for empty request body', async () => {
      try {
        await axios.post(`${BASE_URL}/derived`, {});
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.status).toBe('error');
      }
    });

    test('should return error for missing formula', async () => {
      try {
        await axios.post(`${BASE_URL}/derived`, {
          queries: {
            a: `avg:${testMeasurement}(value){metric:cpu}`
          }
        });
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.status).toBe('error');
      }
    });

    test('should return error for invalid formula syntax', async () => {
      try {
        await axios.post(`${BASE_URL}/derived`, {
          queries: {
            a: `avg:${testMeasurement}(value){metric:cpu}`
          },
          formula: 'a ++ b',
          startTime: now - 1000000000,
          endTime: now + 11000000000
        });
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.status).toBe('error');
        expect(error.response.data.error).toBeDefined();
      }
    });

    test('should return error for undefined query reference', async () => {
      try {
        await axios.post(`${BASE_URL}/derived`, {
          queries: {
            a: `avg:${testMeasurement}(value){metric:cpu}`
          },
          formula: 'a + b',  // b is not defined
          startTime: now - 1000000000,
          endTime: now + 11000000000
        });
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.status).toBe('error');
      }
    });
  });

  describe('Statistics', () => {
    test('should return execution statistics', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`
        },
        formula: 'a + b',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.statistics).toBeDefined();
      expect(response.data.statistics.point_count).toBeDefined();
      expect(response.data.statistics.execution_time_ms).toBeDefined();
      expect(response.data.statistics.sub_queries_executed).toBe(2);
      expect(response.data.statistics.points_dropped_due_to_alignment).toBeDefined();
    });
  });

  describe('Unused Query Optimization', () => {
    test('should only execute referenced queries', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){metric:cpu}`,
          b: `avg:${testMeasurement}(value){metric:memory}`,
          c: `avg:${testMeasurement}(value){metric:unused}`  // Should not be executed
        },
        formula: 'a + b',  // Only uses a and b
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      expect(response.data.statistics.sub_queries_executed).toBe(2);  // Not 3
    });
  });

  describe('Empty Results', () => {
    test('should handle queries that return no data', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:nonexistent_measurement(value){tag:value}`
        },
        formula: 'a',
        startTime: now - 1000000000,
        endTime: now + 11000000000
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');
      // Empty result should still be valid
      expect(response.data.timestamps.length).toBe(0);
      expect(response.data.values.length).toBe(0);
    });
  });
});
