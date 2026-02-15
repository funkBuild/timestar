const axios = require('axios');

// Test configuration
const HOST = process.env.TSDB_HOST || 'localhost';
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Helper: Check if value is NaN (JSON encodes NaN as null)
const isNaNOrNull = (v) => v === null || Number.isNaN(v);
const isValidNumber = (v) => v !== null && !Number.isNaN(v);

// Generate random measurement name to avoid conflicts
const testMeasurement = `transform_test_${Math.floor(Math.random() * 10000)}`;
const now = Date.now() * 1000000; // Convert to nanoseconds
const SECOND_NS = 1000000000; // 1 second in nanoseconds

// Helper function to create test timestamps
const createTestTimestamps = (count, intervalNs = SECOND_NS) => {
  const arr = [];
  for (let i = 0; i < count; i++) {
    arr.push(now + (intervalNs * i));
  }
  return arr;
};

// Test data setup
let testData;

describe('Transform Functions E2E Tests', () => {
  // Before all tests, write test data
  beforeAll(async () => {
    // Create 10 data points with known values for predictable testing
    const timestamps = createTestTimestamps(10);

    // Known dataset for testing transform functions:
    // [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    const values = Array.from({length: 10}, (_, i) => (i + 1) * 10);

    // Write test data
    for (let i = 0; i < 10; i++) {
      await axios.post(`${BASE_URL}/write`, {
        measurement: testMeasurement,
        tags: { sensor: 'main' },
        fields: { value: values[i] },
        timestamp: timestamps[i]
      });
    }

    // Also create a dataset with some negative values: [-20, -10, 0, 10, 20, 30, 40, 50, 60, 70]
    const negativeValues = Array.from({length: 10}, (_, i) => (i - 2) * 10);
    for (let i = 0; i < 10; i++) {
      await axios.post(`${BASE_URL}/write`, {
        measurement: testMeasurement,
        tags: { sensor: 'negative' },
        fields: { value: negativeValues[i] },
        timestamp: timestamps[i]
      });
    }

    // Create a counter dataset that resets: [10, 20, 30, 5, 15, 25, 35, 10, 20, 30]
    // (simulates monotonic counter with resets at positions 3 and 7)
    const counterValues = [10, 20, 30, 5, 15, 25, 35, 10, 20, 30];
    for (let i = 0; i < 10; i++) {
      await axios.post(`${BASE_URL}/write`, {
        measurement: testMeasurement,
        tags: { sensor: 'counter' },
        fields: { value: counterValues[i] },
        timestamp: timestamps[i]
      });
    }

    // Create a dataset with some zeros and NaN-like special values
    // [0, 5, 0, 15, 0, 25, 0, 35, 0, 45]
    const zeroValues = Array.from({length: 10}, (_, i) => i % 2 === 0 ? 0 : (i * 5));
    for (let i = 0; i < 10; i++) {
      await axios.post(`${BASE_URL}/write`, {
        measurement: testMeasurement,
        tags: { sensor: 'zeros' },
        fields: { value: zeroValues[i] },
        timestamp: timestamps[i]
      });
    }

    testData = { timestamps, values, negativeValues, counterValues, zeroValues };

    // Small delay to ensure data is flushed
    await new Promise(resolve => setTimeout(resolve, 500));
  }, 30000);

  describe('diff() - Difference between consecutive points', () => {
    test('should compute difference between consecutive values', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'diff(a)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      // For values [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
      // diff should be [NaN, 10, 10, 10, 10, 10, 10, 10, 10, 10]
      // After first value (which is NaN), all diffs should be 10
      const values = response.data.values;
      expect(values.length).toBeGreaterThan(1);

      // Skip first value (NaN/null), rest should be 10
      for (let i = 1; i < values.length; i++) {
        if (isValidNumber(values[i])) {
          expect(values[i]).toBeCloseTo(10, 1);
        }
      }
    });
  });

  describe('monotonic_diff() - Diff with counter reset handling', () => {
    test('should handle counter resets by using current value instead of negative diff', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:counter}`
        },
        formula: 'monotonic_diff(a)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For counter [10, 20, 30, 5, 15, 25, 35, 10, 20, 30]
      // monotonic_diff should be [NaN, 10, 10, 5, 10, 10, 10, 10, 10, 10]
      // (at resets, it uses the current value instead of negative diff)
      // All non-NaN values should be positive
      for (let i = 0; i < values.length; i++) {
        if (isValidNumber(values[i])) {
          expect(values[i]).toBeGreaterThanOrEqual(0);
        }
      }
    });
  });

  describe('default_zero() - Replace NaN with 0', () => {
    test('should replace NaN values with 0', async () => {
      // First create some NaN values using diff (first element is NaN)
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'default_zero(diff(a))',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // No values should be NaN/null after default_zero
      for (const v of values) {
        expect(isNaNOrNull(v)).toBe(false);
      }
    });
  });

  describe('count_nonzero() - Count non-zero values', () => {
    test('should count non-zero values', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:zeros}`
        },
        formula: 'count_nonzero(a)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For [0, 5, 0, 15, 0, 25, 0, 35, 0, 45], non-zero count is 5
      // All values in the result should be the same (constant series)
      const expectedCount = 5;
      for (const v of values) {
        expect(v).toBe(expectedCount);
      }
    });
  });

  describe('count_not_null() - Count non-NaN values', () => {
    test('should count all values when none are NaN', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'count_not_null(a)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // All 10 values should be counted
      for (const v of values) {
        expect(v).toBe(10);
      }
    });

    test('should exclude NaN values from count', async () => {
      // Use diff to create a NaN in first position
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'count_not_null(diff(a))',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // Should count 9 (10 - 1 NaN from diff)
      for (const v of values) {
        expect(v).toBe(9);
      }
    });
  });

  describe('clamp_min() - Clamp values to minimum', () => {
    test('should clamp negative values to minimum threshold', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:negative}`
        },
        formula: 'clamp_min(a, 0)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For [-20, -10, 0, 10, 20, 30, 40, 50, 60, 70] with clamp_min(0)
      // Result should be [0, 0, 0, 10, 20, 30, 40, 50, 60, 70]
      for (const v of values) {
        expect(v).toBeGreaterThanOrEqual(0);
      }
    });

    test('should not affect values already above minimum', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'clamp_min(a, 5)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      // All values [10, 20, ..., 100] are above 5, so no change
      for (const v of values) {
        expect(v).toBeGreaterThanOrEqual(5);
      }
    });
  });

  describe('clamp_max() - Clamp values to maximum', () => {
    test('should clamp values above threshold to maximum', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'clamp_max(a, 50)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] with clamp_max(50)
      // Result should be [10, 20, 30, 40, 50, 50, 50, 50, 50, 50]
      for (const v of values) {
        expect(v).toBeLessThanOrEqual(50);
      }
    });
  });

  describe('cutoff_min() - Set values below threshold to NaN', () => {
    test('should set values below threshold to NaN', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'cutoff_min(a, 50)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] with cutoff_min(50)
      // Values 10, 20, 30, 40 should become NaN (null in JSON); 50-100 should remain
      // Non-NaN values should be >= 50
      let nanCount = 0;
      let validCount = 0;
      for (const v of values) {
        if (isNaNOrNull(v)) {
          nanCount++;
        } else {
          validCount++;
          expect(v).toBeGreaterThanOrEqual(50);
        }
      }
      expect(nanCount).toBe(4); // 10, 20, 30, 40
      expect(validCount).toBe(6); // 50, 60, 70, 80, 90, 100
    });
  });

  describe('cutoff_max() - Set values above threshold to NaN', () => {
    test('should set values above threshold to NaN', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'cutoff_max(a, 50)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For [10, 20, 30, 40, 50, 60, 70, 80, 90, 100] with cutoff_max(50)
      // Values 60, 70, 80, 90, 100 should become NaN (null in JSON); 10-50 should remain
      let nanCount = 0;
      let validCount = 0;
      for (const v of values) {
        if (isNaNOrNull(v)) {
          nanCount++;
        } else {
          validCount++;
          expect(v).toBeLessThanOrEqual(50);
        }
      }
      expect(nanCount).toBe(5); // 60, 70, 80, 90, 100
      expect(validCount).toBe(5); // 10, 20, 30, 40, 50
    });
  });

  describe('per_minute() - Rate per minute', () => {
    test('should calculate rate per minute', async () => {
      // Data has 1 second intervals, and diff is 10 per interval
      // per_minute with 1 second interval = diff * 60 = 600
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'per_minute(a, 1)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(1);

      // Skip first NaN/null, rest should be diff * 60 = 10 * 60 = 600
      for (let i = 1; i < values.length; i++) {
        if (isValidNumber(values[i])) {
          expect(values[i]).toBeCloseTo(600, 0);
        }
      }
    });
  });

  describe('per_hour() - Rate per hour', () => {
    test('should calculate rate per hour', async () => {
      // Data has 1 second intervals, and diff is 10 per interval
      // per_hour with 1 second interval = diff * 3600 = 36000
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'per_hour(a, 1)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(1);

      // Skip first NaN/null, rest should be diff * 3600 = 10 * 3600 = 36000
      for (let i = 1; i < values.length; i++) {
        if (isValidNumber(values[i])) {
          expect(values[i]).toBeCloseTo(36000, 0);
        }
      }
    });
  });

  describe('abs() - Absolute value', () => {
    test('should compute absolute value of negative numbers', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:negative}`
        },
        formula: 'abs(a)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // For [-20, -10, 0, 10, 20, 30, 40, 50, 60, 70]
      // abs should be [20, 10, 0, 10, 20, 30, 40, 50, 60, 70]
      for (const v of values) {
        expect(v).toBeGreaterThanOrEqual(0);
      }
    });
  });

  describe('Combined Transform Operations', () => {
    test('should chain multiple transforms: default_zero(diff(a))', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'default_zero(diff(a))',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // All values should be valid (no NaN/null)
      for (const v of values) {
        expect(isNaNOrNull(v)).toBe(false);
      }
    });

    test('should chain clamp_min and clamp_max for range clamping', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'clamp_max(clamp_min(a, 30), 70)',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // All values should be between 30 and 70
      for (const v of values) {
        expect(v).toBeGreaterThanOrEqual(30);
        expect(v).toBeLessThanOrEqual(70);
      }
    });

    test('should combine transforms with arithmetic: abs(diff(a)) + 100', async () => {
      const response = await axios.post(`${BASE_URL}/derived`, {
        queries: {
          a: `avg:${testMeasurement}(value){sensor:main}`
        },
        formula: 'default_zero(abs(diff(a))) + 100',
        startTime: now - SECOND_NS,
        endTime: now + 11 * SECOND_NS
      });

      expect(response.status).toBe(200);
      expect(response.data.status).toBe('success');

      const values = response.data.values;
      expect(values.length).toBeGreaterThan(0);

      // abs(diff) should be 0 for first, then 10 for rest
      // +100 means values should be >= 100
      for (const v of values) {
        expect(v).toBeGreaterThanOrEqual(100);
      }
    });
  });

  describe('Error Cases', () => {
    test('should handle unknown function gracefully', async () => {
      try {
        await axios.post(`${BASE_URL}/derived`, {
          queries: {
            a: `avg:${testMeasurement}(value){sensor:main}`
          },
          formula: 'unknown_function(a)',
          startTime: now - SECOND_NS,
          endTime: now + 11 * SECOND_NS
        });
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.status).toBe('error');
      }
    });

    test('should reject clamp_min with wrong number of arguments', async () => {
      try {
        await axios.post(`${BASE_URL}/derived`, {
          queries: {
            a: `avg:${testMeasurement}(value){sensor:main}`
          },
          formula: 'clamp_min(a)',
          startTime: now - SECOND_NS,
          endTime: now + 11 * SECOND_NS
        });
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data.status).toBe('error');
      }
    });
  });
});
