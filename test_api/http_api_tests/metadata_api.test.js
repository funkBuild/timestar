const axios = require('axios');

// Test configuration
const HOST = process.env.TSDB_HOST || 'localhost';
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Generate unique test data namespace
const testPrefix = `metadata_test_${Date.now()}`;

describe('Metadata API Tests', () => {
  // Helper function to write test data
  const writeTestData = async (measurement, tags, fields, timestamp = Date.now() * 1000000) => {
    const response = await axios.post(`${BASE_URL}/write`, {
      writes: [{
        measurement,
        tags,
        fields,
        timestamp
      }]
    });
    return response.data;
  };

  beforeAll(async () => {
    // Set up test data with various measurements, tags, and fields
    const now = Date.now() * 1000000;
    
    // Measurement 1: System metrics with multiple tags and fields
    await writeTestData(
      `${testPrefix}_system`,
      { host: 'server01', datacenter: 'us-east', environment: 'production' },
      { cpu_usage: 45.2, memory_usage: 78.5, disk_usage: 62.1, load_average: 1.5 },
      now
    );
    
    await writeTestData(
      `${testPrefix}_system`,
      { host: 'server02', datacenter: 'us-west', environment: 'production' },
      { cpu_usage: 55.8, memory_usage: 82.3, disk_usage: 45.7, load_average: 2.1 },
      now + 1000000
    );
    
    await writeTestData(
      `${testPrefix}_system`,
      { host: 'server03', datacenter: 'us-east', environment: 'staging' },
      { cpu_usage: 32.1, memory_usage: 45.6, temperature: 65.4 }, // Note: different fields
      now + 2000000
    );
    
    // Measurement 2: Application metrics
    await writeTestData(
      `${testPrefix}_application`,
      { service: 'api', version: 'v1.2.3', region: 'us-east' },
      { request_count: 1523, response_time: 125.4, error_rate: 0.02 },
      now + 3000000
    );
    
    await writeTestData(
      `${testPrefix}_application`,
      { service: 'database', version: 'v2.0.1', region: 'us-west' },
      { query_count: 8734, query_time: 45.2, connection_count: 125 },
      now + 4000000
    );
    
    // Measurement 3: IoT sensor data
    await writeTestData(
      `${testPrefix}_sensor`,
      { device_id: 'sensor001', location: 'warehouse_a', type: 'temperature' },
      { value: 22.5, battery: 85, signal_strength: -45 },
      now + 5000000
    );
    
    // Measurement 4: Business metrics (different data types)
    await writeTestData(
      `${testPrefix}_business`,
      { store: 'NYC001', department: 'electronics' },
      { revenue: 15234.50, transactions: 142, is_open: true, manager: "John Smith" },
      now + 6000000
    );
  });

  describe('GET /measurements', () => {
    test('Returns all measurements', async () => {
      const response = await axios.get(`${BASE_URL}/measurements`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('measurements');
      expect(Array.isArray(response.data.measurements)).toBe(true);
      
      // Check that our test measurements are included
      const testMeasurements = response.data.measurements.filter(m => m.startsWith(testPrefix));
      expect(testMeasurements).toContain(`${testPrefix}_system`);
      expect(testMeasurements).toContain(`${testPrefix}_application`);
      expect(testMeasurements).toContain(`${testPrefix}_sensor`);
      expect(testMeasurements).toContain(`${testPrefix}_business`);
    });

    test('Returns measurements with pagination', async () => {
      const response = await axios.get(`${BASE_URL}/measurements?limit=2&offset=0`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('measurements');
      expect(response.data).toHaveProperty('total');
      expect(response.data.measurements.length).toBeLessThanOrEqual(2);
    });

    test('Returns measurements with prefix filter', async () => {
      const response = await axios.get(`${BASE_URL}/measurements?prefix=${testPrefix}`);
      
      expect(response.status).toBe(200);
      expect(response.data.measurements.every(m => m.startsWith(testPrefix))).toBe(true);
      expect(response.data.measurements.length).toBe(4);
    });

    test('Returns measurement statistics if requested', async () => {
      const response = await axios.get(`${BASE_URL}/measurements?include_stats=true`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('measurements');
      
      // Each measurement should have statistics
      const testMeasurement = response.data.measurements.find(m => 
        typeof m === 'object' && m.name === `${testPrefix}_system`
      );
      
      if (testMeasurement) {
        expect(testMeasurement).toHaveProperty('series_count');
        expect(testMeasurement).toHaveProperty('field_count');
        expect(testMeasurement).toHaveProperty('tag_count');
      }
    });
  });

  describe('GET /tags', () => {
    test('Returns all tag keys and values for a measurement', async () => {
      const response = await axios.get(`${BASE_URL}/tags?measurement=${testPrefix}_system`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('tags');
      expect(response.data.measurement).toBe(`${testPrefix}_system`);
      
      // Check tag keys
      const tagKeys = Object.keys(response.data.tags);
      expect(tagKeys).toContain('host');
      expect(tagKeys).toContain('datacenter');
      expect(tagKeys).toContain('environment');
      
      // Check tag values
      expect(response.data.tags.host).toContain('server01');
      expect(response.data.tags.host).toContain('server02');
      expect(response.data.tags.host).toContain('server03');
      
      expect(response.data.tags.datacenter).toContain('us-east');
      expect(response.data.tags.datacenter).toContain('us-west');
      
      expect(response.data.tags.environment).toContain('production');
      expect(response.data.tags.environment).toContain('staging');
    });

    test('Returns specific tag values when tag key is provided', async () => {
      const response = await axios.get(`${BASE_URL}/tags?measurement=${testPrefix}_system&tag=datacenter`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('values');
      expect(response.data.measurement).toBe(`${testPrefix}_system`);
      expect(response.data.tag).toBe('datacenter');
      
      expect(response.data.values).toContain('us-east');
      expect(response.data.values).toContain('us-west');
    });

    test('Returns empty result for non-existent measurement', async () => {
      const response = await axios.get(`${BASE_URL}/tags?measurement=non_existent_measurement_xyz`);
      
      expect(response.status).toBe(200);
      expect(response.data.tags).toEqual({});
    });

    test('Returns error when measurement parameter is missing', async () => {
      try {
        await axios.get(`${BASE_URL}/tags`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data).toHaveProperty('error');
      }
    });

    test('Returns tag cardinality when requested', async () => {
      const response = await axios.get(`${BASE_URL}/tags?measurement=${testPrefix}_application&include_cardinality=true`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('tags');
      
      // Should include cardinality information
      if (response.data.cardinality) {
        expect(response.data.cardinality).toHaveProperty('total_series');
        expect(response.data.cardinality).toHaveProperty('tag_cardinality');
      }
    });
  });

  describe('GET /fields', () => {
    test('Returns all fields for a measurement', async () => {
      const response = await axios.get(`${BASE_URL}/fields?measurement=${testPrefix}_system`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('fields');
      expect(response.data.measurement).toBe(`${testPrefix}_system`);
      
      // Check that all fields across different series are included
      const fields = response.data.fields;
      expect(fields).toHaveProperty('cpu_usage');
      expect(fields).toHaveProperty('memory_usage');
      expect(fields).toHaveProperty('disk_usage');
      expect(fields).toHaveProperty('load_average');
      expect(fields).toHaveProperty('temperature'); // From server03
      
      // Check field types
      expect(fields.cpu_usage.type).toBe('float');
      expect(fields.memory_usage.type).toBe('float');
      expect(fields.temperature.type).toBe('float');
    });

    test('Returns fields with mixed data types', async () => {
      const response = await axios.get(`${BASE_URL}/fields?measurement=${testPrefix}_business`);
      
      expect(response.status).toBe(200);
      expect(response.data.fields).toHaveProperty('revenue');
      expect(response.data.fields).toHaveProperty('transactions');
      expect(response.data.fields).toHaveProperty('is_open');
      expect(response.data.fields).toHaveProperty('manager');
      
      // Check different data types
      expect(response.data.fields.revenue.type).toBe('float');
      expect(response.data.fields.transactions.type).toBe('integer');
      expect(response.data.fields.is_open.type).toBe('boolean');
      expect(response.data.fields.manager.type).toBe('string');
    });

    test('Returns field statistics when requested', async () => {
      const response = await axios.get(`${BASE_URL}/fields?measurement=${testPrefix}_sensor&include_stats=true`);
      
      expect(response.status).toBe(200);
      expect(response.data.fields).toHaveProperty('value');
      
      // Should include statistics for numeric fields
      const valueField = response.data.fields.value;
      if (valueField.stats) {
        expect(valueField.stats).toHaveProperty('min');
        expect(valueField.stats).toHaveProperty('max');
        expect(valueField.stats).toHaveProperty('mean');
        expect(valueField.stats).toHaveProperty('count');
      }
    });

    test('Returns empty result for non-existent measurement', async () => {
      const response = await axios.get(`${BASE_URL}/fields?measurement=non_existent_measurement_xyz`);
      
      expect(response.status).toBe(200);
      expect(response.data.fields).toEqual({});
    });

    test('Returns error when measurement parameter is missing', async () => {
      try {
        await axios.get(`${BASE_URL}/fields`);
        fail('Should have thrown an error');
      } catch (error) {
        expect(error.response.status).toBe(400);
        expect(error.response.data).toHaveProperty('error');
      }
    });

    test('Filters fields by tag when provided', async () => {
      const response = await axios.get(`${BASE_URL}/fields?measurement=${testPrefix}_system&tags=environment:production`);
      
      expect(response.status).toBe(200);
      expect(response.data).toHaveProperty('fields');
      expect(response.data).toHaveProperty('filtered_by');
      expect(response.data.filtered_by).toEqual({ environment: 'production' });
      
      // Should only include fields from production servers (server01 and server02)
      expect(response.data.fields).toHaveProperty('cpu_usage');
      expect(response.data.fields).toHaveProperty('memory_usage');
      expect(response.data.fields).toHaveProperty('disk_usage');
      // temperature should not be included (only in staging)
    });
  });

  describe('Cross-endpoint consistency', () => {
    test('Measurements returned by /measurements exist in /tags and /fields', async () => {
      const measurementsResponse = await axios.get(`${BASE_URL}/measurements?prefix=${testPrefix}`);
      const measurements = measurementsResponse.data.measurements;
      
      for (const measurement of measurements) {
        // Check tags endpoint
        const tagsResponse = await axios.get(`${BASE_URL}/tags?measurement=${measurement}`);
        expect(tagsResponse.status).toBe(200);
        expect(tagsResponse.data.measurement).toBe(measurement);
        
        // Check fields endpoint
        const fieldsResponse = await axios.get(`${BASE_URL}/fields?measurement=${measurement}`);
        expect(fieldsResponse.status).toBe(200);
        expect(fieldsResponse.data.measurement).toBe(measurement);
        
        // At least one of tags or fields should have data
        const hasTags = Object.keys(tagsResponse.data.tags).length > 0;
        const hasFields = Object.keys(fieldsResponse.data.fields).length > 0;
        expect(hasTags || hasFields).toBe(true);
      }
    });
  });

  describe('Performance', () => {
    test('Metadata queries complete in reasonable time', async () => {
      const start = Date.now();
      
      // Run multiple metadata queries
      await Promise.all([
        axios.get(`${BASE_URL}/measurements`),
        axios.get(`${BASE_URL}/tags?measurement=${testPrefix}_system`),
        axios.get(`${BASE_URL}/fields?measurement=${testPrefix}_system`)
      ]);
      
      const duration = Date.now() - start;
      expect(duration).toBeLessThan(1000); // Should complete within 1 second
    });
  });

  // Clean up test data after tests
  afterAll(async () => {
    // Note: In a real implementation, we might want to delete the test data
    // For now, we'll leave it as the data is namespaced with timestamp
  });
});