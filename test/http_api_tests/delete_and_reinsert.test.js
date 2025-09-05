const axios = require('axios');

const BASE_URL = 'http://localhost:8086';

describe('Delete and Reinsert Operations', () => {
  let testData = [];
  let deletedData = [];
  
  beforeAll(async () => {
    // Generate test data: temperature readings every hour for 3 days
    const startTime = 1704067200000000000; // Jan 1, 2024 00:00:00 in nanoseconds
    const hourInNanos = 3600000000000; // 1 hour in nanoseconds
    
    for (let i = 0; i < 72; i++) { // 72 hours = 3 days
      testData.push({
        measurement: 'sensor_data',
        tags: {
          location: 'warehouse-1',
          sensor_id: 'temp-sensor-01'
        },
        fields: {
          temperature: 20.0 + Math.sin(i / 12 * Math.PI) * 5, // Sine wave between 15-25°C
          humidity: 50.0 + Math.cos(i / 12 * Math.PI) * 10 // Cosine wave between 40-60%
        },
        timestamp: startTime + (i * hourInNanos)
      });
    }
    
    // Store the data we'll delete (hours 24-47, i.e., second day)
    deletedData = testData.slice(24, 48);
  });
  
  test('Insert initial data, delete middle segment, verify deletion, then reinsert', async () => {
    // Step 1: Insert all initial data
    const insertResponse = await axios.post(`${BASE_URL}/write`, {
      writes: testData
    });
    expect(insertResponse.status).toBe(200);
    expect(insertResponse.data.status).toBe('success');
    expect(insertResponse.data.points_written).toBe(144); // 72 points * 2 fields
    
    // Step 2: Query all data to verify initial insertion
    const initialQueryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:sensor_data(temperature,humidity){location:warehouse-1,sensor_id:temp-sensor-01}',
      startTime: testData[0].timestamp,
      endTime: testData[testData.length - 1].timestamp + 1
    });
    
    expect(initialQueryResponse.status).toBe(200);
    expect(initialQueryResponse.data.status).toBe('success');
    expect(initialQueryResponse.data.series).toHaveLength(1);
    
    const initialSeries = initialQueryResponse.data.series[0];
    expect(initialSeries.fields.temperature.timestamps).toHaveLength(72);
    expect(initialSeries.fields.humidity.timestamps).toHaveLength(72);
    
    // Step 3: Delete the middle segment (second day of data)
    const deleteStartTime = testData[24].timestamp; // Start of day 2
    const deleteEndTime = testData[47].timestamp;   // End of day 2
    
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: 'sensor_data',
      tags: {
        location: 'warehouse-1',
        sensor_id: 'temp-sensor-01'
      },
      fields: ['temperature', 'humidity'],
      startTime: deleteStartTime,
      endTime: deleteEndTime
    });
    
    expect(deleteResponse.status).toBe(200);
    expect(deleteResponse.data.status).toBe('success');
    
    // Step 4: Query again to verify deletion
    const afterDeleteResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:sensor_data(temperature,humidity){location:warehouse-1,sensor_id:temp-sensor-01}',
      startTime: testData[0].timestamp,
      endTime: testData[testData.length - 1].timestamp + 1
    });
    
    expect(afterDeleteResponse.status).toBe(200);
    expect(afterDeleteResponse.data.status).toBe('success');
    expect(afterDeleteResponse.data.series).toHaveLength(1);
    
    const afterDeleteSeries = afterDeleteResponse.data.series[0];
    expect(afterDeleteSeries.fields.temperature.timestamps).toHaveLength(48); // 72 - 24 deleted
    expect(afterDeleteSeries.fields.humidity.timestamps).toHaveLength(48);
    
    // Verify the deleted timestamps are not present
    const remainingTimestamps = afterDeleteSeries.fields.temperature.timestamps;
    for (let i = 24; i < 48; i++) {
      expect(remainingTimestamps).not.toContain(testData[i].timestamp);
    }
    
    // Verify the first day and third day data are still present
    for (let i = 0; i < 24; i++) {
      expect(remainingTimestamps).toContain(testData[i].timestamp);
    }
    for (let i = 48; i < 72; i++) {
      expect(remainingTimestamps).toContain(testData[i].timestamp);
    }
    
    // Step 5: Reinsert the deleted data
    const reinsertResponse = await axios.post(`${BASE_URL}/write`, {
      writes: deletedData
    });
    
    expect(reinsertResponse.status).toBe(200);
    expect(reinsertResponse.data.status).toBe('success');
    expect(reinsertResponse.data.points_written).toBe(48); // 24 points * 2 fields
    
    // Step 6: Query final data to verify reinsertion
    const finalQueryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:sensor_data(temperature,humidity){location:warehouse-1,sensor_id:temp-sensor-01}',
      startTime: testData[0].timestamp,
      endTime: testData[testData.length - 1].timestamp + 1
    });
    
    expect(finalQueryResponse.status).toBe(200);
    expect(finalQueryResponse.data.status).toBe('success');
    expect(finalQueryResponse.data.series).toHaveLength(1);
    
    const finalSeries = finalQueryResponse.data.series[0];
    expect(finalSeries.fields.temperature.timestamps).toHaveLength(72); // Back to original count
    expect(finalSeries.fields.humidity.timestamps).toHaveLength(72);
    
    // Verify all timestamps are present again
    const finalTimestamps = finalSeries.fields.temperature.timestamps;
    for (let i = 0; i < 72; i++) {
      expect(finalTimestamps).toContain(testData[i].timestamp);
    }
    
    // Verify the data values match what was reinserted
    for (let i = 24; i < 48; i++) {
      const idx = finalTimestamps.indexOf(testData[i].timestamp);
      expect(idx).not.toBe(-1);
      // Check temperature value is close to what we inserted (accounting for floating point)
      expect(Math.abs(finalSeries.fields.temperature.values[idx] - testData[i].fields.temperature)).toBeLessThan(0.001);
      expect(Math.abs(finalSeries.fields.humidity.values[idx] - testData[i].fields.humidity)).toBeLessThan(0.001);
    }
  });
  
  test('Delete with partial field selection', async () => {
    // Insert multi-field data
    const multiFieldData = {
      measurement: 'multi_sensor',
      tags: {
        device: 'sensor-02'
      },
      fields: {
        temperature: 22.5,
        humidity: 55.0,
        pressure: 1013.25
      },
      timestamp: 1704067200000000000
    };
    
    const insertResponse = await axios.post(`${BASE_URL}/write`, multiFieldData);
    expect(insertResponse.status).toBe(200);
    
    // Delete only temperature field
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: 'multi_sensor',
      tags: {
        device: 'sensor-02'
      },
      fields: ['temperature'],
      startTime: 1704067200000000000,
      endTime: 1704067200000000000
    });
    
    expect(deleteResponse.status).toBe(200);
    expect(deleteResponse.data.status).toBe('success');
    
    // Query and verify only temperature is deleted
    const queryResponse = await axios.post(`${BASE_URL}/query`, {
      query: 'avg:multi_sensor(temperature,humidity,pressure){device:sensor-02}',
      startTime: 1704067200000000000,
      endTime: 1704067200000000001
    });
    
    expect(queryResponse.status).toBe(200);
    expect(queryResponse.data.series).toHaveLength(1);
    
    const series = queryResponse.data.series[0];
    // Temperature should be missing or empty
    if (series.fields.temperature) {
      expect(series.fields.temperature.timestamps).toHaveLength(0);
    }
    // Humidity and pressure should still be present
    expect(series.fields.humidity.timestamps).toHaveLength(1);
    expect(series.fields.pressure.timestamps).toHaveLength(1);
    expect(series.fields.humidity.values[0]).toBe(55.0);
    expect(series.fields.pressure.values[0]).toBe(1013.25);
  });
  
  test('Delete non-existent data returns success', async () => {
    // Try to delete data that doesn't exist
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: 'non_existent_measurement',
      tags: {
        fake: 'tag'
      },
      fields: ['fake_field'],
      startTime: 1704067200000000000,
      endTime: 1704067300000000000
    });
    
    // Should still return success (idempotent operation)
    expect(deleteResponse.status).toBe(200);
    expect(deleteResponse.data.status).toBe('success');
  });
  
  test('Delete without time range deletes all matching series data', async () => {
    // Insert test data
    const testMeasurement = 'delete_all_test';
    const dataPoints = [];
    for (let i = 0; i < 10; i++) {
      dataPoints.push({
        measurement: testMeasurement,
        tags: {
          test: 'delete-all'
        },
        fields: {
          value: i * 10
        },
        timestamp: 1704067200000000000 + (i * 1000000000)
      });
    }
    
    const insertResponse = await axios.post(`${BASE_URL}/write`, {
      writes: dataPoints
    });
    expect(insertResponse.status).toBe(200);
    
    // Verify data exists
    const beforeDeleteResponse = await axios.post(`${BASE_URL}/query`, {
      query: `avg:${testMeasurement}(value){test:delete-all}`,
      startTime: 1704067200000000000,
      endTime: 1704067210000000000
    });
    expect(beforeDeleteResponse.data.series[0].fields.value.timestamps).toHaveLength(10);
    
    // Delete without specifying time range
    const deleteResponse = await axios.post(`${BASE_URL}/delete`, {
      measurement: testMeasurement,
      tags: {
        test: 'delete-all'
      },
      fields: ['value']
      // No startTime or endTime specified
    });
    
    expect(deleteResponse.status).toBe(200);
    expect(deleteResponse.data.status).toBe('success');
    
    // Verify all data is deleted
    const afterDeleteResponse = await axios.post(`${BASE_URL}/query`, {
      query: `avg:${testMeasurement}(value){test:delete-all}`,
      startTime: 1704067200000000000,
      endTime: 1704067210000000000
    });
    
    // Should either have no series or empty timestamps
    if (afterDeleteResponse.data.series && afterDeleteResponse.data.series.length > 0) {
      const series = afterDeleteResponse.data.series[0];
      if (series.fields.value) {
        expect(series.fields.value.timestamps).toHaveLength(0);
      }
    }
  });
});