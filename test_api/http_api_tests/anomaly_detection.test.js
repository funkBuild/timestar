/**
 * End-to-end tests for Anomaly Detection API
 * Tests all three algorithms (basic, agile, robust) with synthetic data
 */

const http = require('http');

const BASE_URL = 'http://localhost:8086';

// Helper to make HTTP requests
function httpRequest(method, path, body = null) {
    return new Promise((resolve, reject) => {
        const url = new URL(path, BASE_URL);
        const options = {
            hostname: url.hostname,
            port: url.port,
            path: url.pathname,
            method: method,
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    resolve({ status: res.statusCode, data: JSON.parse(data) });
                } catch (e) {
                    resolve({ status: res.statusCode, data: data });
                }
            });
        });

        req.on('error', reject);
        if (body) req.write(JSON.stringify(body));
        req.end();
    });
}

// Generate timestamps (1-minute intervals, nanoseconds)
function generateTimestamps(count, startNs = 1704067200000000000n) {
    const interval = 60000000000n; // 1 minute in nanoseconds
    const timestamps = [];
    for (let i = 0; i < count; i++) {
        timestamps.push(startNs + BigInt(i) * interval);
    }
    return timestamps;
}

// Insert test data
async function insertData(measurement, tags, field, timestamps, values) {
    const writes = timestamps.map((ts, i) => ({
        measurement,
        tags,
        fields: { [field]: values[i] },
        timestamp: Number(ts)
    }));

    // Insert in batches of 100
    for (let i = 0; i < writes.length; i += 100) {
        const batch = writes.slice(i, i + 100);
        const response = await httpRequest('POST', '/write', { writes: batch });
        if (response.status !== 200 && response.status !== 204) {
            throw new Error(`Failed to insert data: ${JSON.stringify(response.data)}`);
        }
    }
}

// Execute anomaly detection query
async function queryAnomalies(queryName, queryString, formula, startTime, endTime) {
    const request = {
        queries: { [queryName]: queryString },
        formula: formula,
        startTime: Number(startTime),
        endTime: Number(endTime)
    };

    const response = await httpRequest('POST', '/derived', request);
    return response;
}

// Generate constant series with known anomalies
function generateConstantWithAnomalies(count, baseline, anomalyPositions, anomalyDeviation) {
    const values = new Array(count).fill(baseline);
    for (const pos of anomalyPositions) {
        if (pos < count) {
            values[pos] = baseline + anomalyDeviation;
        }
    }
    return values;
}

// Generate sinusoidal series (for seasonal tests)
function generateSinusoidal(count, baseline, amplitude, period) {
    const values = [];
    for (let i = 0; i < count; i++) {
        values.push(baseline + amplitude * Math.sin(2 * Math.PI * i / period));
    }
    return values;
}

// Generate series with level shift
function generateLevelShift(count, level1, level2, shiftPoint) {
    const values = [];
    for (let i = 0; i < count; i++) {
        values.push(i < shiftPoint ? level1 : level2);
    }
    return values;
}

// Add Gaussian noise
function addNoise(values, stddev) {
    return values.map(v => {
        // Box-Muller transform for Gaussian noise
        const u1 = Math.random();
        const u2 = Math.random();
        const noise = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2) * stddev;
        return v + noise;
    });
}

describe('Anomaly Detection API', () => {
    const testPrefix = `anomaly_test_${Date.now()}`;

    describe('Basic Algorithm', () => {
        const measurement = `${testPrefix}_basic`;
        const timestamps = generateTimestamps(200);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            // Insert constant series with clear anomalies at positions 100, 150
            const values = generateConstantWithAnomalies(200, 50.0, [100, 150], 50.0);
            const noisyValues = addNoise(values, 2.0);

            await insertData(
                measurement,
                { host: 'test-server-basic' },
                'value',
                timestamps,
                noisyValues
            );

            // Wait for data to be written
            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should detect anomalies in constant series', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-server-basic}`,
                "anomalies(cpu, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.series).toBeDefined();
            expect(response.data.series.length).toBeGreaterThanOrEqual(4);

            // Find the scores piece
            const scoresPiece = response.data.series.find(s => s.piece === 'scores');
            expect(scoresPiece).toBeDefined();

            // Check that we detected anomalies
            expect(response.data.statistics.anomaly_count).toBeGreaterThan(0);
            expect(response.data.statistics.algorithm).toBe('basic');
        });

        test('should return all required series pieces', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-server-basic}`,
                "anomalies(cpu, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const pieces = response.data.series.map(s => s.piece);
            expect(pieces).toContain('raw');
            expect(pieces).toContain('upper');
            expect(pieces).toContain('lower');
            expect(pieces).toContain('scores');
        });

        test('should have valid bounds (upper > lower)', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-server-basic}`,
                "anomalies(cpu, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const upperPiece = response.data.series.find(s => s.piece === 'upper');
            const lowerPiece = response.data.series.find(s => s.piece === 'lower');

            expect(upperPiece).toBeDefined();
            expect(lowerPiece).toBeDefined();

            // Skip warmup period and check bounds
            const skipPoints = 20;
            for (let i = skipPoints; i < upperPiece.values.length; i++) {
                const upper = upperPiece.values[i];
                const lower = lowerPiece.values[i];
                if (upper !== null && lower !== null) {
                    expect(upper).toBeGreaterThan(lower);
                }
            }
        });

        test('should detect known anomaly positions', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-server-basic}`,
                "anomalies(cpu, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const scoresPiece = response.data.series.find(s => s.piece === 'scores');
            expect(scoresPiece).toBeDefined();

            // Anomalies were inserted at positions 100 and 150
            // Check that at least one of these has a non-zero score
            const score100 = scoresPiece.values[100];
            const score150 = scoresPiece.values[150];

            // At least one should be detected (accounting for noise)
            expect(score100 > 0 || score150 > 0).toBe(true);
        });
    });

    describe('Robust Algorithm', () => {
        const measurement = `${testPrefix}_robust`;
        const timestamps = generateTimestamps(300);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            // Insert sinusoidal series with anomalies
            // Period of 60 points = hourly pattern with 1-minute intervals
            let values = generateSinusoidal(300, 100.0, 20.0, 60);
            values = addNoise(values, 2.0);

            // Insert anomalies that break the seasonal pattern
            values[180] = 200.0; // Large spike
            values[240] = 20.0;  // Large drop

            await insertData(
                measurement,
                { host: 'test-server-robust' },
                'value',
                timestamps,
                values
            );

            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should handle seasonal data with robust algorithm', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-robust}`,
                "anomalies(metric, 'robust', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.statistics.algorithm).toBe('robust');
        });

        test('should detect anomalies breaking seasonal pattern', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-robust}`,
                "anomalies(metric, 'robust', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const scoresPiece = response.data.series.find(s => s.piece === 'scores');
            expect(scoresPiece).toBeDefined();

            // Should detect the anomalies at 180 and 240
            expect(response.data.statistics.anomaly_count).toBeGreaterThan(0);
        });

        test('should support hourly seasonality', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-robust}`,
                "anomalies(metric, 'robust', 2, 'hourly')",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.statistics.seasonality).toBe('hourly');
        });

        test('should have predictions piece for robust algorithm', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-robust}`,
                "anomalies(metric, 'robust', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const predictionsPiece = response.data.series.find(s => s.piece === 'predictions');
            expect(predictionsPiece).toBeDefined();
            expect(predictionsPiece.values.length).toBe(response.data.times.length);
        });
    });

    describe('Agile Algorithm', () => {
        const measurement = `${testPrefix}_agile`;
        const timestamps = generateTimestamps(200);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            // Insert series with level shift
            // First 100 points at level 50, next 100 at level 100
            let values = generateLevelShift(200, 50.0, 100.0, 100);
            values = addNoise(values, 3.0);

            await insertData(
                measurement,
                { host: 'test-server-agile' },
                'value',
                timestamps,
                values
            );

            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should handle level shifts with agile algorithm', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-agile}`,
                "anomalies(metric, 'agile', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.statistics.algorithm).toBe('agile');
        });

        test('should adapt to new level after shift', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-agile}`,
                "anomalies(metric, 'agile', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const scoresPiece = response.data.series.find(s => s.piece === 'scores');
            const upperPiece = response.data.series.find(s => s.piece === 'upper');
            const lowerPiece = response.data.series.find(s => s.piece === 'lower');

            expect(scoresPiece).toBeDefined();
            expect(upperPiece).toBeDefined();
            expect(lowerPiece).toBeDefined();

            // After adapting to the new level (last 50 points),
            // the bounds should be around the new level (100)
            const lastPoints = 20;
            const startIdx = upperPiece.values.length - lastPoints;

            let avgUpper = 0;
            let avgLower = 0;
            for (let i = startIdx; i < upperPiece.values.length; i++) {
                avgUpper += upperPiece.values[i];
                avgLower += lowerPiece.values[i];
            }
            avgUpper /= lastPoints;
            avgLower /= lastPoints;

            // The average bounds should be around 100 (the new level)
            const midpoint = (avgUpper + avgLower) / 2;
            expect(midpoint).toBeGreaterThan(80);  // Should have adapted up from 50
            expect(midpoint).toBeLessThan(120);    // Should be around 100
        });

        test('should detect level shift as initial anomalies', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-server-agile}`,
                "anomalies(metric, 'agile', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            // The level shift should cause some anomalies to be detected
            expect(response.data.statistics.anomaly_count).toBeGreaterThan(0);
        });
    });

    describe('Bounds Parameter Validation', () => {
        const measurement = `${testPrefix}_bounds`;
        const timestamps = generateTimestamps(100);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            const values = addNoise(new Array(100).fill(50.0), 5.0);
            await insertData(
                measurement,
                { host: 'test-bounds' },
                'value',
                timestamps,
                values
            );
            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should accept bounds=1', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-bounds}`,
                "anomalies(cpu, 'basic', 1)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.bounds).toBe(1);
        });

        test('should accept bounds=4', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-bounds}`,
                "anomalies(cpu, 'basic', 4)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.bounds).toBe(4);
        });

        test('should have wider bounds with higher bounds parameter', async () => {
            const response1 = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-bounds}`,
                "anomalies(cpu, 'basic', 1)",
                startTime,
                endTime
            );

            const response2 = await queryAnomalies(
                'cpu',
                `avg:${measurement}(value){host:test-bounds}`,
                "anomalies(cpu, 'basic', 3)",
                startTime,
                endTime
            );

            expect(response1.status).toBe(200);
            expect(response2.status).toBe(200);

            const upper1 = response1.data.series.find(s => s.piece === 'upper');
            const lower1 = response1.data.series.find(s => s.piece === 'lower');
            const upper2 = response2.data.series.find(s => s.piece === 'upper');
            const lower2 = response2.data.series.find(s => s.piece === 'lower');

            // Calculate average band width after warmup
            const skipPoints = 30;
            let avgWidth1 = 0, avgWidth2 = 0;
            let count = 0;

            for (let i = skipPoints; i < upper1.values.length; i++) {
                if (upper1.values[i] !== null && lower1.values[i] !== null &&
                    upper2.values[i] !== null && lower2.values[i] !== null) {
                    avgWidth1 += upper1.values[i] - lower1.values[i];
                    avgWidth2 += upper2.values[i] - lower2.values[i];
                    count++;
                }
            }

            if (count > 0) {
                avgWidth1 /= count;
                avgWidth2 /= count;
                // bounds=3 should have wider bands than bounds=1
                expect(avgWidth2).toBeGreaterThan(avgWidth1);
            }
        });
    });

    describe('Seasonality Options', () => {
        const measurement = `${testPrefix}_seasonal`;
        const timestamps = generateTimestamps(200);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            const values = addNoise(generateSinusoidal(200, 100.0, 20.0, 60), 2.0);
            await insertData(
                measurement,
                { host: 'test-seasonal' },
                'value',
                timestamps,
                values
            );
            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should support hourly seasonality', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-seasonal}`,
                "anomalies(metric, 'robust', 2, 'hourly')",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.seasonality).toBe('hourly');
        });

        test('should support daily seasonality', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-seasonal}`,
                "anomalies(metric, 'agile', 2, 'daily')",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.seasonality).toBe('daily');
        });

        test('should support weekly seasonality', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-seasonal}`,
                "anomalies(metric, 'robust', 2, 'weekly')",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.seasonality).toBe('weekly');
        });
    });

    describe('Error Handling', () => {
        test('should reject invalid algorithm', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "anomalies(cpu, 'invalid', 2)",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });

        test('should reject bounds out of range', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "anomalies(cpu, 'basic', 10)",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });

        test('should reject invalid seasonality', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "anomalies(cpu, 'agile', 2, 'monthly')",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });

        test('should reject missing query reference', async () => {
            const response = await queryAnomalies(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "anomalies(missing, 'basic', 2)",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });
    });

    describe('Response Format Validation', () => {
        const measurement = `${testPrefix}_format`;
        const timestamps = generateTimestamps(50);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            const values = addNoise(new Array(50).fill(50.0), 2.0);
            await insertData(
                measurement,
                { host: 'test-format', region: 'us-west' },
                'value',
                timestamps,
                values
            );
            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should include all required response fields', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "anomalies(metric, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.times).toBeDefined();
            expect(response.data.series).toBeDefined();
            expect(response.data.statistics).toBeDefined();
        });

        test('should include statistics with all fields', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "anomalies(metric, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const stats = response.data.statistics;
            expect(stats.algorithm).toBeDefined();
            expect(stats.bounds).toBeDefined();
            expect(stats.anomaly_count).toBeDefined();
            expect(stats.total_points).toBeDefined();
            expect(stats.execution_time_ms).toBeDefined();
        });

        test('should include group_tags in series pieces', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "anomalies(metric, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            for (const piece of response.data.series) {
                expect(piece.group_tags).toBeDefined();
                expect(Array.isArray(piece.group_tags)).toBe(true);
            }
        });

        test('should include alert_value in scores piece', async () => {
            const response = await queryAnomalies(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "anomalies(metric, 'basic', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const scoresPiece = response.data.series.find(s => s.piece === 'scores');
            expect(scoresPiece).toBeDefined();
            expect(scoresPiece.alert_value).toBeDefined();
            expect(typeof scoresPiece.alert_value).toBe('number');
        });
    });
});
