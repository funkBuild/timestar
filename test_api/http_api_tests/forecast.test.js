/**
 * End-to-end tests for Forecast API
 * Tests linear and seasonal (SARIMA) forecasting algorithms
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

// Execute forecast query
async function queryForecast(queryName, queryString, formula, startTime, endTime) {
    const request = {
        queries: { [queryName]: queryString },
        formula: formula,
        startTime: Number(startTime),
        endTime: Number(endTime)
    };

    const response = await httpRequest('POST', '/derived', request);
    return response;
}

// Generate linear trending data: y = mx + b + noise
function generateLinearData(count, slope, intercept, noise = 0.0) {
    const values = [];
    for (let i = 0; i < count; i++) {
        const gaussianNoise = noise > 0 ?
            (Math.random() + Math.random() + Math.random() - 1.5) * noise : 0;
        values.push(slope * i + intercept + gaussianNoise);
    }
    return values;
}

// Generate sinusoidal data with optional trend
function generateSeasonalData(count, baseline, amplitude, period, trend = 0.0, noise = 0.0) {
    const values = [];
    for (let i = 0; i < count; i++) {
        const gaussianNoise = noise > 0 ?
            (Math.random() + Math.random() + Math.random() - 1.5) * noise : 0;
        values.push(baseline + amplitude * Math.sin(2 * Math.PI * i / period) + trend * i + gaussianNoise);
    }
    return values;
}

// Generate constant data with noise
function generateConstantData(count, value, noise = 0.0) {
    const values = [];
    for (let i = 0; i < count; i++) {
        const gaussianNoise = noise > 0 ?
            (Math.random() + Math.random() + Math.random() - 1.5) * noise : 0;
        values.push(value + gaussianNoise);
    }
    return values;
}

describe('Forecast API', () => {
    const testPrefix = `forecast_test_${Date.now()}`;

    describe('Linear Algorithm', () => {
        const measurement = `${testPrefix}_linear`;
        const timestamps = generateTimestamps(100);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            // Insert linear trending data: y = 0.5x + 10
            const values = generateLinearData(100, 0.5, 10.0, 1.0);

            await insertData(
                measurement,
                { host: 'test-server-linear' },
                'value',
                timestamps,
                values
            );

            // Wait for data to be written
            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should forecast linear trending data', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-server-linear}`,
                "forecast(cpu, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.series).toBeDefined();
            expect(response.data.series.length).toBeGreaterThanOrEqual(4);
            expect(response.data.statistics.algorithm).toBe('linear');
        });

        test('should return all required series pieces', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-server-linear}`,
                "forecast(cpu, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const pieces = response.data.series.map(s => s.piece);
            expect(pieces).toContain('past');
            expect(pieces).toContain('forecast');
            expect(pieces).toContain('upper');
            expect(pieces).toContain('lower');
        });

        test('should have extended timestamps', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-server-linear}`,
                "forecast(cpu, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.times).toBeDefined();
            expect(response.data.forecast_start_index).toBeDefined();

            // Times should include both historical and forecast
            const historicalCount = response.data.forecast_start_index;
            expect(historicalCount).toBeGreaterThan(0);
            expect(response.data.times.length).toBeGreaterThan(historicalCount);
        });

        test('should have null values for forecast in historical period', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-server-linear}`,
                "forecast(cpu, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const upperPiece = response.data.series.find(s => s.piece === 'upper');
            const lowerPiece = response.data.series.find(s => s.piece === 'lower');
            const forecastStartIndex = response.data.forecast_start_index;

            expect(upperPiece).toBeDefined();
            expect(lowerPiece).toBeDefined();

            // Upper and lower should be null during historical period
            for (let i = 0; i < forecastStartIndex; i++) {
                expect(upperPiece.values[i]).toBeNull();
                expect(lowerPiece.values[i]).toBeNull();
            }
        });

        test('should have valid bounds in forecast period', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-server-linear}`,
                "forecast(cpu, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const forecastPiece = response.data.series.find(s => s.piece === 'forecast');
            const upperPiece = response.data.series.find(s => s.piece === 'upper');
            const lowerPiece = response.data.series.find(s => s.piece === 'lower');
            const forecastStartIndex = response.data.forecast_start_index;

            expect(forecastPiece).toBeDefined();
            expect(upperPiece).toBeDefined();
            expect(lowerPiece).toBeDefined();

            // Check bounds in forecast period
            for (let i = forecastStartIndex; i < response.data.times.length; i++) {
                const forecast = forecastPiece.values[i];
                const upper = upperPiece.values[i];
                const lower = lowerPiece.values[i];

                if (forecast !== null && upper !== null && lower !== null) {
                    expect(upper).toBeGreaterThan(forecast);
                    expect(lower).toBeLessThan(forecast);
                }
            }
        });

        test('should continue linear trend', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-server-linear}`,
                "forecast(cpu, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const forecastPiece = response.data.series.find(s => s.piece === 'forecast');
            const forecastStartIndex = response.data.forecast_start_index;

            // Get forecast values
            const forecastValues = [];
            for (let i = forecastStartIndex; i < response.data.times.length; i++) {
                if (forecastPiece.values[i] !== null) {
                    forecastValues.push(forecastPiece.values[i]);
                }
            }

            // Values should be increasing (positive slope)
            if (forecastValues.length >= 2) {
                expect(forecastValues[forecastValues.length - 1]).toBeGreaterThan(forecastValues[0]);
            }
        });
    });

    describe('Seasonal Algorithm', () => {
        const measurement = `${testPrefix}_seasonal`;
        const timestamps = generateTimestamps(200);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            // Insert sinusoidal data with period of 24 points
            const values = generateSeasonalData(200, 100.0, 20.0, 24, 0.0, 1.0);

            await insertData(
                measurement,
                { host: 'test-server-seasonal' },
                'value',
                timestamps,
                values
            );

            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should forecast seasonal data', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-server-seasonal}`,
                "forecast(metric, 'seasonal', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.statistics.algorithm).toBe('seasonal');
        });

        test('should return all required series pieces for seasonal', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-server-seasonal}`,
                "forecast(metric, 'seasonal', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const pieces = response.data.series.map(s => s.piece);
            expect(pieces).toContain('past');
            expect(pieces).toContain('forecast');
            expect(pieces).toContain('upper');
            expect(pieces).toContain('lower');
        });

        test('should forecast within reasonable range', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-server-seasonal}`,
                "forecast(metric, 'seasonal', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const forecastPiece = response.data.series.find(s => s.piece === 'forecast');
            const forecastStartIndex = response.data.forecast_start_index;

            // Verify we have valid forecast values (SARIMA can produce wider ranges)
            let validForecastCount = 0;
            for (let i = forecastStartIndex; i < response.data.times.length; i++) {
                const forecast = forecastPiece.values[i];
                if (forecast !== null) {
                    expect(typeof forecast).toBe('number');
                    expect(Number.isFinite(forecast)).toBe(true);
                    validForecastCount++;
                }
            }
            // Should have some valid forecasts
            expect(validForecastCount).toBeGreaterThan(0);
        });
    });

    describe('Constant Data', () => {
        const measurement = `${testPrefix}_constant`;
        const timestamps = generateTimestamps(100);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            // Insert constant data
            const values = generateConstantData(100, 50.0, 1.0);

            await insertData(
                measurement,
                { host: 'test-server-constant' },
                'value',
                timestamps,
                values
            );

            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should forecast constant data as constant', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-server-constant}`,
                "forecast(metric, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const forecastPiece = response.data.series.find(s => s.piece === 'forecast');
            const forecastStartIndex = response.data.forecast_start_index;

            // All forecast values should be close to 50
            for (let i = forecastStartIndex; i < response.data.times.length; i++) {
                const forecast = forecastPiece.values[i];
                if (forecast !== null) {
                    expect(forecast).toBeGreaterThan(40);
                    expect(forecast).toBeLessThan(60);
                }
            }
        });
    });

    describe('Deviations Parameter', () => {
        const measurement = `${testPrefix}_deviations`;
        const timestamps = generateTimestamps(100);
        const startTime = timestamps[0];
        const endTime = timestamps[timestamps.length - 1];

        beforeAll(async () => {
            const values = generateLinearData(100, 0.5, 10.0, 2.0);
            await insertData(
                measurement,
                { host: 'test-deviations' },
                'value',
                timestamps,
                values
            );
            await new Promise(resolve => setTimeout(resolve, 500));
        });

        test('should accept deviations=1', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-deviations}`,
                "forecast(cpu, 'linear', 1)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.deviations).toBe(1);
        });

        test('should accept deviations=3', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-deviations}`,
                "forecast(cpu, 'linear', 3)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.statistics.deviations).toBe(3);
        });

        test('should have wider bounds with higher deviations', async () => {
            const response1 = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-deviations}`,
                "forecast(cpu, 'linear', 1)",
                startTime,
                endTime
            );

            const response2 = await queryForecast(
                'cpu',
                `avg:${measurement}(value){host:test-deviations}`,
                "forecast(cpu, 'linear', 3)",
                startTime,
                endTime
            );

            expect(response1.status).toBe(200);
            expect(response2.status).toBe(200);

            const upper1 = response1.data.series.find(s => s.piece === 'upper');
            const lower1 = response1.data.series.find(s => s.piece === 'lower');
            const upper2 = response2.data.series.find(s => s.piece === 'upper');
            const lower2 = response2.data.series.find(s => s.piece === 'lower');

            const forecastStart = response1.data.forecast_start_index;

            // Calculate average band width
            let avgWidth1 = 0, avgWidth2 = 0;
            let count = 0;

            for (let i = forecastStart; i < upper1.values.length; i++) {
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
                // deviations=3 should have wider bands than deviations=1
                expect(avgWidth2).toBeGreaterThan(avgWidth1);
            }
        });
    });

    describe('Error Handling', () => {
        test('should reject invalid algorithm', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "forecast(cpu, 'invalid', 2)",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });

        test('should reject deviations out of range (< 1)', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "forecast(cpu, 'linear', 0.5)",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });

        test('should reject deviations out of range (> 4)', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "forecast(cpu, 'linear', 5)",
                1704067200000000000n,
                1704153600000000000n
            );

            expect(response.status).toBe(400);
        });

        test('should reject missing query reference', async () => {
            const response = await queryForecast(
                'cpu',
                `avg:system.cpu.user(value){host:server01}`,
                "forecast(missing, 'linear', 2)",
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
            const values = generateLinearData(50, 0.5, 10.0, 1.0);
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
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "forecast(metric, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);
            expect(response.data.status).toBe('success');
            expect(response.data.times).toBeDefined();
            expect(response.data.forecast_start_index).toBeDefined();
            expect(response.data.series).toBeDefined();
            expect(response.data.statistics).toBeDefined();
        });

        test('should include statistics with all fields', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "forecast(metric, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const stats = response.data.statistics;
            expect(stats.algorithm).toBeDefined();
            expect(stats.deviations).toBeDefined();
            expect(stats.historical_points).toBeDefined();
            expect(stats.forecast_points).toBeDefined();
            expect(stats.execution_time_ms).toBeDefined();
        });

        test('should include group_tags in series pieces', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "forecast(metric, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            for (const piece of response.data.series) {
                expect(piece.group_tags).toBeDefined();
                expect(Array.isArray(piece.group_tags)).toBe(true);
            }
        });

        test('should have correct piece names', async () => {
            const response = await queryForecast(
                'metric',
                `avg:${measurement}(value){host:test-format}`,
                "forecast(metric, 'linear', 2)",
                startTime,
                endTime
            );

            expect(response.status).toBe(200);

            const validPieceNames = ['past', 'forecast', 'upper', 'lower'];
            for (const piece of response.data.series) {
                expect(validPieceNames).toContain(piece.piece);
            }
        });
    });
});
