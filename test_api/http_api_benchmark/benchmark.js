const axios = require("axios");
const { performance } = require("perf_hooks");

// Configuration
const HOST = process.env.TSDB_HOST || "localhost";
const PORT = process.env.TSDB_PORT || 8086;
const BASE_URL = `http://${HOST}:${PORT}`;

// Benchmark configuration
const MEASUREMENT = "server.metrics";
const NUM_HOSTS = 10;
const NUM_RACKS = 2;
const FIELDS = [
  "cpu_usage",
  "memory_usage",
  "disk_io_read",
  "disk_io_write",
  "network_in",
  "network_out",
  "load_avg_1m",
  "load_avg_5m",
  "load_avg_15m",
  "temperature",
];

// Time configuration - 1 year of data at 1 minute intervals
const MINUTES_PER_YEAR = 365 * 24 * 60; // 525,600 minutes
const MINUTE_IN_NS = 60 * 1000000000; // 60 seconds in nanoseconds
const START_TIME = Date.now() * 1000000 - MINUTES_PER_YEAR * MINUTE_IN_NS; // 1 year ago

// Benchmark settings
const BENCHMARK_ITERATIONS = 10;
const BATCH_SIZE = 10000; // Points per batch insert

// Statistics tracking
class BenchmarkStats {
  constructor(name) {
    this.name = name;
    this.times = [];
  }

  addTime(ms) {
    this.times.push(ms);
  }

  getStats() {
    if (this.times.length === 0) return null;
    const sorted = [...this.times].sort((a, b) => a - b);
    return {
      name: this.name,
      min: sorted[0].toFixed(2),
      max: sorted[sorted.length - 1].toFixed(2),
      avg: (this.times.reduce((a, b) => a + b, 0) / this.times.length).toFixed(
        2
      ),
      median: sorted[Math.floor(sorted.length / 2)].toFixed(2),
      iterations: this.times.length,
    };
  }
}

// Generate server monitoring data
function generateDataPoint(hostId, rackId, timestamp) {
  const fields = {};
  FIELDS.forEach((field) => {
    // Generate realistic values for each field
    switch (field) {
      case "cpu_usage":
        fields[field] = 20 + Math.random() * 60; // 20-80%
        break;
      case "memory_usage":
        fields[field] = 30 + Math.random() * 50; // 30-80%
        break;
      case "disk_io_read":
      case "disk_io_write":
        fields[field] = Math.random() * 100; // 0-100 MB/s
        break;
      case "network_in":
      case "network_out":
        fields[field] = Math.random() * 1000; // 0-1000 Mbps
        break;
      case "load_avg_1m":
      case "load_avg_5m":
      case "load_avg_15m":
        fields[field] = Math.random() * 4; // 0-4 load
        break;
      case "temperature":
        fields[field] = 50 + Math.random() * 30; // 50-80°C
        break;
      default:
        fields[field] = Math.random() * 100;
    }
  });

  return {
    measurement: MEASUREMENT,
    tags: {
      host: `host-${hostId.toString().padStart(2, "0")}`,
      rack: `rack-${rackId}`,
    },
    fields: fields,
    timestamp: timestamp,
  };
}

// Helper function to generate a single batch write request
function generateBatchWriteRequest(batchSize) {
  const hostId = Math.floor(Math.random() * NUM_HOSTS) + 1;
  const rackId = Math.floor(Math.random() * NUM_RACKS) + 1;
  const writes = [];

  for (let j = 0; j < batchSize; j++) {
    const timestamp =
      START_TIME +
      Math.floor(Math.random() * MINUTES_PER_YEAR) * MINUTE_IN_NS;

    // Generate values for each field
    const fields = {};
    FIELDS.forEach((field) => {
      let value;
      switch (field) {
        case "cpu_usage":
          value = 20 + Math.random() * 60; // 20-80%
          break;
        case "memory_usage":
          value = 30 + Math.random() * 50; // 30-80%
          break;
        case "disk_io_read":
        case "disk_io_write":
          value = Math.random() * 100; // 0-100 MB/s
          break;
        case "network_in":
        case "network_out":
          value = Math.random() * 1000; // 0-1000 Mbps
          break;
        case "load_avg_1m":
        case "load_avg_5m":
        case "load_avg_15m":
          value = Math.random() * 4; // 0-4 load
          break;
        case "temperature":
          value = 50 + Math.random() * 30; // 50-80°C
          break;
        default:
          value = Math.random() * 100;
      }
      fields[field] = value;
    });

    writes.push({
      measurement: MEASUREMENT,
      tags: {
        host: `host-${hostId.toString().padStart(2, "0")}`,
        rack: `rack-${rackId}`,
      },
      fields: fields,
      timestamp: timestamp,
    });
  }

  return { writes };
}

// Benchmark batch inserts with specified concurrency
async function benchmarkBatchInsertsWithConcurrency(concurrency) {
  const stats = new BenchmarkStats(
    `Batch Insert (${BATCH_SIZE} points, concurrency=${concurrency})`
  );

  for (let i = 0; i < BENCHMARK_ITERATIONS; i++) {
    // Create multiple requests based on concurrency level
    const requests = [];
    for (let c = 0; c < concurrency; c++) {
      const writeRequest = generateBatchWriteRequest(BATCH_SIZE);
      requests.push(axios.post(`${BASE_URL}/write`, writeRequest));
    }

    const start = performance.now();
    try {
      await Promise.all(requests);
    } catch (error) {
      console.error("Batch insert failed:", error.message);
      if (error.response) {
        console.error("Response status:", error.response.status);
        console.error("Response data:", error.response.data);
      }
      process.exit(1);
    }
    const end = performance.now();

    stats.addTime(end - start);
    process.stdout.write(`\rIteration ${i + 1}/${BENCHMARK_ITERATIONS}`);
  }

  console.log("\n" + JSON.stringify(stats.getStats(), null, 2));

  // Calculate actual data points: each write has multiple fields
  const actualDataPoints = BATCH_SIZE * FIELDS.length * concurrency;
  const throughput = (actualDataPoints * 1000) / parseFloat(stats.getStats().avg);
  console.log(
    `Average throughput: ${throughput.toFixed(0)} points/sec (${concurrency} × ${BATCH_SIZE} writes × ${FIELDS.length} fields = ${actualDataPoints} data points)`
  );
  return stats;
}

// Run batch insert benchmarks with different concurrency levels
async function benchmarkBatchInserts() {
  console.log("\n=== BATCH INSERT BENCHMARK (CONCURRENCY COMPARISON) ===");
  console.log(`Batch size: ${BATCH_SIZE} points per request`);
  console.log(`Testing concurrency levels: 1, 2, 5`);

  const results = {};

  // Test with concurrency = 1
  console.log("\n--- Concurrency = 1 (sequential) ---");
  results.concurrency1 = await benchmarkBatchInsertsWithConcurrency(1);

  // Test with concurrency = 2
  console.log("\n--- Concurrency = 2 (parallel) ---");
  results.concurrency2 = await benchmarkBatchInsertsWithConcurrency(2);

  // Test with concurrency = 5
  console.log("\n--- Concurrency = 5 (parallel) ---");
  results.concurrency5 = await benchmarkBatchInsertsWithConcurrency(5);

  // Print comparison summary
  console.log("\n=== CONCURRENCY COMPARISON SUMMARY ===");
  console.log("Concurrency | Avg Latency (ms) | Throughput (points/sec)");
  console.log("-".repeat(65));

  [1, 2, 5].forEach((c) => {
    const stats = results[`concurrency${c}`].getStats();
    const dataPoints = BATCH_SIZE * FIELDS.length * c;
    const throughput = ((dataPoints * 1000) / parseFloat(stats.avg)).toFixed(0);
    console.log(
      `     ${c}      |      ${stats.avg.padStart(6)}      |  ${throughput.padStart(12)}`
    );
  });

  return results;
}

// Insert sample data for queries (1 year of data)
async function insertSampleData() {
  console.log("\n=== INSERTING SAMPLE DATA FOR QUERY BENCHMARKS ===");
  console.log(
    "This will insert 1 year of data for all hosts (525,600 points per host)..."
  );

  const yearAgo = Date.now() * 1000000 - MINUTES_PER_YEAR * MINUTE_IN_NS;

  let totalInserted = 0;
  const batchSize = 100; // Use same batch size as benchmark
  const totalPoints = NUM_HOSTS * MINUTES_PER_YEAR;

  for (let hostId = 1; hostId <= NUM_HOSTS; hostId++) {
    const rackId = ((hostId - 1) % NUM_RACKS) + 1;

    for (let minute = 0; minute < MINUTES_PER_YEAR; minute += batchSize) {
      // Build batch writes array (individual write objects)
      const writes = [];

      const batchEnd = Math.min(minute + batchSize, MINUTES_PER_YEAR);
      for (let m = minute; m < batchEnd; m++) {
        const timestamp = yearAgo + m * MINUTE_IN_NS;

        // Generate values for each field
        const fields = {};
        FIELDS.forEach((field) => {
          let value;
          switch (field) {
            case "cpu_usage":
              value = 20 + Math.random() * 60;
              break;
            case "memory_usage":
              value = 30 + Math.random() * 50;
              break;
            case "disk_io_read":
            case "disk_io_write":
              value = Math.random() * 100;
              break;
            case "network_in":
            case "network_out":
              value = Math.random() * 1000;
              break;
            case "load_avg_1m":
            case "load_avg_5m":
            case "load_avg_15m":
              value = Math.random() * 4;
              break;
            case "temperature":
              value = 50 + Math.random() * 30;
              break;
            default:
              value = Math.random() * 100;
          }
          fields[field] = value;
        });

        writes.push({
          measurement: MEASUREMENT,
          tags: {
            host: `host-${hostId.toString().padStart(2, "0")}`,
            rack: `rack-${rackId}`,
          },
          fields: fields,
          timestamp: timestamp,
        });
      }

      // Create the batch-format write request
      const writeRequest = { writes };

      try {
        await axios.post(`${BASE_URL}/write`, writeRequest);
        totalInserted += writes.length;
        process.stdout.write(
          `\rInserted ${totalInserted}/${totalPoints} points (${(
            (totalInserted * 100) /
            totalPoints
          ).toFixed(1)}%)`
        );
      } catch (error) {
        console.error("\nFailed to insert batch:", error.message);
        if (error.response) {
          console.error("Response status:", error.response.status);
          console.error("Response data:", error.response.data);
        }
        process.exit(1);
      }
    }
  }

  console.log("\nSample data insertion complete!");
}

// Benchmark queries
async function benchmarkQueries() {
  console.log("\n=== QUERY BENCHMARKS ===");

  const now = Date.now() * 1000000;
  const yearAgo = now - MINUTES_PER_YEAR * MINUTE_IN_NS;

  // 1. Single host query
  console.log("\n--- Single Host Query ---");
  const singleHostStats = new BenchmarkStats("Single Host Query");

  for (let i = 0; i < BENCHMARK_ITERATIONS; i++) {
    const hostId = Math.floor(Math.random() * NUM_HOSTS) + 1;
    const hostName = `host-${hostId.toString().padStart(2, "0")}`;

    const query = {
      query: `avg:${MEASUREMENT}(cpu_usage,memory_usage){host:${hostName}}`,
      startTime: yearAgo,
      endTime: now,
      aggregationInterval: "12h", // 12-hour buckets for year of data
    };

    const start = performance.now();
    try {
      const response = await axios.post(`${BASE_URL}/query`, query);
      const pointCount = response.data.statistics?.point_count || 0;
    } catch (error) {
      console.error("Query failed:", error.message);
    }
    const end = performance.now();

    singleHostStats.addTime(end - start);
    process.stdout.write(`\rIteration ${i + 1}/${BENCHMARK_ITERATIONS}`);
  }

  console.log("\n" + JSON.stringify(singleHostStats.getStats(), null, 2));

  // 2. Group by rack query
  console.log("\n--- Group by Rack Query ---");
  const groupByRackStats = new BenchmarkStats("Group by Rack");

  for (let i = 0; i < BENCHMARK_ITERATIONS; i++) {
    const query = {
      query: `avg:${MEASUREMENT}(cpu_usage,memory_usage,network_in,network_out){} by {rack}`,
      startTime: yearAgo,
      endTime: now,
      aggregationInterval: "12h", // 12-hour buckets for year of data
    };

    const start = performance.now();
    try {
      const response = await axios.post(`${BASE_URL}/query`, query);
      const seriesCount = response.data.statistics?.series_count || 0;
    } catch (error) {
      console.error("Query failed:", error.message);
    }
    const end = performance.now();

    groupByRackStats.addTime(end - start);
    process.stdout.write(`\rIteration ${i + 1}/${BENCHMARK_ITERATIONS}`);
  }

  console.log("\n" + JSON.stringify(groupByRackStats.getStats(), null, 2));

  // 3. Group by host query
  console.log("\n--- Group by Host Query ---");
  const groupByHostStats = new BenchmarkStats("Group by Host");

  for (let i = 0; i < BENCHMARK_ITERATIONS; i++) {
    const query = {
      query: `avg:${MEASUREMENT}(cpu_usage,memory_usage,load_avg_1m){} by {host}`,
      startTime: yearAgo,
      endTime: now,
      aggregationInterval: "12h", // 12-hour buckets for year of data
    };

    const start = performance.now();
    try {
      const response = await axios.post(`${BASE_URL}/query`, query);
      const seriesCount = response.data.statistics?.series_count || 0;
    } catch (error) {
      console.error("Query failed:", error.message);
    }
    const end = performance.now();

    groupByHostStats.addTime(end - start);
    process.stdout.write(`\rIteration ${i + 1}/${BENCHMARK_ITERATIONS}`);
  }

  console.log("\n" + JSON.stringify(groupByHostStats.getStats(), null, 2));

  return {
    singleHost: singleHostStats,
    groupByRack: groupByRackStats,
    groupByHost: groupByHostStats,
  };
}

// Main benchmark runner
async function runBenchmarks() {
  console.log("=".repeat(60));
  console.log(" TSDB HTTP API BENCHMARK TOOL");
  console.log("=".repeat(60));
  console.log(`Server: ${BASE_URL}`);
  console.log(`Measurement: ${MEASUREMENT}`);
  console.log(`Hosts: ${NUM_HOSTS}`);
  console.log(`Racks: ${NUM_RACKS}`);
  console.log(`Fields: ${FIELDS.length} (${FIELDS.join(", ")})`);
  console.log(`Iterations per benchmark: ${BENCHMARK_ITERATIONS}`);
  console.log("=".repeat(60));

  // Check server health
  try {
    await axios.get(`${BASE_URL}/health`);
    console.log("✓ Server is healthy\n");
  } catch (error) {
    console.error("✗ Server health check failed:", error.message);
    process.exit(1);
  }

  const results = {
    timestamp: new Date().toISOString(),
    config: {
      server: BASE_URL,
      measurement: MEASUREMENT,
      hosts: NUM_HOSTS,
      racks: NUM_RACKS,
      fields: FIELDS.length,
      iterations: BENCHMARK_ITERATIONS,
    },
    benchmarks: {},
  };

  // Run insert benchmarks
  results.benchmarks.batchInsert = await benchmarkBatchInserts();

  // Insert sample data and run query benchmarks
  await insertSampleData();
  const queryResults = await benchmarkQueries();
  results.benchmarks.queries = queryResults;

  // Print summary
  console.log("\n" + "=".repeat(60));
  console.log(" BENCHMARK SUMMARY");
  console.log("=".repeat(60));

  console.log("\n📊 INSERT PERFORMANCE (Concurrency Comparison):");
  console.log("  Concurrency 1: " + results.benchmarks.batchInsert.concurrency1.getStats().avg + "ms avg");
  console.log("  Concurrency 2: " + results.benchmarks.batchInsert.concurrency2.getStats().avg + "ms avg");
  console.log("  Concurrency 5: " + results.benchmarks.batchInsert.concurrency5.getStats().avg + "ms avg");

  console.log("\n📊 QUERY PERFORMANCE:");
  console.log(`  Single Host: ${queryResults.singleHost.getStats().avg}ms avg`);
  console.log(
    `  Group by Rack: ${queryResults.groupByRack.getStats().avg}ms avg`
  );
  console.log(
    `  Group by Host: ${queryResults.groupByHost.getStats().avg}ms avg`
  );

  console.log("\n" + "=".repeat(60));
  console.log("Benchmark complete!");
}

// Run the benchmarks
runBenchmarks().catch(console.error);
