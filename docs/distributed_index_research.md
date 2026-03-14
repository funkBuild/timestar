# Distributed Indexing Research: Lessons from Production Systems

**Date**: March 14, 2026
**Purpose**: Research how major distributed time series databases and search engines handle the inverted index / tag index problem, and identify ideas applicable to TimeStar's distributed index design.

---

## Table of Contents

1. [InfluxDB TSI (Time Series Index)](#1-influxdb-tsi-time-series-index)
2. [VictoriaMetrics](#2-victoriametrics)
3. [Prometheus / Thanos](#3-prometheus--thanos)
4. [ClickHouse](#4-clickhouse)
5. [Elasticsearch / OpenSearch](#5-elasticsearch--opensearch)
6. [M3DB (Uber)](#6-m3db-uber)
7. [Supplementary: Datadog](#7-supplementary-datadog)
8. [Comparison Table](#8-comparison-table)
9. [Recommendations for TimeStar](#9-recommendations-for-timestar)

---

## 1. InfluxDB TSI (Time Series Index)

### Problem Statement

Before TSI, InfluxDB kept its entire inverted index in memory, rebuilt at startup from TSM data. Every measurement, tag key-value pair, and field name had an in-memory lookup table mapping metadata to underlying series. This made the system RAM-bound and unable to scale beyond ~10M series per node.

### Index Structure

TSI is a **log-structured merge tree (LSM)** for series metadata, separate from the TSM data store. The key components:

- **SeriesFile**: A global (database-wide) file shared across all shards that stores every unique series key. Each series gets an auto-incrementing **series ID**. The series file uses a **roaring bitmap** to track which series IDs exist and efficiently skip duplicates.

- **Index**: Contains the entire index dataset for a single shard. Each shard has its own TSI index.

- **Partition**: Each shard's index is further split into partitions (default 8) for **lock-splitting**. Series are hashed to partitions to reduce write contention.

- **LogFile (L0)**: Newly written series go into an in-memory index backed by a WAL-like log file. The LogFile acts as the mutable write buffer.

- **IndexFile (L1+)**: Immutable, memory-mapped index files. When a LogFile exceeds a threshold (5 MB), it is compacted into an IndexFile at L1. Multiple contiguous IndexFiles can be merged into higher levels.

### On-Disk IndexFile Format

Each IndexFile contains three major blocks:

1. **Measurement Block**: Stores measurement names, each pointing to a list of tag keys.
2. **Tag Block**: For each tag key, stores a list of tag values. Each tag value points to a **sorted list of series IDs** (not full series keys -- IDs are much smaller and sorted integer sets can be operated on faster).
3. **Series Block**: Maps local series IDs to series keys, combined with a block ID to form globally addressable uint64 series IDs.
4. **Trailer**: Contains offset information and **HyperLogLog sketches** for cardinality estimation.

### Tag Lookup Flow

1. Hash the query's tag constraints to identify relevant partitions.
2. For each partition, scan the LogFile (in-memory) and all IndexFiles (memory-mapped).
3. From each tag block, retrieve the sorted series ID list for each (tagKey, tagValue) pair.
4. **Intersect** the sorted ID lists to find series matching all tag constraints.
5. Use the SeriesFile to resolve IDs back to full series keys if needed.

### Distribution Model

- Each **shard** (time-bounded partition of data) has its own TSI index.
- The **SeriesFile** is shared across all shards in a database -- so series ID assignment is global.
- In InfluxDB Enterprise, shards are distributed across nodes, and each node maintains TSI indexes for its local shards.
- Tag lookups during queries require scanning the TSI index of every shard that overlaps the query's time range.

### Key Optimizations

- **Memory-mapped files**: Hot index data pulled into OS page cache; cold data stays on disk.
- **Roaring bitmaps**: Efficient set operations on series IDs (union, intersection, difference).
- **Compaction levels**: L0 (log) -> L1 -> L2+ with merge, similar to LSM tree compaction.
- **HyperLogLog sketches**: Cardinality estimation in the trailer avoids full scans for `SHOW SERIES CARDINALITY`.
- **Partition-level lock splitting**: 8 partitions per shard index reduces write contention.

### Strengths and Weaknesses

**Strengths**: Disk-based index scales to billions of series; roaring bitmaps make set operations fast; per-shard indexes mean no cross-shard coordination for writes.

**Weaknesses**: Discovery queries (`SHOW TAG VALUES`) must scan all shards; no per-day pruning; compaction can cause latency spikes; the global SeriesFile is a centralization point.

---

## 2. VictoriaMetrics

### Index Architecture: IndexDB with MergeSet

VictoriaMetrics built a custom storage engine called **mergeset** -- inspired by ClickHouse's MergeTree but optimized for inverted index workloads. IndexDB sits on top of mergeset and maps human-readable label queries to internal **TSIDs** (Time Series IDs).

### Seven Index Key Types

IndexDB uses numeric prefixes to distinguish seven types of index entries:

| Prefix | Type | Scope | Example |
|--------|------|-------|---------|
| 1 | Tag -> Metric IDs | Global | `1 method=GET -> [49, 53]` |
| 2 | Metric ID -> TSID | Global | `2 49 -> TSID{metricID=49,...}` |
| 3 | Metric ID -> Metric Name | Global | `3 49 -> http_request_total{method="GET",status="200"}` |
| 4 | Deleted Metric ID | Global | `4 152` |
| 5 | Date -> Metric IDs | Per-day | `5 2024-01-01 -> [49, 53, 67]` |
| 6 | Date + Tag -> Metric IDs | Per-day | `6 2024-01-01 method=GET -> [49, 53]` |
| 7 | Date + Metric Name -> TSID | Per-day | `7 2024-01-01 http_request_total{...} -> TSID{...}` |

### Per-Day Index: The Key Innovation

The **per-day index** (prefixes 5, 6, 7) is VictoriaMetrics' most important optimization for query performance:

- When querying a time range, IndexDB starts with the **date prefix** and immediately narrows to entries from relevant dates only.
- Without per-day indexes, a global scan would traverse entries across the entire retention period.
- Trade-off: Storage cost is higher. A series appearing daily generates 21 per-day entries over 7 days versus 3 global entries.
- **Pre-population**: During the last hour of each day, VictoriaMetrics progressively pre-populates the next day's index entries with linearly increasing probability (0 at 23:00 to 1.0 at midnight). This prevents a write spike at midnight.

### MergeSet Internals

The mergeset follows an LSM-like structure:
- **In-memory buffer** (write buffer for recent items)
- **In-memory parts** (recently flushed)
- **Small parts** (on disk, recently compacted)
- **Big parts** (on disk, heavily compacted)

Each part has: `metaindex.bin` (block boundaries), `index.bin` (within-block offsets), `items.bin` (actual key-value data), `lens.bin` (item lengths).

### Distributed Architecture (Cluster Mode)

VictoriaMetrics cluster has three stateless/stateful component types:

- **vminsert**: Stateless. Distributes incoming data across vmstorage nodes using **consistent hashing** over metric name + all labels.
- **vmstorage**: Stateful. Stores both raw data and IndexDB. Each vmstorage node has its own complete IndexDB for its shard of data.
- **vmselect**: Stateless. Fans out queries to **all** vmstorage nodes, each node performs local index lookup and data retrieval, then vmselect merges results.

### Query Flow

1. vmselect receives a PromQL query.
2. vmselect sends the label matchers to **every** vmstorage node.
3. Each vmstorage uses its local IndexDB to resolve matchers -> TSIDs.
4. Each vmstorage reads data blocks for matched TSIDs and returns them.
5. vmselect merges and deduplicates results.

### High-Cardinality Optimizations

- **In-memory TSID cache**: Maps active series to internal IDs. When all active series fit in cache, no disk reads needed on write path.
- **10x less RAM than InfluxDB**: Achieved through mergeset compression and efficient on-disk format.
- **Date-scoped queries**: Per-day indexes avoid scanning irrelevant time periods.
- **Bloom filters**: In mergeset parts for fast negative lookups.

### Strengths and Weaknesses

**Strengths**: Per-day index is brilliant for time-scoped queries; mergeset is purpose-built (not generic KV store); very memory-efficient; simple cluster architecture.

**Weaknesses**: Fan-out to all vmstorage nodes for every query; no routing optimization to skip irrelevant nodes; per-day index has storage overhead with high churn.

---

## 3. Prometheus / Thanos

### Prometheus In-Memory Inverted Index

Prometheus TSDB uses a purely **in-memory inverted index** in the Head block (the active, mutable block):

- **Postings List**: For every label name-value pair `(labelName, labelValue)`, there is a sorted list of series IDs (called "postings"). This is the core inverted index.
- **Label-to-Values Map**: Maps each label name to all known values, enabling `label_values()` queries.
- **Series by ID**: Maps series ID to the in-memory series object with its label set and chunk data.

### On-Disk Index (Persistent Blocks)

When the Head block is compacted to disk, the index is persisted as:

1. **Symbol Table**: Deduplicated list of all label names and values as strings.
2. **Series Section**: List of all series with their label sets (referencing the symbol table).
3. **Postings Section**: For each (labelName, labelValue) pair, a list of series IDs.
4. **Postings Offset Table**: Sorted by label name and value, stores offsets into the postings section.
5. **Table of Contents (TOC)**: Offsets to each section.

### Memory-Efficient Postings Lookup

Rather than loading the entire postings offset table into memory, Prometheus stores **every 32nd** (labelValue, offset) pair in memory. On lookup:
1. Binary search the in-memory sparse table to find the nearest entry before the target.
2. Walk the memory-mapped index file from that point to find the exact entry.

This reduces memory usage dramatically while keeping lookups fast.

### Postings Intersection Algorithm

When resolving a query with multiple label matchers (e.g., `{job="api", method="GET"}`):

1. Retrieve the postings list for each matcher.
2. **Sort lists by size** (smallest first).
3. Intersect iteratively: start with the smallest list, then for each subsequent list, use a **seek-based intersection** -- for each ID in the smaller list, binary-seek in the larger list.
4. The "smallest first" optimization dramatically reduces work when one label is highly selective.

The optimized intersection algorithm showed **63-96% reduction** in operation time and **50-100% reduction** in allocations compared to the original tree-recursive approach.

### Thanos: Distributed Queries Over Prometheus

Thanos adds a distributed query layer on top of multiple Prometheus instances:

- **StoreAPI**: A gRPC interface that any data source can implement. Prometheus sidecars, object storage gateways, and other Thanos components all speak StoreAPI.
- **Querier**: Stateless component that **fans out** to all registered StoreAPI endpoints, deduplicates results, and evaluates PromQL.
- **Deduplication**: Given a configured replica label (e.g., `replica`), series from HA pairs that differ only by the replica label are merged into one.
- **Hierarchical fan-out**: Queriers can query other Queriers, enabling multi-region federation.
- **Stateless and horizontally scalable**: Any Querier can handle any query.

### Block-Level Metadata for Pruning

Thanos Store Gateway uses block-level metadata (min/max time, label sets) to **prune irrelevant blocks** before loading index data. The binary index header format allows loading just enough of the index to answer a query without reading entire blocks.

### Strengths and Weaknesses

**Strengths**: Simple, elegant in-memory index design; sorted postings enable fast intersection; smallest-first optimization is highly effective; Thanos StoreAPI is a clean abstraction for fan-out.

**Weaknesses**: Prometheus index is entirely in-memory (limits cardinality per instance); Thanos fans out to all stores; no routing to avoid irrelevant nodes; each Prometheus instance is independent (no distributed index).

---

## 4. ClickHouse

### MergeTree Primary Index

ClickHouse does not use a traditional inverted index for its primary data access path. Instead, MergeTree tables rely on:

- **Primary Key (Sorting Key)**: Data is physically sorted by the primary key columns. This is not a unique key -- it defines sort order.
- **Sparse Primary Index**: For every **granule** (default 8192 rows), ClickHouse stores the primary key values of the first row as a **mark**. The index maps marks to byte offsets in data files.
- **Mark-based range queries**: To find rows matching a WHERE clause, ClickHouse binary-searches the sparse index to identify relevant mark ranges, then reads only those granules.

### Skip Indexes (Secondary Indexes)

For columns not in the primary key, ClickHouse offers skip indexes:

- **minmax**: Stores min/max per granule. Eliminates granules where the queried value falls outside the range.
- **set(N)**: Stores up to N unique values per granule. Eliminates granules that don't contain the queried value.
- **bloom_filter**: Probabilistic filter for membership testing.
- **tokenbf_v1 / ngrambf_v1** (deprecated): Bloom filter-based text search indexes.
- **Full inverted index** (modern): True inverted index with a token dictionary and posting lists mapping tokens to row numbers. Recommended over bloom filter approaches.

Skip indexes operate on **granule groups** (controlled by GRANULARITY parameter). `GRANULARITY 1` means one index entry per granule; `GRANULARITY 4` means one entry per 4 granules.

### Partition Pruning

Tables can be partitioned (e.g., by month). Query execution first prunes irrelevant partitions using the partition key, then applies the primary key index, then skip indexes. This three-level pruning is very efficient.

### Distributed Query Execution

- **Distributed Table**: A virtual table that references local tables on multiple shards.
- **Coordinator Node**: Any node receiving a query becomes the coordinator. It rewrites the query for each shard and sends sub-queries.
- **Partial Aggregation**: For GROUP BY queries, each shard performs local aggregation and sends **intermediate aggregate states** (not raw data) to the coordinator. The coordinator merges these states.
- **QueryProcessingStage**: Internal enum that controls how much work each shard does before sending results. `WithMergeableState` means shards aggregate locally and return mergeable states.
- **Local optimization**: The coordinator runs one sub-query locally instead of sending it over the network.

### Key Insights for TimeStar

- **Partial aggregation on shards** is critical: send aggregate states, not raw data, over the network.
- **Three-level pruning** (partition -> primary index -> skip index) minimizes data scanned.
- **Sort order as a first-class optimization**: Choosing the right sort key is more important than secondary indexes.
- **Granule-based indexing**: The concept of indexing at a coarser granularity than individual rows reduces index size while maintaining good selectivity.

### Strengths and Weaknesses

**Strengths**: Extremely fast analytical queries; partial aggregation reduces network transfer; three-level pruning is elegant; sort-key-based primary index is simple and effective.

**Weaknesses**: Not designed for the inverted-index-style "find all series matching tags" problem; skip indexes are coarse-grained; distributed queries still fan out to all shards.

---

## 5. Elasticsearch / OpenSearch

### Lucene Segment Architecture

Each Elasticsearch shard is a Lucene index composed of multiple **segments**. Each segment is an independent, immutable inverted index containing:

1. **Term Dictionary**: Maps terms (tag values) to term metadata. Uses **Finite State Transducers (FST)** for memory-efficient, prefix-sharing lookups. FSTs allow millions of terms to be stored with minimal memory by sharing common character sequences.

2. **Postings Lists**: For each term, a sorted list of document IDs. Compressed using:
   - **Delta encoding**: Store differences between consecutive IDs (e.g., [1, 9, 420] stored as [1, 8, 411]).
   - **Roaring bitmaps** or **Frame of Reference (FOR)** encoding for dense posting lists.
   - **Skip lists**: Embedded in postings for efficient seeking during intersection.

3. **Stored Fields**: Original document data for retrieval in the fetch phase.

4. **Doc Values**: Column-oriented storage for aggregations and sorting.

### Distributed Query: Two-Phase Scatter-Gather

Elasticsearch uses a **two-phase** distributed search:

**Phase 1 -- Query Phase (Scatter)**:
1. The **coordinating node** receives the query.
2. It broadcasts the query to **all relevant shards** (primary or replica copy of each).
3. Each shard executes the query locally against its Lucene segments.
4. Each shard returns **only document IDs and sort values** (not full documents) for its top-N matches.
5. The coordinating node merges these partial results to determine the globally correct top-N.

**Phase 2 -- Fetch Phase (Gather)**:
1. The coordinating node identifies which documents to retrieve.
2. It issues **multi-GET** requests only to the shards that hold the winning documents.
3. Shards return full documents.
4. The coordinating node assembles the final response.

### Aggregation Processing

- **Shard-level aggregation**: Each shard computes partial aggregations locally using **global ordinals** (per-shard integer mappings for each unique value).
- **Reduce phase**: The coordinating node merges partial aggregation results.
- This is analogous to ClickHouse's partial aggregation approach.

### Routing to Reduce Fan-Out

Elasticsearch provides several mechanisms to reduce fan-out:

1. **Custom routing**: Documents can be routed to specific shards based on a routing value (e.g., `customer_id`). Queries with the same routing value only hit one shard instead of all.
2. **`_preference` parameter**: `_local` restricts to local shards; `_prefer_nodes` targets specific nodes; `_shards` targets specific shards.
3. **`max_concurrent_shard_requests`**: Limits concurrent shard requests per node (default 5) to prevent overload.
4. **Adaptive Replica Selection (ARS)**: Instead of round-robin, the coordinating node routes to the replica most likely to respond fastest, based on:
   - Prior response times
   - Prior service times (how long the shard took to process)
   - Search thread pool queue depth

### Strengths and Weaknesses

**Strengths**: Mature, battle-tested distributed search; FST-based term dictionary is extremely memory-efficient; two-phase search avoids transferring unnecessary data; ARS reduces tail latency; custom routing can eliminate fan-out entirely.

**Weaknesses**: Default is full fan-out to all shards; two-phase search adds latency for simple queries; segment merging is I/O intensive; not optimized for time series workloads specifically.

---

## 6. M3DB (Uber)

### Architecture Overview

M3DB is inspired by Gorilla (Facebook's in-memory TSDB) and Cassandra (distributed KV store). At Uber, it stores 6.6 billion time series, aggregates 500 million metrics/second, and persists 20 million metrics/second.

### Sharding with Consistent Hashing

- Series keys are hashed (murmur3 by default) to **4096 virtual shards**.
- Virtual shards are assigned to physical nodes via a **placement** (stored in etcd).
- This is identical to Cassandra's consistent hashing ring.
- On write: hash(seriesKey) -> virtual shard -> physical node.

### Inverted Index: m3ninx

M3DB's index engine is called **m3ninx**, inspired by Apache Lucene:

- **Mutable Segments**: In-memory segments for recently written series. These hold the inverted index for the current time block.
- **Immutable/FST Segments**: When a time block closes, the mutable segment is compacted into an immutable segment using **Vellum FST** (Finite State Transducer, same concept as Lucene).
- **Postings Lists**: Stored as **roaring bitmaps** (from the Pilosa library). Each (field, term) pair maps to a roaring bitmap of document IDs.
- **Document Structure**: Each indexed "document" represents a series with its tags as fields.

### FST Segment Format

Each immutable segment contains:
- **FST Fields file**: Maps field names (tag keys) to offsets in the FST Terms file.
- **FST Terms file**: For each field, maps term values (tag values) to postings offsets.
- **Postings Data file**: Contains Pilosa roaring bitmaps.
- **Documents Index/Data files**: Map document IDs back to full tag sets.

### Query Operations

- **Term queries**: Exact match on (tagKey, tagValue) -- direct FST lookup.
- **Regexp queries**: FST traversal with regex automaton.
- **Boolean combinators**: AND (intersection of roaring bitmaps), OR (union), NOT (difference).
- **Read-through caching**: Immutable segments wrap postings lists with a cache layer, transparently caching frequently accessed postings.

### Distributed Query Flow

1. **M3 Query** (or M3 Coordinator) receives a query.
2. Fan-out to M3DB nodes based on the placement (knows which nodes own which shards).
3. Each node queries its local m3ninx index for matching series.
4. Each node reads data for matched series from its local storage.
5. Results are streamed back and merged.
6. **Namespace stitching**: For queries spanning multiple retention namespaces (e.g., unaggregated + aggregated), M3 Query stitches results across namespaces.

### Key Innovations

- **Bloom filters for series existence**: Before checking disk, M3DB consults in-memory bloom filters for all (shard, blockStart) combinations that overlap the query range. This quickly determines if a series exists on disk without reading index segments.
- **Consistent hashing with virtual shards**: The 4096 virtual shard pool allows rebalancing without rehashing all data.
- **Roaring bitmap postings**: Same as InfluxDB's TSI, enabling fast set operations.
- **FST-based term lookup**: Same approach as Lucene/Elasticsearch, providing memory-efficient term dictionaries.

### Strengths and Weaknesses

**Strengths**: Proven at massive scale (billions of series); FST + roaring bitmaps are best-in-class for inverted index; consistent hashing enables clean scaling; bloom filters avoid unnecessary disk reads.

**Weaknesses**: Complexity of Cassandra-style placement management; namespace stitching adds query complexity; still requires fan-out (though placement-aware fan-out is better than blind broadcast).

---

## 7. Supplementary: Datadog

### Custom Indexing at Scale

Datadog built a custom timeseries indexing service on top of **RocksDB** that handles trillions of events per day.

### Intra-Node Sharding

Each node splits its RocksDB instance into **multiple isolated shards** (8 shards on 32-core nodes):
- Timeseries IDs are hashed and assigned to shards.
- Queries execute across all local shards **in parallel**, then merge results.
- This yielded a **nearly 8x performance boost** on 32-core machines.
- Allowed scaling to larger node types (more cores) rather than more nodes.

**This is directly analogous to TimeStar's Seastar shard-per-core model.**

### Key Insight

Datadog's architecture validates the approach of co-locating index entries with the data they reference and using intra-node parallelism (sharding across CPU cores) as a primary scaling strategy. Their experience with 8 RocksDB shards on 32 cores closely mirrors TimeStar's shard-per-core design.

---

## 8. Comparison Table

| Feature | InfluxDB TSI | VictoriaMetrics | Prometheus/Thanos | ClickHouse | Elasticsearch | M3DB |
|---------|-------------|-----------------|-------------------|------------|--------------|------|
| **Index Type** | LSM-based inverted index | MergeSet inverted index | In-memory postings lists | Sparse primary + skip indexes | Lucene inverted index (FST + postings) | m3ninx (FST + roaring bitmaps) |
| **Index Storage** | Memory-mapped files on disk | MergeSet on disk | In-memory (Head); mmap'd (blocks) | Embedded in data parts | Per-segment on disk | Mutable in-memory; FST on disk |
| **Series ID Type** | Auto-incrementing uint64 | Internal TSID struct | Auto-incrementing uint64 | N/A (row-based) | Lucene doc ID | Internal doc ID |
| **Set Operations** | Roaring bitmaps | Sorted ID lists in mergeset | Sorted postings intersection | N/A | Delta-encoded + skip lists | Roaring bitmaps (Pilosa) |
| **Distribution** | Per-shard index, global SeriesFile | Per-vmstorage IndexDB | Per-instance; Thanos fans out | Per-shard MergeTree parts | Per-shard Lucene index | Per-node m3ninx, consistent hashing |
| **Discovery Query** | Scan all shard indexes | Fan out to all vmstorage | Fan out to all StoreAPI endpoints | Fan out to all shards | Scatter-gather to all shards | Fan out via placement |
| **Fan-Out Optimization** | None (scan all shards) | None (all vmstorage) | None (all stores) | Partition pruning | Custom routing, ARS | Placement-aware routing |
| **Time Pruning** | Per-shard time bounds | **Per-day index** (key innovation) | Block-level time bounds | Partition pruning | None built-in | Block-level time bounds |
| **High-Cardinality** | Disk-based index, HLL sketches | TSID cache, per-day pruning | Limited by RAM | Sort key + skip indexes | FST compression | FST + bloom filters |
| **Partial Aggregation** | No | No (vmselect merges raw) | No (Querier evaluates PromQL) | **Yes** (mergeable aggregate states) | **Yes** (shard-level aggregation) | No |
| **Write Path Index Cost** | Hash + LogFile append | Hash + in-memory buffer append | In-memory map insert | Sorted insert (primary key) | Lucene segment buffer | Mutable segment insert |
| **Compaction** | L0 -> L1 -> L2+ merge | In-memory -> small -> big parts | Head -> persistent blocks | Background merge of parts | Lucene segment merging | Mutable -> FST segments |

---

## 9. Recommendations for TimeStar

Based on the research above, here are the most impactful ideas for TimeStar's distributed index, organized by priority.

### 9.1 Validate: Co-Located Index (Confirmed by Industry)

**Every system studied co-locates index entries with the data they reference.**

- InfluxDB: Each shard has its own TSI index.
- VictoriaMetrics: Each vmstorage has its own IndexDB.
- M3DB: Each node has its own m3ninx segments.
- Elasticsearch: Each shard has its own Lucene index.
- Datadog: Each RocksDB shard has its own inverted index.

TimeStar's proposed approach -- co-locating TAG_INDEX entries with series data on each shard -- is the universal standard. **No production system centralizes the tag index.** The current centralization on shard 0 should be eliminated.

**Action**: Proceed with the co-located index design. Each Seastar shard maintains its own TAG_INDEX for the series it owns.

### 9.2 High Impact: Per-Day (or Per-Block) Index Scoping

**Adopt VictoriaMetrics' per-day index concept.**

VictoriaMetrics' most distinctive optimization is maintaining per-day copies of the tag-to-metric-ID mapping alongside the global mapping. This allows queries to immediately narrow to entries from relevant dates, avoiding scanning the entire retention period.

For TimeStar, the analogous approach:
- When writing a TAG_INDEX entry `(measurement, tagKey, tagValue) -> seriesId`, also write a **time-scoped** entry: `(date, measurement, tagKey, tagValue) -> seriesId`.
- On query, use the time-scoped entries to prune series that have no data in the query's time range.
- Consider using a coarser granularity than per-day (e.g., per-hour or per-TSM-block) depending on typical query ranges.

**Trade-off**: Higher storage cost (more index entries) but dramatically faster queries when the time range is narrow relative to retention.

**Action**: Add a time-scoped prefix to TAG_INDEX keys. The prefix granularity (hour/day) should be configurable.

### 9.3 High Impact: Roaring Bitmaps for Series ID Sets

**Replace sorted vectors with roaring bitmaps for series ID postings lists.**

Used by: InfluxDB TSI, M3DB, Elasticsearch (optionally). Roaring bitmaps provide:
- Fast set operations: intersection, union, difference in O(n) with small constants.
- Excellent compression for both sparse and dense ID distributions.
- Up to 900x faster than alternatives when intersecting sparse with dense bitmaps.

For TimeStar, each TAG_INDEX entry's value (the set of matching SeriesIds) should be stored as a roaring bitmap. This directly enables:
- Fast multi-tag queries via bitmap intersection.
- Efficient serialization/deserialization for the LevelDB-backed index.
- Memory-efficient representation for high-cardinality tags.

**Action**: Integrate [CRoaring](https://github.com/RoaringBitmap/CRoaring) (the C/C++ roaring bitmap library) for TAG_INDEX postings lists.

### 9.4 High Impact: Shard-Level Partial Aggregation

**Adopt ClickHouse/Elasticsearch's partial aggregation pattern.**

Currently, TimeStar's query fan-out collects raw data from all shards and aggregates on the coordinator. Instead:
1. Each shard should compute **partial aggregate states** locally (e.g., partial sum, count, min, max, running variance for stddev).
2. Ship only the aggregate states to the coordinator (or the querying shard).
3. The coordinator merges aggregate states into final results.

This dramatically reduces network transfer, especially for queries over many series with aggregation. For TimeStar's Seastar model, this means each shard's query coroutine returns `AggregationState` objects rather than raw `TSMResult` vectors.

**Action**: Modify the fan-out query path to support a `shard_aggregate` mode that returns mergeable `AggregationState` objects instead of raw data.

### 9.5 Medium Impact: Smallest-First Postings Intersection

**Adopt Prometheus' smallest-first intersection algorithm.**

When a query has multiple tag constraints (e.g., `{location:us-west, host:server-*}`), the order of intersection matters:
1. Retrieve postings lists for all tag constraints.
2. Sort by list size (smallest first).
3. Iterate the smallest list and seek into larger lists.

Prometheus showed 63-96% performance improvement from this optimization alone. For TimeStar, when resolving multi-tag scope filters, always intersect starting with the most selective tag.

**Action**: When multiple scope filters are present, retrieve cardinality estimates (or actual list sizes) first, then intersect smallest-to-largest.

### 9.6 Medium Impact: FST-Based Term Dictionary (Future)

**Consider FST for high-cardinality tag value lookup.**

Both Elasticsearch/Lucene and M3DB use Finite State Transducers for memory-efficient term (tag value) dictionaries. FSTs:
- Share common prefixes and suffixes, dramatically reducing memory.
- Support efficient prefix/regex queries.
- Are immutable and cache-friendly.

For TimeStar's current scale (single server), LevelDB's built-in prefix compression may be sufficient. But as cardinality grows into the millions of unique tag values, FSTs become compelling.

**Action**: Defer to post-v1. Keep LevelDB for now but design the TAG_INDEX abstraction so the backing store can be swapped.

### 9.7 Medium Impact: Bloom Filters for Existence Checks

**Adopt M3DB's bloom filter approach for series existence.**

Before performing a full index lookup, consult an in-memory bloom filter to quickly determine if a series exists in a given shard/time-block. This is especially valuable for:
- Write path: Checking if a series already exists before creating a new index entry.
- Query path: Quickly pruning shards/blocks that definitely don't contain the queried series.

TimeStar already has bloom filter support (SIMD-accelerated). Extend it to the index layer.

**Action**: Maintain per-shard bloom filters of active series IDs. Check on write (skip index update if series already known) and on query (skip shard if bloom filter says "definitely not here").

### 9.8 Medium Impact: HyperLogLog for Cardinality Estimation

**Adopt InfluxDB's HLL sketches for cardinality queries.**

Discovery queries like "how many unique series match this tag pattern?" are expensive if they require a full scan. HyperLogLog sketches can provide approximate cardinality with:
- Fixed memory (typically 16 KB per sketch).
- O(1) query time.
- Mergeable across shards (union of HLL sketches).

**Action**: Maintain per-measurement and per-tag-key HLL sketches. Use them for `SHOW SERIES CARDINALITY` equivalent queries and for query planning (choosing intersection order).

### 9.9 Low Impact (but important for multi-server): Placement-Aware Fan-Out

**Learn from M3DB and Elasticsearch for multi-server deployment.**

When TimeStar scales beyond a single server:
- Use M3DB's approach: maintain a **placement** (mapping of virtual shards to physical nodes) so the coordinator knows exactly which nodes to query.
- Use Elasticsearch's **adaptive replica selection**: track response times per node and route to the fastest replica.
- Consider Elasticsearch's **custom routing**: allow users to route writes by a tag value, so queries filtering on that tag only hit relevant shards.

For Seastar's intra-server model, the shard ID (derived from `SeriesId128::Hash{}(id) % smp::count`) already provides deterministic routing. The same hash can be extended to `hash % total_shards_across_cluster` for multi-server.

**Action**: Design the sharding hash to be a two-level scheme: `hash % total_virtual_shards` (cluster-wide), then virtual shards assigned to (server, core) pairs. This extends naturally from single-server to multi-server.

### 9.10 Architecture Summary: Target Design

```
Write Path:
  1. Hash(seriesKey) -> shard (core-level, extensible to server-level)
  2. Write data to local WAL + MemoryStore
  3. Update local TAG_INDEX:
     a. (measurement, tagKey, tagValue) -> add seriesId to roaring bitmap
     b. (date, measurement, tagKey, tagValue) -> add to time-scoped bitmap
     c. Update bloom filter with seriesId
     d. Update HLL sketch for measurement

Query Path (tag-filtered):
  1. Parse query, extract tag scope filters
  2. Fan out to ALL shards (each shard has co-located index)
  3. Each shard:
     a. Check bloom filter -> skip if definitely no match
     b. Look up time-scoped TAG_INDEX entries for each scope filter
     c. Intersect roaring bitmaps (smallest first)
     d. Read data for matched series
     e. Compute partial aggregation state
  4. Coordinator merges partial aggregation states
  5. Return final result

Discovery Path (metadata):
  1. Fan out to all shards
  2. Each shard returns local metadata (measurements, tags, values)
  3. Coordinator deduplicates and merges
  4. Use HLL sketches for cardinality estimation
```

### 9.11 Prioritized Implementation Order

| Priority | Item | Effort | Impact |
|----------|------|--------|--------|
| P0 | Co-locate TAG_INDEX on each shard | High | Eliminates shard-0 bottleneck |
| P0 | Roaring bitmaps for postings lists | Medium | Fast multi-tag intersection |
| P1 | Shard-level partial aggregation | Medium | Reduces fan-out data transfer |
| P1 | Per-day/per-block time-scoped index | Medium | Prunes irrelevant series on query |
| P1 | Smallest-first intersection ordering | Low | 60-90% faster multi-tag queries |
| P2 | Bloom filter for series existence | Low | Avoid unnecessary index lookups |
| P2 | HLL sketches for cardinality | Low | Fast cardinality estimation |
| P3 | Two-level shard hash for multi-server | Medium | Enables cluster scaling |
| P3 | Adaptive replica selection | Medium | Reduces tail latency in cluster |
| P4 | FST-based term dictionary | High | Needed only at extreme cardinality |

---

## Sources

### InfluxDB TSI
- [Time Series Index (TSI) details - InfluxDB OSS v1](https://docs.influxdata.com/influxdb/v1/concepts/tsi-details/)
- [Time Series Index (TSI) overview - InfluxDB OSS v1](https://docs.influxdata.com/influxdb/v1/concepts/time-series-index/)
- [TSI Proposal - Issue #7173 - influxdata/influxdb](https://github.com/influxdata/influxdb/issues/7173)
- [Path to 1 Billion Time Series - InfluxData Blog](https://www.influxdata.com/blog/path-1-billion-time-series-influxdb-high-cardinality-indexing-ready-testing/)
- [InfluxDB 3.0 FDAP Architecture - InfluxData Blog](https://www.influxdata.com/blog/flight-datafusion-arrow-parquet-fdap-architecture-influxdb/)

### VictoriaMetrics
- [How vmstorage's IndexDB Works - VictoriaMetrics Blog](https://victoriametrics.com/blog/vmstorage-how-indexdb-works/)
- [VictoriaMetrics Cluster Architecture](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/)
- [VictoriaMetrics FAQ](https://docs.victoriametrics.com/victoriametrics/faq/)
- [VictoriaMetrics Churn Rate and IndexDB - ITNEXT](https://itnext.io/victoriametrics-churn-rate-high-cardinality-metrics-an-indexdb-004137029164)
- [How vmstorage Handles Data Ingestion](https://victoriametrics.com/blog/vmstorage-how-it-handles-data-ingestion/)

### Prometheus / Thanos
- [Prometheus TSDB Persistent Block and Index - Ganesh Vernekar](https://ganeshvernekar.com/blog/prometheus-tsdb-persistent-block-and-its-index/)
- [Prometheus TSDB Index Format - GitHub](https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/index.md)
- [Prometheus Postings Implementation - GitHub](https://github.com/prometheus/prometheus/blob/main/tsdb/index/postings.go)
- [Postings Intersection Optimization - PR #616](https://github.com/prometheus-junkyard/tsdb/pull/616)
- [Thanos Query Component Documentation](https://thanos.io/tip/components/query.md/)

### ClickHouse
- [MergeTree Table Engine - ClickHouse Docs](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse Skip Indexes Best Practices](https://clickhouse.com/docs/best-practices/use-data-skipping-indices-where-appropriate)
- [ClickHouse Distributed Table Engine](https://clickhouse.com/docs/engines/table-engines/special/distributed)
- [ClickHouse Query Optimization Guide 2026](https://clickhouse.com/resources/engineering/clickhouse-query-optimisation-definitive-guide)
- [ClickHouse Black Magic: Skipping Indices - Altinity](https://altinity.com/blog/clickhouse-black-magic-skipping-indices)

### Elasticsearch / OpenSearch
- [Elasticsearch from the Top Down - Elastic Blog](https://www.elastic.co/blog/found-elasticsearch-top-down)
- [Elasticsearch Distributed Search Phases - Medium](https://medium.com/@musabdogan/elasticsearchs-distributed-search-query-and-fetch-phases-df869d35f4b3)
- [Elasticsearch Adaptive Replica Selection - Elastic Blog](https://www.elastic.co/blog/improving-response-latency-in-elasticsearch-with-adaptive-replica-selection)
- [Elasticsearch Search Shard Routing](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search-shard-routing)
- [Exploring Apache Lucene Index](https://j.blaszyk.me/tech-blog/exploring-apache-lucene-index/)
- [Frame of Reference and Roaring Bitmaps - Elastic Blog](https://www.elastic.co/blog/frame-of-reference-and-roaring-bitmaps)

### M3DB
- [M3DB Architecture Overview](https://m3db.io/docs/architecture/m3db/)
- [M3DB Sharding](https://m3db.io/docs/architecture/m3db/sharding/)
- [M3DB Storage Engine](https://m3db.io/docs/architecture/m3db/engine/)
- [FOSDEM 2020 - M3DB Inverted Index](https://archive.fosdem.org/2020/schedule/event/m3db/)
- [M3 Query Fan-out Architecture](https://m3db.io/docs/architecture/m3query/fanout/)
- [Uber M3 Blog Post](https://www.uber.com/blog/m3/)

### Datadog
- [Timeseries Indexing at Scale - Datadog Engineering](https://www.datadoghq.com/blog/engineering/timeseries-indexing-at-scale/)

### General
- [Database of Databases - VictoriaMetrics](https://dbdb.io/db/victoriametrics)
- [Database of Databases - InfluxDB](https://dbdb.io/db/influxdb)
- [Roaring Bitmaps: Better Bitmap Performance](https://arxiv.org/pdf/1402.6407)
