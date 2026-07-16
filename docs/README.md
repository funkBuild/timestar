# TimeStar Documentation

## API Reference

- [Write API](api-write.md) - Ingest time series data
- [Query API](api-query.md) - Query with aggregation and filtering
- [Derived Queries](api-derived.md) - Multi-query expressions, anomaly detection, forecasting
- [Delete API](api-delete.md) - Delete data by series, pattern, or time range
- [Metadata API](api-metadata.md) - List measurements, tags, and fields; estimate series cardinality
- [Streaming API](api-streaming.md) - Real-time SSE subscriptions
- [Retention API](api-retention.md) - Manage retention and downsampling policies
- [Health API](api-health.md) - Server health check

## Query & Functions

- [Query Language](query-language.md) - Query syntax, aggregation methods, time intervals
- [Expression Functions](expression-functions.md) - 40+ functions for transforms, rolling windows, cross-series math
- [Anomaly Detection](anomaly-detection.md) - Basic, agile, and robust anomaly algorithms
- [Forecasting](forecasting.md) - Linear and seasonal forecasting with STL decomposition

## Architecture

- [Architecture Overview](architecture.md) - Storage engine, data flow, sharding, compression

## Operational

- [Backup & Restore](backup-restore.md) - Backing up and restoring data directories
- [Security](security.md) - Authentication and deployment hardening

## Format & Internals

- [TSM File Format](tsm_format.md) - On-disk block layout, headers, compression details
- [NaN Policy](nan_policy.md) - How NaN values are handled across ingest, storage, and aggregation

## Design History

- [docs/history/](history/) - Point-in-time plans, proposals, and reviews (distributed index, native index, SSTable async I/O, codebase and performance reviews). Historical documents; symbol names and details have since changed.
