# Cube Store Architecture

Cube Store is a distributed OLAP storage engine designed to serve as the
pre-aggregation layer for Cube. It ingests data from cloud data warehouses,
stores it in a columnar format optimized for analytical queries, and serves
results with sub-second latency at high concurrency.

**Technology stack:** Rust, Apache Arrow, DataFusion, Parquet, RocksDB.

## Motivation

Cloud data warehouses excel at cheap data replays, low maintenance, and
scalable joins, but fall short on sub-second query latency, high concurrency
(1000+ QPS), and low ingestion latency. Cube Store closes that gap: it sits
in front of a warehouse and turns its output into a query-optimized store that
delivers the latency and concurrency profile of a dedicated OLAP datastore
while preserving the warehouse's ease of use.

### Requirements

- Billions of rows as input
- Sub-second response time
- High query concurrency (1000+ QPS)
- Input data always has a unique primary key
- Sources are both batch and real-time streaming
- Joins between data sources

## High-Level Architecture

```
                        ┌──────────────┐
                        │   Cube API   │
                        └──────┬───────┘
                               │ SQL (MySQL wire protocol / HTTP)
                               ▼
                        ┌──────────────┐
                        │    Router    │
                        │  (MetaStore) │
                        └──┬───┬───┬──┘
                           │   │   │   TCP RPC
                    ┌──────┘   │   └──────┐
                    ▼          ▼          ▼
              ┌──────────┐┌──────────┐┌──────────┐
              │ Worker 1 ││ Worker 2 ││ Worker N │
              └────┬─────┘└────┬─────┘└────┬─────┘
                   │           │           │
                   └─────┬─────┴─────┬─────┘
                         ▼           ▼
                  ┌────────────────────────┐
                  │   Cloud Object Store   │
                  │   (S3 / GCS / MinIO)   │
                  └────────────────────────┘
```

The router node owns the MetaStore and coordinates query execution. Worker
nodes execute partial queries on partitions they own and return results to
the router for final merge. All persistent data lives in a cloud object store;
compute nodes are disposable and interchangeable.

## Design Decisions

Each design decision maps to one or more of the requirements above.

| Decision | Billions of rows | Sub-second latency | High concurrency | Unique keys | Batch + streaming | Joins |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| High-throughput MetaStore | | | | | | |
| Indexes are sorted copies | | | | | | |
| Auto-partitioning | | | | | | |
| Parquet format | | | | | | |
| Distributed FS | | | | | | |
| Shared-nothing architecture | | | | | | |
| Collocated join indexes | | | | | | |
| Real-time in-memory chunks | | | | | | |

The sections below describe each decision in detail.

## MetaStore

The MetaStore is the most frequently accessed component. It is relatively
small compared to the data it describes but can still reach 10M+ rows. It
requires strong read-after-write consistency and handles heavy read traffic
with significant write concurrency.

RocksDB backs the MetaStore. Keys use a fixed 13-byte prefix that encodes
the table type, followed by the primary key. Secondary indexes are maintained
alongside primary data to support efficient lookups (e.g., partitions by
index ID, chunks by partition ID).

### Data Model

```
Schema
  └─ Table
       ├─ columns, unique_key_column_indices, aggregate_column_indices
       ├─ stream_offset (for Kafka sources)
       └─ Index (1..N per table)
            ├─ sort_key_size (first N columns form the sort key)
            ├─ partition_split_key_size
            ├─ index_type: Regular | Aggregate
            └─ Partition (1..N per index)
                 ├─ min_value / max_value  (partition boundaries)
                 ├─ min / max              (actual data bounds)
                 ├─ main_table_row_count, file_size
                 ├─ multi_partition_id     (for collocated joins)
                 └─ Chunk (0..N per partition)
                      ├─ in_memory: bool
                      ├─ row_count, file_size
                      ├─ min / max (data bounds)
                      └─ replay_handle_id (streaming replay)

MultiIndex ──► MultiPartition (tree)
  └─ Aligns partitions across tables for collocated joins
```

### MetaStore Tables (RocksDB key prefixes)

| Prefix | Entity | Purpose |
|--------|--------|---------|
| `0x01` | Schema | Logical schema grouping |
| `0x02` | Table | Table definition, columns, locations |
| `0x03` | Index | Sort key, partitioning, multi-index reference |
| `0x04` | Partition | Data range, parent/child splits, file location |
| `0x05` | Chunk | Partition fragment, in-memory or Parquet file |
| `0x06` | WAL | Write-ahead log tracking |
| `0x07` | Job | Async operation metadata |
| `0x08` | Source | Data source registration |
| `0x09` | MultiIndex | Cross-table partition key definition |
| `0x0A` | MultiPartition | Aligned partition ranges for joins |
| `0x0B` | ReplayHandle | Streaming sequence tracking for recovery |

### Access Flow

Workers connect to the router's MetaStore port over TCP. MetaStore RPC calls
are serialized as `NetworkMessage::MetaStoreCall` and processed on the router.
All metadata mutations go through the router to maintain consistency. Workers
cache metadata locally and receive invalidation events via broadcast channels.

## Indexes Are Sorted Copies

Sorting is the most efficient way to compress columnar data. Sorted columns
produce long runs of identical or sequential values, which compress extremely
well under dictionary and run-length encoding in Parquet. Sorting also makes
filtering optimal: range predicates can skip entire row groups via min/max
statistics.

An index in Cube Store defines a column ordering. The first `sort_key_size`
columns form the sort key; data within each partition is physically sorted
by these columns. Every merge operation—sort, join, aggregate—is
merge-based, which works naturally with pre-sorted data.

The system trades write amplification (maintaining sorted copies) for read
speed. Index selection is filter-driven: the query planner examines WHERE
clause predicates, collects constraints via `CollectConstraints`, and picks
the index whose sort key best matches the filter columns.

## Auto-Partitioning

Partitions split automatically during compaction to keep all partitions
roughly the same size. Because data is sorted, split points are easy to
compute: pick the midpoint of the sort key range.

```
Before compaction:
  Partition A (large)
  ┌─────────────────────────────┐
  │  sorted data                │
  └─────────────────────────────┘

After split at compaction:
  Partition A'            Partition A''
  ┌──────────────┐        ┌──────────────┐
  │ first half   │        │ second half  │
  └──────────────┘        └──────────────┘
```

MultiPartitions coordinate splits across tables that participate in
collocated joins. When a `MultiPartition` exceeds its row count threshold,
it sets `prepared_for_split` to block concurrent compactions, computes child
ranges, repartitions all underlying table partitions, and activates the
children.

## Parquet Format

Parquet is the storage format for all persistent data. It implements
Partition Attributes Across (PAX), which groups values by column within
row groups, enabling both columnar compression and efficient row
reconstruction.

Key properties of Cube Store's Parquet usage:

- **Row group size:** 16,384 rows (constant `ROW_GROUP_SIZE`)
- **Writer version:** Parquet 2.0
- **Min/max statistics:** Every row group records per-column min and max
  values. The query planner uses these for partition pruning—partitions
  whose min/max ranges don't intersect the query's WHERE clause are skipped
  entirely.

File naming:
- Partition main table: `{partition_id}-{suffix}.parquet`
- Chunk: `{chunk_id}-{suffix}.chunk.parquet`

## Distributed File System

Cloud object storage (S3, GCS, MinIO) decouples storage from compute.
Worker nodes are disposable: any worker can download the partitions it needs
and begin serving queries. This makes replication straightforward and enables
elastic scaling.

### Partition Warmup

Tables do not become queryable until all their partitions have been
downloaded to the workers that own them. Before a table goes online, the
system warms up every partition:

1. Router assigns partitions to workers via consistent hashing
2. Workers download their assigned partition files from object storage
3. Once all downloads complete, the table becomes active

This avoids download latency on the query path.

### RemoteFs Abstraction

The `RemoteFs` trait abstracts all cloud storage operations:

| Method | Purpose |
|--------|---------|
| `upload_file()` | Move local file to cloud storage |
| `download_file()` | Download and cache locally |
| `delete_file()` | Remove from cloud storage |
| `list()` / `list_with_metadata()` | Enumerate remote files |

Implementations: `S3RemoteFs`, `GCSRemoteFs`, `MINIORemoteFs`,
`LocalDirRemoteFs` (testing). A `QueueRemoteFs` wrapper deduplicates
concurrent operations and adds retry logic.

## Real-Time In-Memory Chunks

In-memory chunks act as a buffer to avoid single-row writes to Parquet.
Streaming rows (e.g., from Kafka) are sent to the partition owner worker
and stored as Apache Arrow `RecordBatch` objects in a
`RwLock<HashMap<String, RecordBatch>>`.

```
Kafka / streaming source
        │
        ▼
  ┌─────────────┐     route to partition owner
  │   Router    │ ──────────────────────────────┐
  └─────────────┘                               │
                                                ▼
                                         ┌─────────────┐
                                         │  Worker N   │
                                         │ (in-memory  │
                                         │   chunks)   │
                                         └──────┬──────┘
                                                │ compact every ~1 min
                                                ▼
                                         ┌─────────────┐
                                         │  Parquet    │
                                         │  partition  │
                                         └─────────────┘
```

In-memory chunks are compacted to persistent Parquet when any threshold is
exceeded:

- **Chunk count** exceeds `compaction_in_memory_chunks_count_threshold`
- **Chunk age** exceeds `compaction_in_memory_chunks_max_lifetime_threshold`
- **Total size** exceeds `compaction_in_memory_chunks_size_limit`

Compaction merges chunks, applies sort order, deduplicates by unique key
(`LastRowByUniqueKeyExec`), and evaluates aggregate columns (SUM, MAX, MIN,
MERGE).

## Shared-Nothing Architecture

Worker nodes never communicate with each other. Each worker owns a set of
partitions (assigned by consistent hashing) and executes queries
independently on its local data. The router receives partial results from
all workers and performs a final merge.

### Partition-to-Worker Assignment

```rust
fn pick_worker_by_partitions(config, partitions) -> &str {
    let mut hasher = DefaultHasher::new();
    for p in partitions {
        p.min_val().hash(&mut hasher);
        p.max_val().hash(&mut hasher);
        p.index_id().hash(&mut hasher);
    }
    workers[(hasher.finish() % workers.len()) as usize]
}
```

The assignment is deterministic—every node independently computes the same
mapping without coordination.

### Query Execution (Lambda Architecture)

Queries combine persisted Parquet data with real-time in-memory chunks:

```
                    Router
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Worker 1    Worker 2    Worker N
       ┌──────┐    ┌──────┐    ┌──────┐
       │Parquet│    │Parquet│    │Parquet│   ← persisted
       │chunks │    │chunks │    │chunks │
       ├──────┤    ├──────┤    ├──────┤
       │Memory│    │Memory│    │Memory│   ← real-time
       │chunks│    │chunks│    │chunks│
       └──┬───┘    └──┬───┘    └──┬───┘
          │           │           │
          └─────┬─────┴─────┬─────┘
                │  partial  │
                │  results  │
                ▼           ▼
             ┌──────────────────┐
             │  Router: merge   │
             │  sort, aggregate │
             └──────────────────┘
```

## Collocated Join Indexes

Collocated joins are the fastest distributed join algorithm. Instead of
shuffling data at query time, Cube Store ensures that rows that will be
joined are already on the same worker.

A `MultiIndex` defines the join key columns shared across multiple tables.
Each table that participates in a join has an index whose
`multi_index_id` references the shared MultiIndex. `MultiPartition` nodes
form a tree that defines aligned key ranges across all participating tables.

When data is ingested, it is partitioned using the MultiIndex key so that
matching key ranges land in the same MultiPartition. Since partition-to-worker
assignment is deterministic, rows with the same join key always reside on
the same worker. At query time, each worker executes its local join without
any network data movement.

This mechanism also enables shared-nothing Top-K optimizations, where each
worker computes a local top-K and the router merges the results.

## Query Execution Pipeline

### End-to-End Flow

```
SQL string
    │
    ▼
┌──────────────────────────────────────────┐
│ 1. Parse         (sqlparser-rs)          │
│ 2. Logical plan  (DataFusion)            │
│ 3. Index select  (choose_index_ext)      │
│    └─ collect WHERE constraints          │
│    └─ pick best index per table          │
│    └─ insert ClusterSendNode             │
│ 4. Serialize plan for workers            │
└──────────────────┬───────────────────────┘
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
  ┌─────────┐ ┌─────────┐ ┌─────────┐
  │Worker 1 │ │Worker 2 │ │Worker N │
  │         │ │         │ │         │
  │ Prune   │ │ Prune   │ │ Prune   │  ← min/max filter
  │ Scan    │ │ Scan    │ │ Scan    │  ← Parquet + memory
  │ Partial │ │ Partial │ │ Partial │  ← aggregation
  │ agg     │ │ agg     │ │ agg     │
  └────┬────┘ └────┬────┘ └────┬────┘
       │           │           │
       └─────┬─────┴─────┬─────┘
             ▼           ▼
        ┌──────────────────────┐
        │ Router:              │
        │  SortPreservingMerge │
        │  Final aggregation   │
        │  LIMIT / OFFSET      │
        └──────────────────────┘
```

### Key Physical Plan Nodes

| Node | Side | Purpose |
|------|------|---------|
| `ClusterSendExec` | Router | Distributes work to workers, collects results |
| `WorkerExec` | Worker | Wraps the worker-side portion of the plan |
| `CubeTableExec` | Worker | Reads Parquet files and in-memory chunks |
| `InlineAggregateExec` | Both | Streaming aggregation (Partial on workers, Final on router) |
| `AggregateTopKExec` | Both | Optimized LIMIT + ORDER BY + GROUP BY |
| `SortPreservingMergeExec` | Router | Merges sorted streams from workers |
| `LastRowByUniqueKeyExec` | Worker | Deduplication by unique key |

### Optimizations

- **Aggregate pushdown:** Transforms `Final(Partial(ClusterSend))` into
  `Final(ClusterSend(Partial))`, moving partial aggregation to workers.
- **Inline aggregation:** Replaces hash-based aggregation with streaming
  merge-based aggregation on sorted data.
- **Top-K pushdown:** Detects `Limit(Sort(Aggregate(ClusterSend)))` and
  pushes partial top-K selection to workers.
- **Partition pruning:** `PartitionFilter` extracts range predicates from
  WHERE clauses and eliminates partitions whose min/max bounds cannot match.
- **Limit pushdown:** Propagates LIMIT to worker plans.
- **Parquet metadata caching:** LRU cache (`moka`) avoids re-reading Parquet
  footers for partition statistics.

## Storage Layer and Compaction

### Data Ingestion Flow

```
Incoming data (CSV, streaming, INSERT)
    │
    ▼
partition_data()  ─── splits rows by partition key using index sort key
    │
    ├──► Small batch: add_memory_chunk()  → HashMap<String, RecordBatch>
    │
    └──► Large batch: add_persistent_chunk() → Parquet file → upload to cloud
```

### Compaction

The `CompactionService` runs background jobs that merge chunks:

1. **In-memory compaction:** Merges multiple in-memory chunks into one,
   applying sort, deduplication, and aggregation.
2. **Persistent compaction:** Merges chunks into the partition's main
   Parquet file. Multiple small chunk files become one sorted file.
3. **Partition split:** When a partition exceeds its row threshold, it
   splits into two child partitions at the sort key midpoint.
4. **MultiPartition split:** Coordinates splits across all tables in a
   collocated join group.

Compaction swap is atomic: old chunks are deactivated and new chunks
activated in a single MetaStore transaction (`swap_chunks_without_check`).

## Cluster Communication

### Network Protocol

Nodes communicate via TCP using Flexbuffers serialization with a magic
number (`94107`) and version (`1`) header for protocol compatibility.

### Message Types

| Message | Direction | Purpose |
|---------|-----------|---------|
| `Select` / `RouterSelect` | Router → Worker | Execute plan on worker |
| `SelectStart` / `SelectResultBatch` | Router ↔ Worker | Streaming results |
| `WarmupDownload` | Router → Worker | Pre-fetch partition files |
| `AddMemoryChunk` / `FreeMemoryChunk` | Router → Worker | Manage in-memory data |
| `MetaStoreCall` | Worker → Router | Metadata read/write RPC |
| `NotifyJobListeners` | Router → Worker | Trigger job processing |

### Worker Process Pool

On Unix systems, query execution runs in a separate process pool
(`WorkerPool`) to isolate memory usage and prevent a single query from
affecting others. The router spawns worker processes and communicates via
IPC channels. Idle workers are killed after a timeout and respawned on
demand.

## Approximate Algorithms

Cube Store includes several approximate distinct count implementations to
support different warehouse dialects:

| Crate | Algorithm | Compatible with |
|-------|-----------|-----------------|
| `cubehll` | HyperLogLog (Airlift) | Presto, Athena |
| `cubehll` | HyperLogLog (Snowflake) | Snowflake |
| `cubehll` | HyperLogLog (Postgres) | PostgreSQL |
| `cubezetasketch` | ZetaSketch (Theta Sketch) | BigQuery |
| `cubedatasketches` | DataSketches | Databricks |

These are stored as `BYTE_ARRAY` columns with `HyperLogLog(flavour)` type
and merged during aggregation.

## Crate Structure

```
rust/cubestore/
├── cubestore/            Main implementation
│   └── src/
│       ├── metastore/        RocksDB-backed metadata (schema, table, index, partition, chunk)
│       ├── queryplanner/     Query planning, optimization, distributed execution
│       ├── store/            Storage layer, compaction, data management
│       ├── cluster/          Router-worker coordination, RPC, job scheduling
│       ├── table/            Row/column types, Parquet I/O, data redistribution
│       ├── cachestore/       Cache and queue management (separate RocksDB instance)
│       ├── remotefs/         Cloud storage abstraction (S3, GCS, MinIO)
│       ├── streaming/        Kafka consumer, buffering, post-processing
│       ├── sql/              SQL parsing and query dispatch
│       ├── import/           CSV import, type coercion, streaming decompression
│       ├── config/           Dependency injection, service configuration
│       ├── http/             HTTP API
│       ├── mysql/            MySQL wire protocol
│       └── telemetry/        OpenTelemetry tracing
├── cubestore-sql-tests/  SQL compatibility tests
├── cubehll/              HyperLogLog implementations
├── cubedatasketches/     DataSketches integration
├── cubezetasketch/       Theta Sketch (ZetaSketch)
├── cuberpc/              RPC transport layer
└── cuberockstore/        RocksDB wrapper
```

## Configuration and Deployment

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `CUBESTORE_SERVER_NAME` | Node identifier |
| `CUBESTORE_WORKERS` | Comma-separated worker addresses |
| `CUBESTORE_META_ADDR` | MetaStore (router) address for workers |
| `CUBESTORE_WORKER_PORT` | Worker listen port |
| `CUBESTORE_METASTORE_PORT` | MetaStore listen port |

A single-node deployment runs both router and worker in one process.
Multi-node deployments use one router and N workers, with all nodes
pointing to the same cloud object store.

### Dependency Injection

Services are wired via a custom DI container (`Injector`). Each service
is registered as a factory and lazily initialized on first access. This
decouples component initialization and simplifies testing with mock
implementations.
