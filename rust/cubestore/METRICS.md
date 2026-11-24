# Cube Store metrics

Cube Store pushes metrics over UDP in StatsD or DogStatsD format
(`CUBESTORE_METRICS_FORMAT=statsd|dogstatsd`, `CUBESTORE_METRICS_ADDRESS`, `CUBESTORE_METRICS_PORT`).
Every metric is defined in `cubestore/src/app_metrics.rs`. Names below are the StatsD names; a
`statsd_exporter` with default mapping exposes `cs.sql.query.data.ms` as `cs_sql_query_data_ms`.

Constant tags (e.g. host and tenant) are whatever the embedding binary passes to `init_metrics`;
the OSS `cubestored` passes none. Tags listed below are the ones set per metric.

## Types and their traps

| Type | StatsD | DogStatsD | In Prometheus (statsd_exporter defaults) |
|------|--------|-----------|------------------------------------------|
| counter | `c` | `c` | `_total`-less counter; use `rate()` |
| gauge | `g` | `g` | last value received, kept **forever** |
| histogram | `ms` (timer) | `h` | summary: `{quantile=...}`, `_sum`, `_count` |

- **Summaries have sticky quantiles.** statsd_exporter computes quantiles over a sliding window
  (10 minutes by default), so one slow query keeps `quantile="0.99"` (and the max) high for the
  whole window. `_sum` and `_count` are plain cumulative counters in the metric's own unit; compute
  averages from them. For server-side percentiles, map the timers to histograms in the exporter
  (`observer_type: histogram` with `histogram_options.buckets`) and use `histogram_quantile`.
- **Gauges outlive the process that sent them.** A gauge stays at its last value until the same
  series is sent again. When the exporter runs per node (DaemonSet), a pod moved to another node
  leaves its old series behind on the old exporter, and summing a gauge over all series counts that
  pod twice. The periodically refreshed gauges (`cs.metastore.chunks*`, `cs.workers.in_memory_chunks*`,
  `cs.streaming.lag*`, `cs.jobs`) are re-sent every 30–75 s, so a `ttl` of 5 minutes or more on
  mappings matching them safely drops dead series. Do not set it as a global default: gauges
  sent only on change (`cs.sql.query.data.cache.*`, `planning.throttle.*`,
  `cs.streaming.lastoffset`, `cs.remote_fs.files_to_remove.*`, `cs.scheduler.events_pending`,
  `in_memory_compaction.discovery.age.ms`) would vanish from a quiet but live process.
- Tagged gauges that are reported as a set (marked *set* below) send `0` for a tag combination
  that disappeared, so they never keep a stale non-zero value for a vanished series.

## Query path (router)

| Metric | Type | Tags | Unit | Semantics and cadence |
|--------|------|------|------|-----------------------|
| `cs.sql.query.data.incoming` | counter | | | Selects on arrival, before planning |
| `cs.sql.query.data` | counter | `command` | | Queries planned and dispatched |
| `cs.sql.query.data.cache.hit` / `.stale_hit` | counter | | | Result cache hits |
| `cs.sql.query.data.cache.size` / `.weight`, `.cache.stale.size` / `.weight` | gauge | | entries / bytes | Result cache occupancy, on every cache change |
| `cs.sql.query.data.ms` | histogram | | ms | Router execution time (`collect` of the router plan) of a data query that missed the result cache; excludes planning. One sample per query |
| `cs.sql.query.data.planning.throttle.in_flight` / `.queued` | gauge | | | Planning throttle occupancy |
| `cs.sql.query.data.planning.throttle.wait.us` | histogram | | µs | Wait for a planning slot; only queries that waited |
| `cs.sql.query.data.planning.throttle.rejected` | counter | | | Queries rejected by the throttle |
| `cs.sql.query.data.planning.logical_plan.*.us` | histogram | | µs | Logical planning phases: `total_creation`, `execution_context`, `query_planner_setup`, `statement_to_plan`, `optimize`, `is_data_select_query` |
| `cs.sql.query.data.planning.choose_index_and_workers.us` | histogram | | µs | Per query: `choose_index_ext` + `compute_workers`, i.e. the main's metastore-bound planning cost |
| `cs.sql.query.data.planning.choose_index_ext.*.us` | histogram | | µs | Parts of `choose_index_ext`: `get_tables_with_indices`, `pick_index`, `get_active_partitions_and_chunks_by_index_id`, `get_multi_partition_subtree`, `total_awaiting` |
| `cs.sql.query.data.planning.to_serialized_plan.us` | histogram | | µs | Plan serialization |
| `cs.sql.query.data.planning.router_plan.us` / `.worker_plan.us` | histogram | | µs | Physical planning on the router / on a worker |
| `cs.sql.data_frame_serialization.us`, `cs.http.data_frame_serialization.us` | histogram | | µs | Result serialization for MySQL / HTTP |
| `cs.sql.query.meta` / `.meta.ms` | counter / histogram | | ms | Metadata and trivial queries |
| `cs.sql.query.cache` / `.cache.ms`, `cs.sql.query.queue` / `.queue.ms` | counter / histogram | | ms | Cache and queue commands |
| `cs.worker_pool.errors` | counter | `subprocess_type`, `error_type` | | IPC errors with select worker subprocesses |

## In-memory chunks held by a process

Reported by every node that holds in-memory chunks (the workers owning streaming partitions).
Refreshed on every add/release and by a state pass every 60 s (±20% jitter), which also runs right after
startup, so a restarted process overwrites the previous incarnation's value within a minute.

| Metric | Type | Unit | Semantics |
|--------|------|------|-----------|
| `cs.workers.in_memory_chunks` | gauge | chunks | All in-memory chunks the process holds, whatever their metastore state |
| `cs.workers.in_memory_chunks.rows` / `.memory` | gauge | rows / bytes | Same set |
| `cs.workers.in_memory_chunks.active` | gauge | chunks | Held chunks the metastore lists as active; compare with `cs.metastore.chunks.in_memory_active_by_node` of this node |
| `cs.workers.in_memory_chunks.inactive` | gauge | chunks | Held chunks already deactivated, waiting for GC (≈ `in_memory_not_used_timeout`, 30 s). Chunks not activated yet (e.g. a compaction output before its swap) are in neither gauge |
| `cs.workers.in_memory_chunks.orphans` | gauge | chunks | Held chunks whose metastore row was already deleted: their release was lost and they stay in memory until the process restarts |

A held chunk is released when the scheduler on the main processes its deletion event. Events are
dropped when the scheduler's event channel overflows (`cs.scheduler.events_dropped`), and those
chunks stay in worker memory for the life of the process, which makes `cs.workers.in_memory_chunks`
far exceed the metastore's count. `orphans` counts them exactly. `held − active − inactive` is
only an upper bound: it also counts chunks added since the last pass and chunks not activated yet.

## Metastore chunk truth (main)

Gathered by the scheduler's chunk sweep, which scans the chunk table every 30 s anyway; the
per-node count costs one partition lookup per partition that holds active in-memory chunks.

| Metric | Type | Tags | Unit | Semantics |
|--------|------|------|------|-----------|
| `cs.metastore.chunks` | gauge, set | `in_memory`, `active` | chunks | `SELECT in_memory, active, count(*) FROM system.chunks`; `active:false` also holds chunks not activated yet |
| `cs.metastore.chunks.rows` | gauge, set | `in_memory`, `active` | rows | Same, `sum(row_count)` |
| `cs.metastore.chunks.in_memory_active_by_node` | gauge, set | `node` | chunks | Active in-memory chunks by owning node |
| `cs.metastore.chunks.in_memory.oldest_active_age.secs` | gauge | | s | Age (since creation) of the oldest active in-memory chunk; 0 when there is none |
| `cs.metastore.chunks.oldest_inactive_age.secs` | gauge, set | `in_memory` | s | Time since the oldest still-present deactivated chunk was deactivated: the GC backlog. Chunks never activated are not counted |
| `cs.gc.chunks_deleted` | counter | `in_memory` | chunks | Chunk rows deleted by GC |
| `cs.gc.chunk_delete_batches` | counter | | | GC delete batches (≤ 1000 chunks each) |

## In-memory compaction (main)

| Metric | Type | Unit | Semantics and cadence |
|--------|------|------|-----------------------|
| `cs.metastore.in_memory_compaction.discovery.ms` | histogram | ms | Duration of the discovery scan (full chunk-table scan, single-flight, cached for `CUBESTORE_IN_MEMORY_CHUNKS_SCHEDULE_PERIOD_SECS`); only when a streaming table exists |
| `cs.metastore.in_memory_compaction.discovery.age.ms` | gauge | ms | Age of the discovery result served to a node's compaction job; high values mean a scan outlives the cache period |
| `cs.metastore.in_memory_compaction.pending_partitions` | gauge | partitions | Partitions holding active in-memory chunks, per the last scan |

## Scheduler (main)

| Metric | Type | Unit | Semantics |
|--------|------|------|-----------|
| `cs.scheduler.events_dropped` | counter | events | Metastore events lost to channel overflow; the same number as the `Scheduler is lagging on meta store event processing for N messages` log line |
| `cs.scheduler.events_pending` | gauge | events | Events waiting in the scheduler's channel; refreshed at most once a second while events flow and at once when the channel drains |

## Jobs

| Metric | Type | Tags | Semantics and cadence |
|--------|------|------|-----------------------|
| `cs.jobs` | gauge, set | `job_type`, `status` | Jobs in the metastore (what `system.jobs` shows), by status `Scheduled`, `ProcessingBy`, `Completed`, `Timeout`, `Error`, `Orphaned`. Main, from the orphaned-job scan of every reconcile (30 s) |
| `cs.jobs.scheduled` | counter | `job_type` | Jobs added; a job already queued for the same row is not added and not counted. Main |
| `cs.jobs.completed` | counter | `job_type` | Jobs finished successfully. Emitted by the node that ran the job |
| `cs.jobs.failed` | counter | `job_type`, `error_class` | Jobs that failed or timed out. `error_class`: `offset_overflow`, `source_chunk_not_active`, `partition_not_active`, `timeout` (the job exceeded its timeout), `stale_stream` (an idle stream ended by `CUBESTORE_STALE_STREAM_TIMEOUT`, not a failure), `shutdown`, `panic`, `other` |
| `cs.jobs.partition_compaction.{count,completed,failures}`, `cs.jobs.multi_partition_split.*`, `cs.jobs.finish_multi_split.*`, `cs.jobs.repartition_chunk.*` | counter | | Older per-type counters, kept for existing dashboards |
| `cs.jobs.unknown.deleted` | counter | | Jobs of a type this binary does not know, removed by cleanup (version skew) |

## Streaming

Tagged `location` (the stream location of the table).

| Metric | Type | Unit | Semantics and cadence |
|--------|------|------|-----------------------|
| `cs.streaming.rows` / `cs.streaming.chunks` | counter | | Rows / chunks ingested |
| `cs.streaming.lastoffset` | gauge | offset | Last sequence value of the last received batch |
| `cs.streaming.lag` | gauge | messages | Partition high watermark minus the last consumed offset (Kafka only). Recomputed every 10 s whether or not messages arrive (while the fetch fails, the last value is re-sent and `.age.ms` grows), only when a metrics sink is configured; set to 0 when the stream stops cleanly on this node (table dropped, stream moved, sealed, or idle past `CUBESTORE_STALE_STREAM_TIMEOUT`) |
| `cs.streaming.lag.age.ms` | gauge | ms | Time since `cs.streaming.lag` was last computed (before the first success: since the first consumed offset), reported every 10 s. Kafka only. Stays below ~10 s when healthy; grows while the watermark fetch fails, i.e. while the lag value is stale. When the stream stops cleanly it is set to 0 (with the lag); when the stream fails it is set to `i64::MAX` and the lag keeps its last value until the stream runs again on this node; if it never does (table dropped or moved after the failure), only the exporter `ttl` removes the series |
| `cs.streaming.import_time.ms`, `.partition_time.ms`, `.upload_time.ms`, `.roundtrip_time.ms` | histogram | ms | Per received batch |
| `cs.streaming.roundtrip_rows` / `.roundtrip_chunks` | histogram | | Per received batch |

## Storage and other

| Metric | Type | Tags | Semantics |
|--------|------|------|-----------|
| `cs.startup` | counter | | Process starts |
| `cs.remote_fs.operations.core` | counter | `operation`, `driver` | Remote storage calls (S3 / GCS) |
| `cs.remote_fs.files_to_remove.count` / `.size`, `cs.remote_fs.files_removed.count` | gauge / counter | | Remote storage cleanup |
| `cs.warmup.missing` | counter | | Warmup files gone before the pass reached them |
| `cs.cachestore.*` | | | Cache store size, TTL buffer, eviction and GC queue |
| `cs.metastore.queue_size`, `cs.metastore.*_operation.ms` | | | Defined but not emitted |

## PromQL

Labels such as `tenant` and `host` come from constant tags or the exporter.

In-memory chunk backlog, metastore view vs what workers hold:

```promql
sum by (tenant) (cs_metastore_chunks{in_memory="true", active="true"})
sum by (tenant) (cs_workers_in_memory_chunks_active)
sum by (tenant) (cs_workers_in_memory_chunks)          # includes chunks awaiting GC
sum by (tenant) (cs_workers_in_memory_chunks_orphans)  # held after their release was lost
max by (tenant) (cs_metastore_chunks_in_memory_oldest_active_age_secs)
```

GC backlog:

```promql
sum by (tenant) (cs_metastore_chunks{in_memory="true", active="false"})
max by (tenant, in_memory) (cs_metastore_chunks_oldest_inactive_age_secs)
sum by (tenant, in_memory) (rate(cs_gc_chunks_deleted[5m]))
```

Scheduler lag:

```promql
sum by (tenant) (increase(cs_scheduler_events_dropped[5m]))
max by (tenant) (cs_scheduler_events_pending)
```

Compaction throughput and failures:

```promql
sum by (tenant, job_type) (rate(cs_jobs_completed[5m]))
sum by (tenant, job_type, error_class) (increase(cs_jobs_failed{error_class!="stale_stream"}[30m]))
sum by (tenant, job_type, status) (cs_jobs)
max by (tenant) (cs_metastore_in_memory_compaction_pending_partitions)
```

Streaming lag that is actually current:

```promql
# Lag and age share their labels, so each lag series is kept only if its own age is fresh.
max by (tenant, location) (cs_streaming_lag and cs_streaming_lag_age_ms < 60000)
```

Average data query latency (do not use the summary's max or quantiles for this):

```promql
sum by (tenant) (rate(cs_sql_query_data_ms_sum[5m]))
  / sum by (tenant) (rate(cs_sql_query_data_ms_count[5m]))
```

Main planning cost per query:

```promql
sum by (tenant) (rate(cs_sql_query_data_planning_choose_index_and_workers_us_sum[5m]))
  / sum by (tenant) (rate(cs_sql_query_data_planning_choose_index_and_workers_us_count[5m]))
```
