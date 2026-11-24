//! We keep all CubeStore metrics in one place for discoverability.
//! The convention is to prefix all metrics with `cs.` (short for CubeStore).

use crate::util::metrics;
use crate::util::metrics::{Counter, Gauge, GaugeFamily, Histogram};

/// The number of process startups.
pub static STARTUPS: Counter = metrics::counter("cs.startup");

/// Process starts tagged `version`, `build`, `role` and `slot`, counted once the role is known.
pub static PROCESS_STARTS: Counter = metrics::counter("cs.process.started");

/// Ticks periodically in every live process, tagged like [PROCESS_STARTS]. A counter
/// rather than a gauge: a statsd bridge may keep serving a dead process's last gauge value.
pub static PROCESS_HEARTBEAT: Counter = metrics::counter("cs.process.heartbeat");
pub static PROCESS_RSS_BYTES: Gauge = metrics::gauge("cs.process.rss_bytes");
/// Peak resident set since the process started; catches spikes between samples.
pub static PROCESS_PEAK_RSS_BYTES: Gauge = metrics::gauge("cs.process.peak_rss_bytes");
/// Every panic, caught or not.
pub static PANICS: Counter = metrics::counter("cs.panics");

/// Pool subprocesses that ended while processing a request, tagged `reason`.
pub static WORKER_POOL_PROCESS_EXITS: Counter = metrics::counter("cs.worker_pool.process_exits");

/// Tables deactivated on corrupt data, tagged `source`: `stream` tables fail on upstream stream
/// errors, so they are told apart from `batch` ones.
pub static TABLES_DEACTIVATED: Counter = metrics::counter("cs.tables.deactivated");
pub static REMOTE_FS_FILE_SIZE_MISMATCH: Counter =
    metrics::counter("cs.remote_fs.file_size_mismatch");
/// Inter-node messages that could not be decoded, tagged `reason`: `version_mismatch` is protocol
/// skew between builds, `undecodable` a payload this build can't read.
pub static RPC_DECODE_ERRORS: Counter = metrics::counter("cs.rpc.decode_errors");

/// Errors in IPC.
pub static WORKER_POOL_ERROR: Counter = metrics::counter("cs.worker_pool.errors");

/// Selects counted on arrival, before planning. Compare against [DATA_QUERIES] tagged
/// `command:select` — untagged it also counts DDL/DML, and system-table selects land in
/// [META_QUERIES]; the gap is the planning backlog.
pub static DATA_QUERIES_INCOMING: Counter = metrics::counter("cs.sql.query.data.incoming");

/// Queries counted once planned and dispatched, tagged with the command they ran.
pub static DATA_QUERIES: Counter = metrics::counter("cs.sql.query.data");
pub static DATA_QUERIES_CACHE_HIT: Counter = metrics::counter("cs.sql.query.data.cache.hit");
pub static DATA_QUERIES_CACHE_STALE_HIT: Counter =
    metrics::counter("cs.sql.query.data.cache.stale_hit");
// Approximate number of entries in this cache.
pub static DATA_QUERIES_CACHE_SIZE: Gauge = metrics::gauge("cs.sql.query.data.cache.size");
// Approximate total weighted size of entries in this cache.
pub static DATA_QUERIES_CACHE_WEIGHT: Gauge = metrics::gauge("cs.sql.query.data.cache.weight");
pub static DATA_QUERIES_STALE_CACHE_SIZE: Gauge =
    metrics::gauge("cs.sql.query.data.cache.stale.size");
pub static DATA_QUERIES_STALE_CACHE_WEIGHT: Gauge =
    metrics::gauge("cs.sql.query.data.cache.stale.weight");
pub static DATA_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.data.ms");
pub static QUERY_PLANNING_THROTTLE_IN_FLIGHT: Gauge =
    metrics::gauge("cs.sql.query.data.planning.throttle.in_flight");
pub static QUERY_PLANNING_THROTTLE_QUEUED: Gauge =
    metrics::gauge("cs.sql.query.data.planning.throttle.queued");
/// Only queries that actually waited for a slot are reported, so this describes the waiting
/// minority, not the typical query.
pub static QUERY_PLANNING_THROTTLE_WAIT_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.throttle.wait.us");
pub static QUERY_PLANNING_THROTTLE_REJECTED: Counter =
    metrics::counter("cs.sql.query.data.planning.throttle.rejected");
pub static DATA_QUERY_LOGICAL_PLAN_TOTAL_CREATION_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.logical_plan.total_creation.us");
pub static DATA_QUERY_LOGICAL_PLAN_EXECUTION_CONTEXT_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.logical_plan.execution_context.us");
pub static DATA_QUERY_LOGICAL_PLAN_QUERY_PLANNER_SETUP_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.logical_plan.query_planner_setup.us");
pub static DATA_QUERY_LOGICAL_PLAN_STATEMENT_TO_PLAN_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.logical_plan.statement_to_plan.us");

pub static DATA_QUERY_LOGICAL_PLAN_OPTIMIZE_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.logical_plan.optimize.us");
pub static DATA_QUERY_LOGICAL_PLAN_IS_DATA_SELECT_QUERY_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.logical_plan.is_data_select_query.us");

pub static DATA_QUERY_CHOOSE_INDEX_AND_WORKERS_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.choose_index_and_workers.us");
pub static DATA_QUERY_CHOOSE_INDEX_EXT_GET_TABLES_WITH_INDICES_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.choose_index_ext.get_tables_with_indices.us");
pub static DATA_QUERY_CHOOSE_INDEX_EXT_PICK_INDEX_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.choose_index_ext.pick_index.us");
pub static DATA_QUERY_CHOOSE_INDEX_EXT_GET_ACTIVE_PARTITIONS_AND_CHUNKS_BY_INDEX_ID_TIME_US:
    Histogram = metrics::histogram(
    "cs.sql.query.data.planning.choose_index_ext.get_active_partitions_and_chunks_by_index_id.us",
);
pub static DATA_QUERY_CHOOSE_INDEX_EXT_GET_MULTI_PARTITION_SUBTREE_TIME_US: Histogram =
    metrics::histogram(
        "cs.sql.query.data.planning.choose_index_ext.get_multi_partition_subtree.us",
    );
pub static DATA_QUERY_CHOOSE_INDEX_EXT_TOTAL_AWAITING_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.choose_index_ext.total_awaiting.us");

pub static DATA_QUERY_TO_SERIALIZED_PLAN_TIME_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.to_serialized_plan.us");
pub static DATA_QUERY_CREATE_ROUTER_PHYSICAL_PLAN_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.router_plan.us");
pub static DATA_QUERY_CREATE_WORKER_PHYSICAL_PLAN_US: Histogram =
    metrics::histogram("cs.sql.query.data.planning.worker_plan.us");

pub static SQL_DATA_FRAME_SERIALIZATION_TIME_US: Histogram =
    metrics::histogram("cs.sql.data_frame_serialization.us");
pub static HTTP_MESSAGE_DATA_FRAME_SERIALIZATION_TIME_US: Histogram =
    metrics::histogram("cs.http.data_frame_serialization.us");

/// Incoming SQL queries that only read metadata or do trivial computations.
pub static META_QUERIES: Counter = metrics::counter("cs.sql.query.meta");
pub static META_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.meta.ms");
/// Incoming cache queries.
pub static CACHE_QUERIES: Counter = metrics::counter("cs.sql.query.cache");
pub static CACHE_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.cache.ms");
/// Incoming queue queries.
pub static QUEUE_QUERIES: Counter = metrics::counter("cs.sql.query.queue");
pub static QUEUE_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.queue.ms");
pub static STREAMING_ROWS_READ: Counter = metrics::counter("cs.streaming.rows");
pub static STREAMING_CHUNKS_READ: Counter = metrics::counter("cs.streaming.chunks");
pub static STREAMING_LASTOFFSET: Gauge = metrics::gauge("cs.streaming.lastoffset");
/// In-memory chunks held by this process, whatever their state in the metastore.
pub static IN_MEMORY_CHUNKS_COUNT: Gauge = metrics::gauge("cs.workers.in_memory_chunks");
pub static IN_MEMORY_CHUNKS_ROWS: Gauge = metrics::gauge("cs.workers.in_memory_chunks.rows");
pub static IN_MEMORY_CHUNKS_MEMORY: Gauge = metrics::gauge("cs.workers.in_memory_chunks.memory");
/// Held in-memory chunks the metastore lists as active.
pub static IN_MEMORY_CHUNKS_ACTIVE: Gauge = metrics::gauge("cs.workers.in_memory_chunks.active");
/// Held in-memory chunks the metastore lists as deactivated, i.e. waiting for GC.
pub static IN_MEMORY_CHUNKS_INACTIVE: Gauge =
    metrics::gauge("cs.workers.in_memory_chunks.inactive");
/// Held in-memory chunks whose metastore row was already deleted, i.e. whose release was lost.
pub static IN_MEMORY_CHUNKS_ORPHANS: Gauge = metrics::gauge("cs.workers.in_memory_chunks.orphans");
pub static STREAMING_IMPORT_TIME: Histogram = metrics::histogram("cs.streaming.import_time.ms");
pub static STREAMING_PARTITION_TIME: Histogram =
    metrics::histogram("cs.streaming.partition_time.ms");
pub static STREAMING_UPLOAD_TIME: Histogram = metrics::histogram("cs.streaming.upload_time.ms");
pub static STREAMING_ROUNDTRIP_TIME: Histogram =
    metrics::histogram("cs.streaming.roundtrip_time.ms");
pub static STREAMING_ROUNDTRIP_ROWS: Histogram = metrics::histogram("cs.streaming.roundtrip_rows");
pub static STREAMING_ROUNDTRIP_CHUNKS: Histogram =
    metrics::histogram("cs.streaming.roundtrip_chunks");
/// High watermark minus the last consumed offset, refreshed periodically with or without traffic.
pub static STREAMING_LAG: Gauge = metrics::gauge("cs.streaming.lag");
/// Time since [STREAMING_LAG] was last computed; grows while the watermark fetch fails.
pub static STREAMING_LAG_AGE_MS: Gauge = metrics::gauge("cs.streaming.lag.age.ms");

/// Chunks in the metastore, tagged `in_memory` and `active`.
pub static METASTORE_CHUNKS: Gauge = metrics::gauge("cs.metastore.chunks");
pub static METASTORE_CHUNKS_FAMILY: GaugeFamily = GaugeFamily::new(&METASTORE_CHUNKS);
pub static METASTORE_CHUNKS_ROWS: Gauge = metrics::gauge("cs.metastore.chunks.rows");
pub static METASTORE_CHUNKS_ROWS_FAMILY: GaugeFamily = GaugeFamily::new(&METASTORE_CHUNKS_ROWS);
/// Active in-memory chunks per owning node, tagged `node`; compare with
/// [IN_MEMORY_CHUNKS_ACTIVE] of that node.
pub static METASTORE_IN_MEMORY_CHUNKS_BY_NODE: Gauge =
    metrics::gauge("cs.metastore.chunks.in_memory_active_by_node");
pub static METASTORE_IN_MEMORY_CHUNKS_BY_NODE_FAMILY: GaugeFamily =
    GaugeFamily::new(&METASTORE_IN_MEMORY_CHUNKS_BY_NODE);
pub static METASTORE_OLDEST_ACTIVE_IN_MEMORY_CHUNK_AGE_SECS: Gauge =
    metrics::gauge("cs.metastore.chunks.in_memory.oldest_active_age.secs");
/// Time since the oldest deactivated chunk was deactivated, tagged `in_memory`: the GC backlog.
pub static METASTORE_OLDEST_INACTIVE_CHUNK_AGE_SECS: Gauge =
    metrics::gauge("cs.metastore.chunks.oldest_inactive_age.secs");
pub static METASTORE_OLDEST_INACTIVE_CHUNK_AGE_SECS_FAMILY: GaugeFamily =
    GaugeFamily::new(&METASTORE_OLDEST_INACTIVE_CHUNK_AGE_SECS);

pub static IN_MEMORY_COMPACTION_DISCOVERY_MS: Histogram =
    metrics::histogram("cs.metastore.in_memory_compaction.discovery.ms");
pub static IN_MEMORY_COMPACTION_DISCOVERY_AGE_MS: Gauge =
    metrics::gauge("cs.metastore.in_memory_compaction.discovery.age.ms");
/// Partitions holding active in-memory chunks, as of the last discovery scan.
pub static IN_MEMORY_COMPACTION_PENDING_PARTITIONS: Gauge =
    metrics::gauge("cs.metastore.in_memory_compaction.pending_partitions");

/// Chunk rows deleted by GC, tagged `in_memory`.
pub static GC_CHUNKS_DELETED: Counter = metrics::counter("cs.gc.chunks_deleted");
pub static GC_CHUNK_DELETE_BATCHES: Counter = metrics::counter("cs.gc.chunk_delete_batches");

/// Metastore events the scheduler lost because its channel overflowed. Lost chunk deletions
/// are only recovered by the workers' in-memory chunk reconcile.
pub static SCHEDULER_EVENTS_DROPPED: Counter = metrics::counter("cs.scheduler.events_dropped");
pub static SCHEDULER_EVENTS_PENDING: Gauge = metrics::gauge("cs.scheduler.events_pending");

/// Metastore and cachestore read-write loops, tagged `store` and `loop`. Reported as sums so the
/// mean wait (`operation_time.us` / `operations`) and loop utilisation (`busy.us` per second) can
/// be aggregated across hosts.
pub static RW_LOOP_OPERATIONS: Counter = metrics::counter("cs.metastore.operations");
/// Wall time of operations as seen by callers, queueing included.
pub static RW_LOOP_OPERATION_TIME_US: Counter = metrics::counter("cs.metastore.operation_time.us");
/// Time the loop thread spent executing tasks.
pub static RW_LOOP_BUSY_US: Counter = metrics::counter("cs.metastore.rw_loop.busy.us");
pub static RW_LOOP_QUEUE_SIZE: Gauge = metrics::gauge("cs.metastore.rw_loop.queue_size");

pub static CACHESTORE_ROCKSDB_ESTIMATE_LIVE_DATA_SIZE: Gauge =
    metrics::gauge("cs.cachestore.rocksdb.estimate_live_data_size");
pub static CACHESTORE_ROCKSDB_LIVE_SST_FILES_SIZE: Gauge =
    metrics::gauge("cs.cachestore.rocksdb.live_sst_files_size");
pub static CACHESTORE_ROCKSDB_CF_DEFAULT_SIZE: Gauge =
    metrics::gauge("cs.cachestore.rocksdb.cf.default.size");
pub static CACHESTORE_SCHEDULER_GC_QUEUE: Gauge =
    metrics::gauge("cs.cachestore.scheduler.gc_queue");

// TODO: Maybe these should be a single metric that uses tags.
pub static JOBS_PARTITION_COMPACTION: Counter =
    metrics::counter("cs.jobs.partition_compaction.count");
pub static JOBS_PARTITION_COMPACTION_COMPLETED: Counter =
    metrics::counter("cs.jobs.partition_compaction.completed");
pub static JOBS_PARTITION_COMPACTION_FAILURES: Counter =
    metrics::counter("cs.jobs.partition_compaction.failures");
pub static JOBS_MULTI_PARTITION_SPLIT: Counter =
    metrics::counter("cs.jobs.multi_partition_split.count");
pub static JOBS_MULTI_PARTITION_SPLIT_COMPLETED: Counter =
    metrics::counter("cs.jobs.multi_partition_split.completed");
pub static JOBS_MULTI_PARTITION_SPLIT_FAILURES: Counter =
    metrics::counter("cs.jobs.multi_partition_split.failures");
pub static JOBS_FINISH_MULTI_SPLIT: Counter = metrics::counter("cs.jobs.finish_multi_split.count");
pub static JOBS_FINISH_MULTI_SPLIT_COMPLETED: Counter =
    metrics::counter("cs.jobs.finish_multi_split.completed");
pub static JOBS_FINISH_MULTI_SPLIT_FAILURES: Counter =
    metrics::counter("cs.jobs.finish_multi_split.failures");
pub static JOBS_REPARTITION_CHUNK: Counter = metrics::counter("cs.jobs.repartition_chunk.count");
pub static JOBS_REPARTITION_CHUNK_COMPLETED: Counter =
    metrics::counter("cs.jobs.repartition_chunk.completed");
pub static JOBS_REPARTITION_CHUNK_FAILURES: Counter =
    metrics::counter("cs.jobs.repartition_chunk.failures");
/// Jobs in the metastore, tagged `job_type` and `status` (what `system.jobs` shows).
pub static JOBS: Gauge = metrics::gauge("cs.jobs");
pub static JOBS_FAMILY: GaugeFamily = GaugeFamily::new(&JOBS);
/// Jobs added to the metastore, tagged `job_type`. A job already queued for the same row is
/// not added again and not counted.
pub static JOBS_SCHEDULED: Counter = metrics::counter("cs.jobs.scheduled");
pub static JOBS_COMPLETED: Counter = metrics::counter("cs.jobs.completed");
/// Tagged `job_type` and `error_class`.
pub static JOBS_FAILED: Counter = metrics::counter("cs.jobs.failed");
/// Jobs removed by the cleanup sweep because their type is unknown to this
/// binary. Non-zero means a newer binary wrote a job type this one cannot run
/// (version skew across `latest`/`release` channels).
pub static JOBS_UNKNOWN_DELETED: Counter = metrics::counter("cs.jobs.unknown.deleted");

/// RemoteFs metrics
pub static REMOTE_FS_OPERATION_CORE: Counter = metrics::counter("cs.remote_fs.operations.core");
pub static REMOTE_FS_FILES_TO_REMOVE: Gauge = metrics::gauge("cs.remote_fs.files_to_remove.count");
pub static REMOTE_FS_FILES_SIZE_TO_REMOVE: Gauge =
    metrics::gauge("cs.remote_fs.files_to_remove.size");
pub static REMOTE_FS_FILES_REMOVED: Counter = metrics::counter("cs.remote_fs.files_removed.count");

/// Startup warmup files that were gone by the time the pass reached them, which says how far
/// behind its snapshot a pass ran rather than that anything is wrong.
pub static WARMUP_MISSING: Counter = metrics::counter("cs.warmup.missing");

/// Cache Store Cache
pub static CACHESTORE_TTL_PERSIST: Counter = metrics::counter("cs.cachestore.ttl.persist");
pub static CACHESTORE_TTL_BUFFER: Gauge = metrics::gauge("cs.cachestore.ttl.buffer");
// Cache Store Eviction
pub static CACHESTORE_EVICTION_REMOVED_EXPIRED_KEYS: Counter =
    metrics::counter("cs.cachestore.eviction.expired.keys");
pub static CACHESTORE_EVICTION_REMOVED_EXPIRED_SIZE: Counter =
    metrics::counter("cs.cachestore.eviction.expired.size");
pub static CACHESTORE_EVICTION_REMOVED_KEYS: Counter =
    metrics::counter("cs.cachestore.eviction.removed.keys");
pub static CACHESTORE_EVICTION_REMOVED_SIZE: Counter =
    metrics::counter("cs.cachestore.eviction.removed.size");
