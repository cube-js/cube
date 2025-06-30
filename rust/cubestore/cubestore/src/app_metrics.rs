//! We keep all CubeStore metrics in one place for discoverability.
//! The convention is to prefix all metrics with `cs.` (short for CubeStore).

use crate::util::metrics;
use crate::util::metrics::{Counter, Gauge, Histogram};

/// The number of process startups.
pub static STARTUPS: Counter = metrics::counter("cs.startup");

/// Errors in IPC.
pub static WORKER_POOL_ERROR: Counter = metrics::counter("cs.worker_pool.errors");

/// Incoming SQL queries that do data reads.
pub static DATA_QUERIES: Counter = metrics::counter("cs.sql.query.data");
pub static DATA_QUERIES_CACHE_HIT: Counter = metrics::counter("cs.sql.query.data.cache.hit");
// Approximate number of entries in this cache.
pub static DATA_QUERIES_CACHE_SIZE: Gauge = metrics::gauge("cs.sql.query.data.cache.size");
// Approximate total weighted size of entries in this cache.
pub static DATA_QUERIES_CACHE_WEIGHT: Gauge = metrics::gauge("cs.sql.query.data.cache.weight");
pub static DATA_QUERY_TIME_MS: Histogram = metrics::histogram("cs.sql.query.data.ms");
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
pub static IN_MEMORY_CHUNKS_COUNT: Gauge = metrics::gauge("cs.workers.in_memory_chunks)");
pub static IN_MEMORY_CHUNKS_ROWS: Gauge = metrics::gauge("cs.workers.in_memory_chunks.rows)");
pub static IN_MEMORY_CHUNKS_MEMORY: Gauge = metrics::gauge("cs.workers.in_memory_chunks.memory)");
pub static STREAMING_IMPORT_TIME: Histogram = metrics::histogram("cs.streaming.import_time.ms");
pub static STREAMING_PARTITION_TIME: Histogram =
    metrics::histogram("cs.streaming.partition_time.ms");
pub static STREAMING_UPLOAD_TIME: Histogram = metrics::histogram("cs.streaming.upload_time.ms");
pub static STREAMING_ROUNDTRIP_TIME: Histogram =
    metrics::histogram("cs.streaming.roundtrip_time.ms");
pub static STREAMING_ROUNDTRIP_ROWS: Histogram = metrics::histogram("cs.streaming.roundtrip_rows");
pub static STREAMING_ROUNDTRIP_CHUNKS: Histogram =
    metrics::histogram("cs.streaming.roundtrip_chunks");
pub static STREAMING_LAG: Gauge = metrics::gauge("cs.streaming.lag");

pub static METASTORE_QUEUE: Gauge = metrics::gauge("cs.metastore.queue_size");
pub static METASTORE_READ_OPERATION: Histogram =
    metrics::histogram("cs.metastore.read_operation.ms");
pub static METASTORE_INNER_READ_OPERATION: Histogram =
    metrics::histogram("cs.metastore.inner_read_operation.ms");
pub static METASTORE_WRITE_OPERATION: Histogram =
    metrics::histogram("cs.metastore.write_operation.ms");
pub static METASTORE_INNER_WRITE_OPERATION: Histogram =
    metrics::histogram("cs.metastore.inner_write_operation.ms");
pub static METASTORE_READ_OUT_QUEUE_OPERATION: Histogram =
    metrics::histogram("cs.metastore.read_out_queue_operation.ms");

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

/// RemoteFs metrics
pub static REMOTE_FS_OPERATION_CORE: Counter = metrics::counter("cs.remote_fs.operations.core");
pub static REMOTE_FS_FILES_TO_REMOVE: Gauge = metrics::gauge("cs.remote_fs.files_to_remove.count");
pub static REMOTE_FS_FILES_SIZE_TO_REMOVE: Gauge =
    metrics::gauge("cs.remote_fs.files_to_remove.size");

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
