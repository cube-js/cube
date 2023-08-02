use crate::cachestore::{
    CacheItem, CacheStore, QueueItem, QueueItemStatus, QueueKey, QueueResult, QueueResultResponse,
    QueueRetrieveResponse,
};
use crate::metastore::job::{Job, JobStatus, JobType};
use crate::metastore::multi_index::{MultiIndex, MultiPartition};
use crate::metastore::replay_handle::{ReplayHandle, SeqPointer};
use crate::metastore::snapshot_info::SnapshotInfo;
use crate::metastore::source::{Source, SourceCredentials};
use crate::metastore::table::{StreamOffset, Table, TablePath};
use crate::metastore::{
    Chunk, ChunkMetaStoreTable, Column, IdRow, ImportFormat, Index, IndexDef, IndexMetaStoreTable,
    MetaStore, Partition, PartitionData, PartitionMetaStoreTable, RowKey, Schema,
    SchemaMetaStoreTable, TableMetaStoreTable, WAL,
};
use crate::table::Row;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub struct MetaStoreMock;

#[async_trait]
impl MetaStore for MetaStoreMock {
    async fn wait_for_current_seq_to_sync(&self) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    fn schemas_table(&self) -> SchemaMetaStoreTable {
        panic!("MetaStore mock!")
    }

    async fn create_schema(
        &self,
        _schema_name: String,
        _if_not_exists: bool,
    ) -> Result<IdRow<Schema>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_schemas(&self) -> Result<Vec<IdRow<Schema>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_schema_by_id(&self, _schema_id: u64) -> Result<IdRow<Schema>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_schema_id(&self, _schema_name: String) -> Result<u64, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_schema(&self, _schema_name: String) -> Result<IdRow<Schema>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn rename_schema(
        &self,
        _old_schema_name: String,
        _new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn rename_schema_by_id(
        &self,
        _schema_id: u64,
        _new_schema_name: String,
    ) -> Result<IdRow<Schema>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_schema(&self, _schema_name: String) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_schema_by_id(&self, _schema_id: u64) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    fn tables_table(&self) -> TableMetaStoreTable {
        panic!("MetaStore mock!")
    }

    async fn create_table(
        &self,
        _schema_name: String,
        _table_name: String,
        _columns: Vec<Column>,
        _locations: Option<Vec<String>>,
        _import_format: Option<ImportFormat>,
        _indexes: Vec<IndexDef>,
        _is_ready: bool,
        _build_range_end: Option<DateTime<Utc>>,
        _seal_at: Option<DateTime<Utc>>,
        _select_statement: Option<String>,
        _source_columns: Option<Vec<Column>>,
        _stream_offset: Option<StreamOffset>,
        _unique_key_column_names: Option<Vec<String>>,
        _aggregates: Option<Vec<(String, String)>>,
        _partition_split_threshold: Option<u64>,
        _trace_obj: Option<String>,
    ) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn table_ready(&self, _id: u64, _is_ready: bool) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn seal_table(&self, _id: u64) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn update_location_download_size(
        &self,
        _id: u64,
        _location: String,
        _download_size: u64,
    ) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_table(
        &self,
        _schema_name: String,
        _table_name: String,
    ) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_table_by_id(&self, _table_id: u64) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_tables(&self) -> Result<Vec<IdRow<Table>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_tables_with_path(
        &self,
        _include_non_ready: bool,
    ) -> Result<Arc<Vec<TablePath>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn not_ready_tables(
        &self,
        _created_seconds_ago: i64,
    ) -> Result<Vec<IdRow<Table>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn drop_table(&self, _table_id: u64) -> Result<IdRow<Table>, CubeError> {
        panic!("MetaStore mock!")
    }

    fn partition_table(&self) -> PartitionMetaStoreTable {
        panic!("MetaStore mock!")
    }

    async fn create_partition(&self, _partition: Partition) -> Result<IdRow<Partition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_partition(&self, _partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_partition_for_compaction(
        &self,
        _partition_id: u64,
    ) -> Result<
        (
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Option<IdRow<MultiPartition>>,
        ),
        CubeError,
    > {
        panic!("MetaStore mock!")
    }

    async fn get_partition_chunk_sizes(&self, _partition_id: u64) -> Result<u64, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn swap_compacted_chunks(
        &self,
        _partition_id: u64,
        _old_chunk_ids: Vec<u64>,
        _new_chunk: u64,
        _new_chunk_file_size: u64,
    ) -> Result<bool, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn swap_active_partitions(
        &self,
        _current_active: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        _new_active: Vec<(IdRow<Partition>, u64)>,
        _new_active_min_max: Vec<(u64, (Option<Row>, Option<Row>), (Option<Row>, Option<Row>))>,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_partition(&self, _partition_id: u64) -> Result<IdRow<Partition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn mark_partition_warmed_up(&self, _partition_id: u64) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_middle_man_partition(
        &self,
        _partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn can_delete_partition(&self, _partition_id: u64) -> Result<bool, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn can_delete_middle_man_partition(&self, _partition_id: u64) -> Result<bool, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_inactive_partitions_to_repartition(
        &self,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_inactive_middle_man_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_just_created_partitions(&self) -> Result<Vec<IdRow<Partition>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_partitions_with_chunks_created_seconds_ago(
        &self,
        _seconds_ago: i64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        panic!("MetaStore mock!")
    }

    fn index_table(&self) -> IndexMetaStoreTable {
        panic!("MetaStore mock!")
    }

    async fn create_index(
        &self,
        _schema_name: String,
        _table_name: String,
        _index_def: IndexDef,
    ) -> Result<IdRow<Index>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_default_index(&self, _table_id: u64) -> Result<IdRow<Index>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_table_indexes(&self, _table_id: u64) -> Result<Vec<IdRow<Index>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_active_partitions_by_index_id(
        &self,
        _index_id: u64,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_index(&self, _index_id: u64) -> Result<IdRow<Index>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn create_partitioned_index(
        &self,
        _schema: String,
        _name: String,
        _columns: Vec<Column>,
        _if_not_exists: bool,
    ) -> Result<IdRow<MultiIndex>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn drop_partitioned_index(
        &self,
        _schema: String,
        _name: String,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_multi_partition(&self, _id: u64) -> Result<IdRow<MultiPartition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_child_multi_partitions(
        &self,
        _id: u64,
    ) -> Result<Vec<IdRow<MultiPartition>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_multi_partition_subtree(
        &self,
        _multi_part_ids: Vec<u64>,
    ) -> Result<HashMap<u64, MultiPartition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn create_multi_partition(
        &self,
        _p: MultiPartition,
    ) -> Result<IdRow<MultiPartition>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn prepare_multi_partition_for_split(
        &self,
        _multi_partition_id: u64,
    ) -> Result<(IdRow<MultiIndex>, IdRow<MultiPartition>, Vec<PartitionData>), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn commit_multi_partition_split(
        &self,
        _multi_partition_id: u64,
        _new_multi_partitions: Vec<u64>,
        _new_multi_partition_rows: Vec<u64>,
        _old_partitions: Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>,
        _new_partitions: Vec<(IdRow<Partition>, u64)>,
        _new_partition_rows: Vec<u64>,
        _initial_split: bool,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn find_unsplit_partitions(
        &self,
        _multi_partition_id: u64,
    ) -> Result<Vec<u64>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn prepare_multi_split_finish(
        &self,
        _multi_partition_id: u64,
        _partition_id: u64,
    ) -> Result<(PartitionData, Vec<IdRow<MultiPartition>>), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_active_partitions_and_chunks_by_index_id_for_select(
        &self,
        _index_id: Vec<u64>,
    ) -> Result<Vec<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_warmup_partitions(
        &self,
    ) -> Result<Vec<(IdRow<Partition>, Vec<IdRow<Chunk>>)>, CubeError> {
        panic!("MetaStore mock!")
    }

    fn chunks_table(&self) -> ChunkMetaStoreTable {
        panic!("MetaStore mock!")
    }

    async fn create_chunk(
        &self,
        _partition_id: u64,
        _row_count: usize,
        _min: Option<Row>,
        _max: Option<Row>,
        _in_memory: bool,
    ) -> Result<IdRow<Chunk>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_chunk(&self, _chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_chunks_by_partition(
        &self,
        _partition_id: u64,
        _include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_used_disk_space_out_of_queue(
        &self,
        _node: Option<String>,
    ) -> Result<u64, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_all_partitions_and_chunks_out_of_queue(
        &self,
    ) -> Result<(Vec<IdRow<Partition>>, Vec<IdRow<Chunk>>), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_chunks_by_partition_out_of_queue(
        &self,
        _partition_id: u64,
        _include_inactive: bool,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn chunk_uploaded(&self, _chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn chunk_update_last_inserted(
        &self,
        _chunk_ids: Vec<u64>,
        _last_inserted_at: Option<DateTime<Utc>>,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn deactivate_chunk(&self, _chunk_id: u64) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn deactivate_chunks(&self, _chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn swap_chunks(
        &self,
        _deactivate_ids: Vec<u64>,
        _uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        _new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn swap_chunks_without_check(
        &self,
        _deactivate_ids: Vec<u64>,
        _uploaded_ids_and_sizes: Vec<(u64, Option<u64>)>,
        _new_replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn deactivate_chunks_without_check(
        &self,
        _deactivate_ids: Vec<u64>,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn activate_chunks(
        &self,
        _table_id: u64,
        _uploaded_chunk_ids: Vec<(u64, Option<u64>)>,
        _replay_handle_id: Option<u64>,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_chunk(&self, _chunk_id: u64) -> Result<IdRow<Chunk>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_inactive_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_inactive_not_uploaded_chunks(&self) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn create_wal(&self, _table_id: u64, _row_count: usize) -> Result<IdRow<WAL>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_wal(&self, _wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_wal(&self, _wal_id: u64) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn wal_uploaded(&self, _wal_id: u64) -> Result<IdRow<WAL>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_wals_for_table(&self, _table_id: u64) -> Result<Vec<IdRow<WAL>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn add_job(&self, _job: Job) -> Result<Option<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_job(&self, _job_id: u64) -> Result<IdRow<Job>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_job_by_ref(
        &self,
        _row_reference: RowKey,
        _job_type: JobType,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_orphaned_jobs(
        &self,
        _orphaned_timeout: Duration,
    ) -> Result<Vec<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_job(&self, _job_id: u64) -> Result<IdRow<Job>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn start_processing_job(
        &self,
        _server_name: String,
        _long_term: bool,
    ) -> Result<Option<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn update_status(
        &self,
        _job_id: u64,
        _status: JobStatus,
    ) -> Result<IdRow<Job>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn update_heart_beat(&self, _job_id: u64) -> Result<IdRow<Job>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_all_jobs(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn create_or_update_source(
        &self,
        _name: String,
        _credentials: SourceCredentials,
    ) -> Result<IdRow<Source>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_source(&self, _id: u64) -> Result<IdRow<Source>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_source_by_name(&self, _name: String) -> Result<IdRow<Source>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn delete_source(&self, _id: u64) -> Result<IdRow<Source>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn create_replay_handle(
        &self,
        _table_id: u64,
        _location_index: usize,
        _seq_pointer: SeqPointer,
    ) -> Result<IdRow<ReplayHandle>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn create_replay_handle_from_seq_pointers(
        &self,
        _table_id: u64,
        _seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<IdRow<ReplayHandle>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_replay_handles_by_table(
        &self,
        _table_id: u64,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_replay_handles_by_ids(
        &self,
        _ids: Vec<u64>,
    ) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_replay_handles(&self) -> Result<Vec<IdRow<ReplayHandle>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn all_replay_handles_to_merge(
        &self,
    ) -> Result<Vec<(IdRow<ReplayHandle>, bool)>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn update_replay_handle_failed_if_exists(
        &self,
        _id: u64,
        _failed: bool,
    ) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn replace_replay_handles(
        &self,
        _old_ids: Vec<u64>,
        _new_seq_pointer: Option<Vec<Option<SeqPointer>>>,
    ) -> Result<Option<IdRow<ReplayHandle>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_tables_with_indexes(
        &self,
        _table_name: Vec<(String, String)>,
    ) -> Result<Vec<(IdRow<Schema>, IdRow<Table>, Vec<IdRow<Index>>)>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn debug_dump(&self, _out_path: String) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_snapshots_list(&self) -> Result<Vec<SnapshotInfo>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn set_current_snapshot(&self, _snapshot_id: u128) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_chunks_out_of_queue(
        &self,
        _ids: Vec<u64>,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_partitions_out_of_queue(
        &self,
        _ids: Vec<u64>,
    ) -> Result<Vec<IdRow<Partition>>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn delete_chunks_without_checks(&self, _chunk_ids: Vec<u64>) -> Result<(), CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_chunks_without_partition_created_seconds_ago(
        &self,
        _seconds_ago: i64,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_index_with_active_partitions_out_of_queue(
        &self,
        _index_id: u64,
    ) -> Result<(IdRow<Index>, Vec<IdRow<Partition>>), CubeError> {
        panic!("MetaStore mock!")
    }
    async fn insert_chunks(&self, _chunks: Vec<Chunk>) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_jobs_on_non_exists_nodes(&self) -> Result<Vec<IdRow<Job>>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_trace_obj_by_table_id(&self, _table_id: u64) -> Result<Option<String>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_partition_out_of_queue(
        &self,
        _partition_id: u64,
    ) -> Result<IdRow<Partition>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_partitions_for_in_memory_compaction(
        &self,
        _node: String,
    ) -> Result<
        Vec<(
            IdRow<Partition>,
            IdRow<Index>,
            IdRow<Table>,
            Vec<IdRow<Chunk>>,
        )>,
        CubeError,
    > {
        panic!("MetaStore mock!")
    }
    async fn get_all_node_in_memory_chunks(
        &self,
        _node: String,
    ) -> Result<Vec<IdRow<Chunk>>, CubeError> {
        panic!("MetaStore mock!")
    }
    async fn get_table_indexes_out_of_queue(
        &self,
        _table_id: u64,
    ) -> Result<Vec<IdRow<Index>>, CubeError> {
        panic!("MetaStore mock!")
    }

    async fn get_all_filenames(&self) -> Result<Vec<String>, CubeError> {
        panic!("MetaStore mock!")
    }
}

crate::di_service!(MetaStoreMock, [MetaStore]);

pub struct CacheStoreMock;

#[async_trait]
impl CacheStore for CacheStoreMock {
    async fn cache_all(&self) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn cache_set(
        &self,
        _item: CacheItem,
        _update_if_not_exists: bool,
    ) -> Result<bool, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn cache_truncate(&self) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn cache_delete(&self, _key: String) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn cache_get(&self, _key: String) -> Result<Option<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn cache_keys(&self, _prefix: String) -> Result<Vec<IdRow<CacheItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn cache_incr(&self, _key: String) -> Result<IdRow<CacheItem>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_all(&self) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_results_all(&self) -> Result<Vec<IdRow<QueueResult>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_results_multi_delete(&self, _ids: Vec<u64>) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_add(
        &self,
        _item: QueueItem,
    ) -> Result<crate::cachestore::QueueAddResponse, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_truncate(&self) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_to_cancel(
        &self,
        _prefix: String,
        _orphaned_timeout: Option<u32>,
        _heartbeat_timeout: Option<u32>,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_list(
        &self,
        _prefix: String,
        _status_filter: Option<QueueItemStatus>,
        _priority_sort: bool,
    ) -> Result<Vec<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_get(&self, _key: QueueKey) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_cancel(&self, _key: QueueKey) -> Result<Option<IdRow<QueueItem>>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_heartbeat(&self, _key: QueueKey) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_retrieve_by_path(
        &self,
        _path: String,
        _allow_concurrency: u32,
    ) -> Result<QueueRetrieveResponse, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_ack(&self, _key: QueueKey, _result: Option<String>) -> Result<bool, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_result_by_path(
        &self,
        _path: String,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_result_blocking(
        &self,
        _key: QueueKey,
        _timeout: u64,
    ) -> Result<Option<QueueResultResponse>, CubeError> {
        panic!("CacheStore mock!")
    }

    async fn queue_merge_extra(&self, _key: QueueKey, _payload: String) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn compaction(&self) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }

    async fn healthcheck(&self) -> Result<(), CubeError> {
        panic!("CacheStore mock!")
    }
}

crate::di_service!(CacheStoreMock, [CacheStore]);
