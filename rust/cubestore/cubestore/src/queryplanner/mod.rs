pub mod hll;
pub mod optimizations;
pub mod panic;
mod partition_filter;
mod planning;
// use datafusion::physical_plan::parquet::MetadataCacheFactory;
pub use planning::PlanningMeta;
mod check_memory;
pub mod physical_plan_flags;
pub mod pretty_printers;
mod projection_above_limit;
pub mod query_executor;
pub mod serialized_plan;
mod tail_limit;
mod topk;
pub mod trace_data_loaded;
pub use topk::MIN_TOPK_STREAM_ROWS;
mod coalesce;
mod filter_by_key_range;
mod flatten_union;
pub mod info_schema;
mod merge_sort;
pub mod metadata_cache;
pub mod now;
pub mod providers;
#[cfg(test)]
mod test_utils;
// pub mod udf_xirr;
pub mod udfs;

use crate::cachestore::CacheStore;
use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::table::{Table, TablePath};
use crate::metastore::{IdRow, MetaStore};
use crate::queryplanner::flatten_union::FlattenUnion;
use crate::queryplanner::info_schema::{
    ColumnsInfoSchemaTableDef, RocksDBPropertiesTableDef, SchemataInfoSchemaTableDef,
    SystemCacheTableDef, SystemChunksTableDef, SystemIndexesTableDef, SystemJobsTableDef,
    SystemPartitionsTableDef, SystemQueueResultsTableDef, SystemQueueTableDef,
    SystemReplayHandlesTableDef, SystemSnapshotsTableDef, SystemTablesTableDef,
    TablesInfoSchemaTableDef,
};
// use crate::queryplanner::now::MaterializeNow;
use crate::queryplanner::planning::{choose_index_ext, ClusterSendNode};
// TODO upgrade DF
// use crate::queryplanner::projection_above_limit::ProjectionAboveLimit;
use crate::queryplanner::query_executor::{
    batches_to_dataframe, ClusterSendExec, InlineTableProvider,
};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::queryplanner::topk::ClusterAggregateTopK;
// use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{scalar_udf_by_kind, CubeAggregateUDFKind, CubeScalarUDFKind};

use crate::queryplanner::metadata_cache::MetadataCacheFactory;
use crate::queryplanner::pretty_printers::{pp_plan, pp_plan_ext, PPOptions};
use crate::sql::cache::SqlResultCache;
use crate::sql::InlineTables;
use crate::store::DataFrame;
use crate::{app_metrics, metastore, CubeError};
use async_trait::async_trait;
use core::fmt;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{datatypes::Schema, datatypes::SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::TableReference;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::{provider_as_source, DefaultTableSource, TableType};
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::{
    AggregateUDF, Expr, Extension, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::{cube_ext, datasource::TableProvider};
use futures::TryStreamExt;
use futures_util::TryFutureExt;
use log::{debug, trace};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use smallvec::alloc::fmt::Formatter;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::SystemTime;

#[automock]
#[async_trait]
pub trait QueryPlanner: DIService + Send + Sync {
    async fn logical_plan(
        &self,
        statement: Statement,
        inline_tables: &InlineTables,
        trace_obj: Option<String>,
    ) -> Result<QueryPlan, CubeError>;
    async fn execute_meta_plan(&self, plan: LogicalPlan) -> Result<DataFrame, CubeError>;
}

crate::di_service!(MockQueryPlanner, [QueryPlanner]);

pub struct QueryPlannerImpl {
    meta_store: Arc<dyn MetaStore>,
    cache_store: Arc<dyn CacheStore>,
    config: Arc<dyn ConfigObj>,
    cache: Arc<SqlResultCache>,
    metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
}

crate::di_service!(QueryPlannerImpl, [QueryPlanner]);

pub enum QueryPlan {
    Meta(LogicalPlan),
    Select(SerializedPlan, /*workers*/ Vec<String>),
}

#[async_trait]
impl QueryPlanner for QueryPlannerImpl {
    async fn logical_plan(
        &self,
        statement: Statement,
        inline_tables: &InlineTables,
        trace_obj: Option<String>,
    ) -> Result<QueryPlan, CubeError> {
        let ctx = self.execution_context().await?;

        let state = Arc::new(ctx.state());
        let schema_provider = MetaStoreSchemaProvider::new(
            self.meta_store.get_tables_with_path(false).await?,
            self.meta_store.clone(),
            self.cache_store.clone(),
            inline_tables,
            self.cache.clone(),
            state.clone(),
        );

        let query_planner = SqlToRel::new(&schema_provider);
        let mut logical_plan = query_planner.statement_to_plan(statement)?;

        // TODO upgrade DF remove
        trace!(
            "Initial Logical Plan: {}",
            pp_plan_ext(
                &logical_plan,
                &PPOptions {
                    show_filters: true,
                    show_sort_by: true,
                    show_aggregations: true,
                    show_output_hints: true,
                    show_check_memory_nodes: false,
                }
            )
        );

        logical_plan = state.optimize(&logical_plan)?;
        trace!(
            "Logical Plan: {}",
            pp_plan_ext(
                &logical_plan,
                &PPOptions {
                    show_filters: true,
                    show_sort_by: true,
                    show_aggregations: true,
                    show_output_hints: true,
                    show_check_memory_nodes: false,
                }
            )
        );

        let plan = if SerializedPlan::is_data_select_query(&logical_plan) {
            let (logical_plan, meta) = choose_index_ext(
                logical_plan,
                &self.meta_store.as_ref(),
                self.config.enable_topk(),
            )
            .await?;
            let workers = compute_workers(
                self.config.as_ref(),
                &logical_plan,
                &meta.multi_part_subtree,
            )?;
            QueryPlan::Select(
                SerializedPlan::try_new(logical_plan, meta, trace_obj).await?,
                workers,
            )
        } else {
            QueryPlan::Meta(logical_plan)
        };

        Ok(plan)
    }

    async fn execute_meta_plan(&self, plan: LogicalPlan) -> Result<DataFrame, CubeError> {
        let ctx = self.execution_context().await?;

        let plan_ctx = ctx.clone();
        let plan_to_move = plan.clone();
        let physical_plan = plan_ctx.state().create_physical_plan(&plan_to_move).await?;

        let execution_time = SystemTime::now();
        let results = collect(physical_plan, Arc::new(TaskContext::default())).await?;
        let execution_time = execution_time.elapsed()?;
        app_metrics::META_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        debug!("Meta query data processing time: {:?}", execution_time,);
        let data_frame = cube_ext::spawn_blocking(move || batches_to_dataframe(results)).await??;
        Ok(data_frame)
    }
}

impl QueryPlannerImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        cache_store: Arc<dyn CacheStore>,
        config: Arc<dyn ConfigObj>,
        cache: Arc<SqlResultCache>,
        metadata_cache_factory: Arc<dyn MetadataCacheFactory>,
    ) -> Arc<QueryPlannerImpl> {
        Arc::new(QueryPlannerImpl {
            meta_store,
            cache_store,
            config,
            cache,
            metadata_cache_factory,
        })
    }
}

impl QueryPlannerImpl {
    async fn execution_context(&self) -> Result<Arc<SessionContext>, CubeError> {
        let context = SessionContext::new();
        // TODO upgrade DF
        // context
        // .with_metadata_cache_factory(self.metadata_cache_factory.clone())
        // .add_optimizer_rule(Arc::new(MaterializeNow {}));
        // TODO upgrade DF
        // context
        // .add_optimizer_rule(Arc::new(ProjectionAboveLimit {})),
        Ok(Arc::new(context))
    }
}

struct MetaStoreSchemaProvider {
    /// Keeps the data used by [by_name] alive.
    _data: Arc<Vec<TablePath>>,
    by_name: HashSet<TableKey>,
    meta_store: Arc<dyn MetaStore>,
    cache_store: Arc<dyn CacheStore>,
    inline_tables: InlineTables,
    cache: Arc<SqlResultCache>,
    config_options: ConfigOptions,
    session_state: Arc<SessionState>,
}

/// Points into [MetaStoreSchemaProvider::data], never null.
struct TableKey(*const TablePath);
unsafe impl Send for TableKey {}
unsafe impl Sync for TableKey {}

impl TableKey {
    fn qual_name(&self) -> (&str, &str) {
        let s = unsafe { &*self.0 };
        (s.schema_lower_name.as_str(), s.table_lower_name.as_str())
    }
}

impl PartialEq for TableKey {
    fn eq(&self, o: &Self) -> bool {
        self.qual_name() == o.qual_name()
    }
}
impl Eq for TableKey {}
impl Hash for TableKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.qual_name().hash(state)
    }
}

impl MetaStoreSchemaProvider {
    pub fn new(
        tables: Arc<Vec<TablePath>>,
        meta_store: Arc<dyn MetaStore>,
        cache_store: Arc<dyn CacheStore>,
        inline_tables: &InlineTables,
        cache: Arc<SqlResultCache>,
        session_state: Arc<SessionState>,
    ) -> Self {
        let by_name = tables.iter().map(|t| TableKey(t)).collect();
        Self {
            _data: tables,
            by_name,
            meta_store,
            cache_store,
            cache,
            inline_tables: (*inline_tables).clone(),
            config_options: ConfigOptions::new(),
            session_state,
        }
    }
}

impl ContextProvider for MetaStoreSchemaProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> Result<Arc<dyn TableSource>, DataFusionError> {
        let (schema, table) = match &name {
            TableReference::Partial { schema, table } => (schema.clone(), table.clone()),
            TableReference::Bare { table } => {
                let table = self
                    .inline_tables
                    .iter()
                    .find(|inline_table| inline_table.name == table.as_ref())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!("Inline table {} was not found", name))
                    })?;
                return Ok(provider_as_source(Arc::new(InlineTableProvider::new(
                    table.id,
                    table.data.clone(),
                    Vec::new(),
                ))));
            }
            TableReference::Full { .. } => {
                return Err(DataFusionError::Plan(format!(
                    "Catalog table names aren't supported but {} was provided",
                    name
                )))
            }
        };

        // Mock table path for hash set access.
        let table_path = TablePath::new(
            Arc::new(IdRow::new(0, metastore::Schema::new(schema.to_string()))),
            IdRow::new(
                u64::MAX,
                Table::new(
                    table.to_string(),
                    u64::MAX,
                    Vec::new(),
                    None,
                    None,
                    false,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Vec::new(),
                    None,
                    None,
                    None,
                ),
            ),
        );

        let res = self
            .by_name
            .get(&TableKey(&table_path))
            .map(|table| -> Arc<dyn TableProvider> {
                let table = unsafe { &*table.0 };
                let schema = Arc::new(Schema::new(
                    table
                        .table
                        .get_row()
                        .get_columns()
                        .iter()
                        .map(|c| c.clone().into())
                        .collect::<Vec<Field>>(),
                ));
                Arc::new(CubeTableLogical {
                    table: table.clone(),
                    schema,
                })
            });
        res.or_else(|| -> Option<Arc<dyn TableProvider>> {
            match (schema.as_ref(), table.as_ref()) {
                ("information_schema", "columns") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::Columns,
                ))),
                ("information_schema", "tables") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::Tables,
                ))),
                ("information_schema", "schemata") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::Schemata,
                ))),
                ("system", "query_cache") => Some(Arc::new(
                    providers::InfoSchemaQueryCacheTableProvider::new(self.cache.clone()),
                )),
                ("system", "cache") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemCache,
                ))),
                ("system", "tables") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemTables,
                ))),
                ("system", "indexes") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemIndexes,
                ))),
                ("system", "partitions") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemPartitions,
                ))),
                ("system", "chunks") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemChunks,
                ))),
                ("system", "queue") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemQueue,
                ))),
                ("system", "queue_results") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemQueueResults,
                ))),
                ("system", "replay_handles") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemReplayHandles,
                ))),
                ("system", "jobs") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemJobs,
                ))),
                ("system", "snapshots") => Some(Arc::new(InfoSchemaTableProvider::new(
                    self.meta_store.clone(),
                    self.cache_store.clone(),
                    InfoSchemaTable::SystemSnapshots,
                ))),
                ("metastore", "rocksdb_properties") => {
                    Some(Arc::new(InfoSchemaTableProvider::new(
                        self.meta_store.clone(),
                        self.cache_store.clone(),
                        InfoSchemaTable::MetastoreRocksDBProperties,
                    )))
                }
                ("cachestore", "rocksdb_properties") => {
                    Some(Arc::new(InfoSchemaTableProvider::new(
                        self.meta_store.clone(),
                        self.cache_store.clone(),
                        InfoSchemaTable::CachestoreRocksDBProperties,
                    )))
                }
                _ => None,
            }
        })
        .map(|p| provider_as_source(p))
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Table {} was not found\n{:?}\n{:?}",
                name, table_path, self._data
            ))
        })
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        // TODO upgrade DF
        let kind = match name {
            "cardinality" | "CARDINALITY" => CubeScalarUDFKind::HllCardinality,
            // "coalesce" | "COALESCE" => CubeScalarUDFKind::Coalesce,
            // "now" | "NOW" => CubeScalarUDFKind::Now,
            "unix_timestamp" | "UNIX_TIMESTAMP" => CubeScalarUDFKind::UnixTimestamp,
            "date_add" | "DATE_ADD" => CubeScalarUDFKind::DateAdd,
            "date_sub" | "DATE_SUB" => CubeScalarUDFKind::DateSub,
            "date_bin" | "DATE_BIN" => CubeScalarUDFKind::DateBin,
            _ => return self.session_state.scalar_functions().get(name).cloned(),
        };
        return Some(scalar_udf_by_kind(kind));
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        // TODO upgrade DF
        // HyperLogLog.
        // TODO: case-insensitive names.
        // let kind = match name {
        //     "merge" | "MERGE" => CubeAggregateUDFKind::MergeHll,
        //     _ => return None,
        // };
        self.session_state.aggregate_functions().get(name).cloned() //TODO Some(aggregate_udf_by_kind(kind));
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.session_state.window_functions().get(name).cloned()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.config_options
    }

    fn udf_names(&self) -> Vec<String> {
        let mut res = vec![
            "date_add".to_string(),
            "date_sub".to_string(),
            "date_bin".to_string(),
        ];
        res.extend(self.session_state.scalar_functions().keys().cloned());
        res
    }

    fn udaf_names(&self) -> Vec<String> {
        let mut res = vec!["merge".to_string()];
        res.extend(self.session_state.aggregate_functions().keys().cloned());
        res
    }

    fn udwf_names(&self) -> Vec<String> {
        self.session_state
            .window_functions()
            .keys()
            .cloned()
            .collect()
    }
}

#[derive(Clone, Debug)]
pub enum InfoSchemaTable {
    Columns,
    Tables,
    Schemata,
    SystemJobs,
    SystemTables,
    SystemIndexes,
    SystemPartitions,
    SystemChunks,
    SystemQueue,
    SystemQueueResults,
    SystemReplayHandles,
    SystemCache,
    SystemSnapshots,
    CachestoreRocksDBProperties,
    MetastoreRocksDBProperties,
}

pub struct InfoSchemaTableDefContext {
    meta_store: Arc<dyn MetaStore>,
    cache_store: Arc<dyn CacheStore>,
}

#[async_trait]
pub trait InfoSchemaTableDef {
    type T: Send + Sync;

    async fn rows(
        &self,
        ctx: InfoSchemaTableDefContext,
        limit: Option<usize>,
    ) -> Result<Arc<Vec<Self::T>>, CubeError>;

    fn columns(&self) -> Vec<Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>>;

    fn schema(&self) -> Vec<Field>;
}

#[async_trait]
pub trait BaseInfoSchemaTableDef {
    fn schema_ref(&self) -> SchemaRef;

    async fn scan(
        &self,
        ctx: InfoSchemaTableDefContext,
        limit: Option<usize>,
    ) -> Result<RecordBatch, CubeError>;
}

#[macro_export]
macro_rules! base_info_schema_table_def {
    ($table: ty) => {
        #[async_trait]
        impl crate::queryplanner::BaseInfoSchemaTableDef for $table {
            fn schema_ref(&self) -> datafusion::arrow::datatypes::SchemaRef {
                Arc::new(datafusion::arrow::datatypes::Schema::new(self.schema()))
            }

            async fn scan(
                &self,
                ctx: crate::queryplanner::InfoSchemaTableDefContext,
                limit: Option<usize>,
            ) -> Result<datafusion::arrow::record_batch::RecordBatch, crate::CubeError> {
                let rows = self.rows(ctx, limit).await?;
                let schema = self.schema_ref();
                let columns = self.columns();
                let columns = columns
                    .into_iter()
                    .map(|c| c(rows.clone()))
                    .collect::<Vec<_>>();
                Ok(datafusion::arrow::record_batch::RecordBatch::try_new(
                    schema, columns,
                )?)
            }
        }
    };
}

impl InfoSchemaTable {
    fn table_def(&self) -> Box<dyn BaseInfoSchemaTableDef + Send + Sync> {
        match self {
            InfoSchemaTable::Columns => Box::new(ColumnsInfoSchemaTableDef),
            InfoSchemaTable::Tables => Box::new(TablesInfoSchemaTableDef),
            InfoSchemaTable::Schemata => Box::new(SchemataInfoSchemaTableDef),
            InfoSchemaTable::SystemTables => Box::new(SystemTablesTableDef),
            InfoSchemaTable::SystemIndexes => Box::new(SystemIndexesTableDef),
            InfoSchemaTable::SystemChunks => Box::new(SystemChunksTableDef),
            InfoSchemaTable::SystemQueue => Box::new(SystemQueueTableDef),
            InfoSchemaTable::SystemQueueResults => Box::new(SystemQueueResultsTableDef),
            InfoSchemaTable::SystemReplayHandles => Box::new(SystemReplayHandlesTableDef),
            InfoSchemaTable::SystemPartitions => Box::new(SystemPartitionsTableDef),
            InfoSchemaTable::SystemJobs => Box::new(SystemJobsTableDef),
            InfoSchemaTable::SystemCache => Box::new(SystemCacheTableDef),
            InfoSchemaTable::SystemSnapshots => Box::new(SystemSnapshotsTableDef),
            InfoSchemaTable::CachestoreRocksDBProperties => {
                Box::new(RocksDBPropertiesTableDef::new_cachestore())
            }
            InfoSchemaTable::MetastoreRocksDBProperties => {
                Box::new(RocksDBPropertiesTableDef::new_metastore())
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.table_def().schema_ref()
    }

    async fn scan(
        &self,
        ctx: InfoSchemaTableDefContext,
        limit: Option<usize>,
    ) -> Result<RecordBatch, CubeError> {
        self.table_def().scan(ctx, limit).await
    }
}

pub struct InfoSchemaTableProvider {
    meta_store: Arc<dyn MetaStore>,
    cache_store: Arc<dyn CacheStore>,
    table: InfoSchemaTable,
}

impl InfoSchemaTableProvider {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        cache_store: Arc<dyn CacheStore>,
        table: InfoSchemaTable,
    ) -> Self {
        Self {
            meta_store,
            cache_store,
            table,
        }
    }
}

impl Debug for InfoSchemaTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "InfoSchemaTableProvider")
    }
}

#[async_trait]
impl TableProvider for InfoSchemaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = project_schema(&self.schema(), projection.cloned().as_deref());
        let exec = InfoSchemaTableExec {
            meta_store: self.meta_store.clone(),
            cache_store: self.cache_store.clone(),
            table: self.table.clone(),
            projection: projection.cloned(),
            projected_schema: schema.clone(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                ExecutionMode::Bounded,
            ),
        };
        Ok(Arc::new(exec))
    }
}

fn project_schema(s: &Schema, projection: Option<&[usize]>) -> SchemaRef {
    let projection = match projection {
        None => return Arc::new(s.clone()),
        Some(p) => p,
    };
    let mut fields = Vec::with_capacity(projection.len());
    for &i in projection {
        fields.push(s.field(i).clone())
    }
    Arc::new(Schema::new(fields))
}

#[derive(Clone)]
pub struct InfoSchemaTableExec {
    meta_store: Arc<dyn MetaStore>,
    cache_store: Arc<dyn CacheStore>,
    table: InfoSchemaTable,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl fmt::Debug for InfoSchemaTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{:?}", self.table))
    }
}

impl DisplayAs for InfoSchemaTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "InfoSchemaTableExec")
    }
}

#[async_trait]
impl ExecutionPlan for InfoSchemaTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let table_def = InfoSchemaTableDefContext {
            meta_store: self.meta_store.clone(),
            cache_store: self.cache_store.clone(),
        };
        let table = self.table.clone();
        let limit = self.limit.clone();
        let batch = async move {
            table
                .scan(table_def, limit)
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        };

        let stream = futures::stream::once(batch);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            stream,
        )))
    }

    fn name(&self) -> &str {
        "InfoSchemaTableExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeTableLogical {
    pub table: TablePath,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for CubeTableLogical {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        panic!("scan has been called on CubeTableLogical: serialized plan wasn't preprocessed for select");
    }
    //
    // fn supports_filter_pushdown(
    //     &self,
    //     _filter: &Expr,
    // ) -> Result<TableProviderFilterPushDown, DataFusionError> {
    //     return Ok(TableProviderFilterPushDown::Inexact);
    // }
}

fn compute_workers(
    config: &dyn ConfigObj,
    p: &LogicalPlan,
    tree: &HashMap<u64, MultiPartition>,
) -> Result<Vec<String>, CubeError> {
    struct Visitor<'a> {
        config: &'a dyn ConfigObj,
        tree: &'a HashMap<u64, MultiPartition>,
        workers: Vec<String>,
    }
    impl<'a> TreeNodeVisitor<'a> for Visitor<'a> {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError> {
            match plan {
                LogicalPlan::Extension(Extension { node }) => {
                    let snapshots = if let Some(cs) =
                        node.as_any().downcast_ref::<ClusterSendNode>()
                    {
                        &cs.snapshots
                    } else if let Some(cs) = node.as_any().downcast_ref::<ClusterAggregateTopK>() {
                        &cs.snapshots
                    } else {
                        return Ok(TreeNodeRecursion::Continue);
                    };

                    let workers = ClusterSendExec::distribute_to_workers(
                        self.config,
                        snapshots.as_slice(),
                        self.tree,
                    )?;
                    self.workers = workers.into_iter().map(|w| w.0).collect();
                    Ok(TreeNodeRecursion::Stop)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        }
    }

    let mut v = Visitor {
        config,
        tree,
        workers: Vec::new(),
    };
    match p.visit(&mut v) {
        Ok(TreeNodeRecursion::Stop) => Ok(v.workers),
        Ok(TreeNodeRecursion::Continue) | Ok(TreeNodeRecursion::Jump) => Err(CubeError::internal(
            "no cluster send node found in plan".to_string(),
        )),
        Err(e) => Err(CubeError::internal(e.to_string())),
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::queryplanner::serialized_plan::SerializedPlan;
    use crate::sql::parser::{CubeStoreParser, Statement};

    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion::sql::planner::SqlToRel;
    use pretty_assertions::assert_eq;

    fn initial_plan(s: &str, ctx: MetaStoreSchemaProvider) -> LogicalPlan {
        let statement = match CubeStoreParser::new(s).unwrap().parse_statement().unwrap() {
            Statement::Statement(s) => s,
            other => panic!("not a statement, actual {:?}", other),
        };

        let plan = SqlToRel::new(&ctx)
            .statement_to_plan(DFStatement::Statement(Box::new(statement)))
            .unwrap();
        SessionContext::new().state().optimize(&plan).unwrap()
    }

    fn get_test_execution_ctx() -> MetaStoreSchemaProvider {
        MetaStoreSchemaProvider::new(
            Arc::new(vec![]),
            Arc::new(test_utils::MetaStoreMock {}),
            Arc::new(test_utils::CacheStoreMock {}),
            &vec![],
            Arc::new(SqlResultCache::new(1 << 20, None, 10000)),
            Arc::new(SessionContext::new().state()),
        )
    }

    #[tokio::test]
    pub async fn test_is_data_select_query() {
        let plan = initial_plan(
            "SELECT * FROM information_schema.columns",
            get_test_execution_ctx(),
        );
        assert_eq!(SerializedPlan::is_data_select_query(&plan), false);

        let plan = initial_plan(
            "SELECT * FROM information_schema.columns as r",
            get_test_execution_ctx(),
        );
        assert_eq!(SerializedPlan::is_data_select_query(&plan), false);

        let plan = initial_plan("select * from system.query_cache", get_test_execution_ctx());
        assert_eq!(SerializedPlan::is_data_select_query(&plan), false);

        let plan = initial_plan("SELECT * FROM system.cache", get_test_execution_ctx());
        assert_eq!(SerializedPlan::is_data_select_query(&plan), false);

        let plan = initial_plan("SELECT NOW()", get_test_execution_ctx());
        assert_eq!(SerializedPlan::is_data_select_query(&plan), false);
    }
}
