pub mod hll;
mod optimizations;
pub mod panic;
mod partition_filter;
mod planning;
use datafusion::physical_plan::parquet::MetadataCacheFactory;
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
pub mod now;
pub mod providers;
#[cfg(test)]
mod test_utils;
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
use crate::queryplanner::now::MaterializeNow;
use crate::queryplanner::planning::{choose_index_ext, ClusterSendNode};
use crate::queryplanner::projection_above_limit::ProjectionAboveLimit;
use crate::queryplanner::query_executor::{
    batches_to_dataframe, ClusterSendExec, InlineTableProvider,
};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::queryplanner::topk::ClusterAggregateTopK;
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{scalar_udf_by_kind, CubeAggregateUDFKind, CubeScalarUDFKind};

use crate::sql::cache::SqlResultCache;
use crate::sql::InlineTables;
use crate::store::DataFrame;
use crate::{app_metrics, metastore, CubeError};
use async_trait::async_trait;
use core::fmt;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::Field;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{datatypes::Schema, datatypes::SchemaRef};
use datafusion::catalog::TableReference;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{Expr, LogicalPlan, PlanVisitor};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{collect, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::prelude::ExecutionConfig;
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::{cube_ext, datasource::TableProvider, prelude::ExecutionContext};
use log::{debug, trace};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use smallvec::alloc::fmt::Formatter;
use std::any::Any;
use std::collections::{HashMap, HashSet};
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

        let schema_provider = MetaStoreSchemaProvider::new(
            self.meta_store.get_tables_with_path(false).await?,
            self.meta_store.clone(),
            self.cache_store.clone(),
            inline_tables,
            self.cache.clone(),
        );

        let query_planner = SqlToRel::new(&schema_provider);
        let mut logical_plan = query_planner.statement_to_plan(&statement)?;

        logical_plan = ctx.optimize(&logical_plan)?;
        trace!("Logical Plan: {:#?}", &logical_plan);

        let plan = if SerializedPlan::is_data_select_query(&logical_plan) {
            let (logical_plan, meta) = choose_index_ext(
                &logical_plan,
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
        let physical_plan =
            cube_ext::spawn_blocking(move || plan_ctx.create_physical_plan(&plan_to_move))
                .await??;

        let execution_time = SystemTime::now();
        let results = collect(physical_plan).await?;
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
    async fn execution_context(&self) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::with_config(
            ExecutionConfig::new()
                .with_metadata_cache_factory(self.metadata_cache_factory.clone())
                .add_optimizer_rule(Arc::new(MaterializeNow {}))
                .add_optimizer_rule(Arc::new(FlattenUnion {}))
                .add_optimizer_rule(Arc::new(ProjectionAboveLimit {})),
        )))
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
}

/// Points into [MetaStoreSchemaProvider::data], never null.
struct TableKey(*const TablePath);
unsafe impl Send for TableKey {}
unsafe impl Sync for TableKey {}

impl TableKey {
    fn qual_name(&self) -> (&str, &str) {
        let s = unsafe { &*self.0 };
        (
            s.schema.get_row().get_name().as_str(),
            s.table.get_row().get_table_name().as_str(),
        )
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
    ) -> Self {
        let by_name = tables.iter().map(|t| TableKey(t)).collect();
        Self {
            _data: tables,
            by_name,
            meta_store,
            cache_store,
            cache,
            inline_tables: (*inline_tables).clone(),
        }
    }
}

impl ContextProvider for MetaStoreSchemaProvider {
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
        let (schema, table) = match name {
            TableReference::Partial { schema, table } => (schema, table),
            TableReference::Bare { table } => {
                let table = self
                    .inline_tables
                    .iter()
                    .find(|inline_table| inline_table.name == table)?;
                return Some(Arc::new(InlineTableProvider::new(
                    table.id,
                    table.data.clone(),
                    Vec::new(),
                )));
            }
            TableReference::Full { .. } => return None,
        };

        // Mock table path for hash set access.
        let name = TablePath {
            table: IdRow::new(
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
            schema: Arc::new(IdRow::new(0, metastore::Schema::new(schema.to_string()))),
        };

        let res = self
            .by_name
            .get(&TableKey(&name))
            .map(|table| -> Arc<dyn TableProvider> {
                let table = unsafe { &*table.0 };
                let schema = Arc::new(Schema::new(
                    table
                        .table
                        .get_row()
                        .get_columns()
                        .iter()
                        .map(|c| c.clone().into())
                        .collect::<Vec<_>>(),
                ));
                Arc::new(CubeTableLogical {
                    table: table.clone(),
                    schema,
                })
            });
        res.or_else(|| match (schema, table) {
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
            ("metastore", "rocksdb_properties") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                self.cache_store.clone(),
                InfoSchemaTable::MetastoreRocksDBProperties,
            ))),
            ("cachestore", "rocksdb_properties") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                self.cache_store.clone(),
                InfoSchemaTable::CachestoreRocksDBProperties,
            ))),
            _ => None,
        })
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        let kind = match name {
            "cardinality" | "CARDINALITY" => CubeScalarUDFKind::HllCardinality,
            "coalesce" | "COALESCE" => CubeScalarUDFKind::Coalesce,
            "now" | "NOW" => CubeScalarUDFKind::Now,
            "unix_timestamp" | "UNIX_TIMESTAMP" => CubeScalarUDFKind::UnixTimestamp,
            "date_add" | "DATE_ADD" => CubeScalarUDFKind::DateAdd,
            "date_sub" | "DATE_SUB" => CubeScalarUDFKind::DateSub,
            "date_bin" | "DATE_BIN" => CubeScalarUDFKind::DateBin,
            _ => return None,
        };
        return Some(Arc::new(scalar_udf_by_kind(kind).descriptor()));
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        // HyperLogLog.
        // TODO: case-insensitive names.
        let kind = match name {
            "merge" | "MERGE" => CubeAggregateUDFKind::MergeHll,
            _ => return None,
        };
        return Some(Arc::new(aggregate_udf_by_kind(kind).descriptor()));
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

impl TableProvider for InfoSchemaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = InfoSchemaTableExec {
            meta_store: self.meta_store.clone(),
            cache_store: self.cache_store.clone(),
            table: self.table.clone(),
            projection: projection.clone(),
            projected_schema: project_schema(&self.schema(), projection.as_deref()),
            limit,
        };
        Ok(Arc::new(exec))
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
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
}

impl fmt::Debug for InfoSchemaTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!("{:?}", self.table))
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

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        &self,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(self.clone()))
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let table_def = InfoSchemaTableDefContext {
            meta_store: self.meta_store.clone(),
            cache_store: self.cache_store.clone(),
        };
        let batch = self.table.scan(table_def, self.limit).await?;
        let mem_exec =
            MemoryExec::try_new(&vec![vec![batch]], self.schema(), self.projection.clone())?;
        mem_exec.execute(partition).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeTableLogical {
    pub table: TablePath,
    schema: SchemaRef,
}

impl TableProvider for CubeTableLogical {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        panic!("scan has been called on CubeTableLogical: serialized plan wasn't preprocessed for select");
    }

    fn statistics(&self) -> Statistics {
        // TODO
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        return Ok(TableProviderFilterPushDown::Inexact);
    }
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
    impl<'a> PlanVisitor for Visitor<'a> {
        type Error = CubeError;

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, CubeError> {
            match plan {
                LogicalPlan::Extension { node } => {
                    let snapshots = if let Some(cs) =
                        node.as_any().downcast_ref::<ClusterSendNode>()
                    {
                        &cs.snapshots
                    } else if let Some(cs) = node.as_any().downcast_ref::<ClusterAggregateTopK>() {
                        &cs.snapshots
                    } else {
                        return Ok(true);
                    };

                    let workers = ClusterSendExec::distribute_to_workers(
                        self.config,
                        snapshots.as_slice(),
                        self.tree,
                    )?;
                    self.workers = workers.into_iter().map(|w| w.0).collect();
                    Ok(false)
                }
                _ => Ok(true),
            }
        }
    }

    let mut v = Visitor {
        config,
        tree,
        workers: Vec::new(),
    };
    match p.accept(&mut v) {
        Ok(false) => Ok(v.workers),
        Ok(true) => Err(CubeError::internal(
            "no cluster send node found in plan".to_string(),
        )),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::queryplanner::serialized_plan::SerializedPlan;
    use crate::sql::parser::{CubeStoreParser, Statement};

    use datafusion::execution::context::ExecutionContext;
    use datafusion::logical_plan::LogicalPlan;
    use datafusion::sql::parser::Statement as DFStatement;
    use datafusion::sql::planner::SqlToRel;
    use pretty_assertions::assert_eq;

    fn initial_plan(s: &str, ctx: MetaStoreSchemaProvider) -> LogicalPlan {
        let statement = match CubeStoreParser::new(s).unwrap().parse_statement().unwrap() {
            Statement::Statement(s) => s,
            other => panic!("not a statement, actual {:?}", other),
        };

        let plan = SqlToRel::new(&ctx)
            .statement_to_plan(&DFStatement::Statement(statement))
            .unwrap();
        ExecutionContext::new().optimize(&plan).unwrap()
    }

    fn get_test_execution_ctx() -> MetaStoreSchemaProvider {
        MetaStoreSchemaProvider::new(
            Arc::new(vec![]),
            Arc::new(test_utils::MetaStoreMock {}),
            Arc::new(test_utils::CacheStoreMock {}),
            &vec![],
            Arc::new(SqlResultCache::new(1 << 20, None, 10000)),
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
