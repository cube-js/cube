pub mod hll;
mod optimizations;
pub mod panic;
mod partition_filter;
mod planning;
pub use planning::PlanningMeta;
pub mod pretty_printers;
pub mod query_executor;
pub mod serialized_plan;
mod tail_limit;
mod topk;
pub use topk::MIN_TOPK_STREAM_ROWS;
mod coalesce;
mod filter_by_key_range;
pub mod info_schema;
pub mod now;
pub mod udfs;

use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::multi_index::MultiPartition;
use crate::metastore::table::{Table, TablePath};
use crate::metastore::{IdRow, MetaStore};
use crate::queryplanner::info_schema::info_schema_schemata::SchemataInfoSchemaTableDef;
use crate::queryplanner::info_schema::info_schema_tables::TablesInfoSchemaTableDef;
use crate::queryplanner::info_schema::system_chunks::SystemChunksTableDef;
use crate::queryplanner::info_schema::system_indexes::SystemIndexesTableDef;
use crate::queryplanner::info_schema::system_jobs::SystemJobsTableDef;
use crate::queryplanner::info_schema::system_partitions::SystemPartitionsTableDef;
use crate::queryplanner::info_schema::system_replay_handles::SystemReplayHandlesTableDef;
use crate::queryplanner::info_schema::system_tables::SystemTablesTableDef;
use crate::queryplanner::now::MaterializeNow;
use crate::queryplanner::planning::{choose_index_ext, ClusterSendNode};
use crate::queryplanner::query_executor::{
    batch_to_dataframe, ClusterSendExec, InlineTableProvider,
};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::queryplanner::topk::ClusterAggregateTopK;
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{scalar_udf_by_kind, CubeAggregateUDFKind, CubeScalarUDFKind};
use crate::sql::InlineTables;
use crate::store::DataFrame;
use crate::{app_metrics, metastore, CubeError};
use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use arrow::{datatypes::Schema, datatypes::SchemaRef};
use async_trait::async_trait;
use core::fmt;
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
    ) -> Result<QueryPlan, CubeError>;
    async fn execute_meta_plan(&self, plan: LogicalPlan) -> Result<DataFrame, CubeError>;
}

crate::di_service!(MockQueryPlanner, [QueryPlanner]);

pub struct QueryPlannerImpl {
    meta_store: Arc<dyn MetaStore>,
    config: Arc<dyn ConfigObj>,
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
    ) -> Result<QueryPlan, CubeError> {
        let ctx = self.execution_context().await?;

        let schema_provider = MetaStoreSchemaProvider::new(
            self.meta_store.get_tables_with_path(false).await?,
            self.meta_store.clone(),
            inline_tables,
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
            QueryPlan::Select(SerializedPlan::try_new(logical_plan, meta).await?, workers)
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
        let data_frame = cube_ext::spawn_blocking(move || batch_to_dataframe(&results)).await??;
        Ok(data_frame)
    }
}

impl QueryPlannerImpl {
    pub fn new(
        meta_store: Arc<dyn MetaStore>,
        config: Arc<dyn ConfigObj>,
    ) -> Arc<QueryPlannerImpl> {
        Arc::new(QueryPlannerImpl { meta_store, config })
    }
}

impl QueryPlannerImpl {
    async fn execution_context(&self) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::with_config(
            ExecutionConfig::new().add_optimizer_rule(Arc::new(MaterializeNow {})),
        )))
    }
}

struct MetaStoreSchemaProvider {
    /// Keeps the data used by [by_name] alive.
    _data: Arc<Vec<TablePath>>,
    by_name: HashSet<TableKey>,
    meta_store: Arc<dyn MetaStore>,
    inline_tables: InlineTables,
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
        inline_tables: &InlineTables,
    ) -> Self {
        let by_name = tables.iter().map(|t| TableKey(t)).collect();
        Self {
            _data: tables,
            by_name,
            meta_store,
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
                    Vec::new(),
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
            ("information_schema", "tables") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::Tables,
            ))),
            ("information_schema", "schemata") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::Schemata,
            ))),
            ("system", "tables") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::SystemTables,
            ))),
            ("system", "indexes") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::SystemIndexes,
            ))),
            ("system", "partitions") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::SystemPartitions,
            ))),
            ("system", "chunks") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::SystemChunks,
            ))),
            ("system", "replay_handles") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::SystemReplayHandles,
            ))),
            ("system", "jobs") => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::SystemJobs,
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
    Tables,
    Schemata,
    SystemJobs,
    SystemTables,
    SystemIndexes,
    SystemPartitions,
    SystemChunks,
    SystemReplayHandles,
}

#[async_trait]
pub trait InfoSchemaTableDef {
    type T: Send + Sync;

    async fn rows(&self, meta_store: Arc<dyn MetaStore>) -> Result<Arc<Vec<Self::T>>, CubeError>;

    fn columns(&self) -> Vec<(Field, Box<dyn Fn(Arc<Vec<Self::T>>) -> ArrayRef>)>;
}

#[async_trait]
pub trait BaseInfoSchemaTableDef {
    fn schema(&self) -> SchemaRef;

    async fn scan(&self, meta_store: Arc<dyn MetaStore>) -> Result<RecordBatch, CubeError>;
}

#[macro_export]
macro_rules! base_info_schema_table_def {
    ($table: ty) => {
        #[async_trait]
        impl crate::queryplanner::BaseInfoSchemaTableDef for $table {
            fn schema(&self) -> arrow::datatypes::SchemaRef {
                Arc::new(arrow::datatypes::Schema::new(
                    self.columns()
                        .into_iter()
                        .map(|(f, _)| f)
                        .collect::<Vec<_>>(),
                ))
            }

            async fn scan(
                &self,
                meta_store: Arc<dyn crate::metastore::MetaStore>,
            ) -> Result<arrow::record_batch::RecordBatch, crate::CubeError> {
                let rows = self.rows(meta_store).await?;
                let schema = self.schema();
                let columns = self.columns();
                let columns = columns
                    .into_iter()
                    .map(|(_, c)| c(rows.clone()))
                    .collect::<Vec<_>>();
                Ok(arrow::record_batch::RecordBatch::try_new(schema, columns)?)
            }
        }
    };
}

impl InfoSchemaTable {
    fn table_def(&self) -> Box<dyn BaseInfoSchemaTableDef + Send + Sync> {
        match self {
            InfoSchemaTable::Tables => Box::new(TablesInfoSchemaTableDef),
            InfoSchemaTable::Schemata => Box::new(SchemataInfoSchemaTableDef),
            InfoSchemaTable::SystemTables => Box::new(SystemTablesTableDef),
            InfoSchemaTable::SystemIndexes => Box::new(SystemIndexesTableDef),
            InfoSchemaTable::SystemChunks => Box::new(SystemChunksTableDef),
            InfoSchemaTable::SystemReplayHandles => Box::new(SystemReplayHandlesTableDef),
            InfoSchemaTable::SystemPartitions => Box::new(SystemPartitionsTableDef),
            InfoSchemaTable::SystemJobs => Box::new(SystemJobsTableDef),
        }
    }

    fn schema(&self) -> SchemaRef {
        self.table_def().schema()
    }

    async fn scan(&self, meta_store: Arc<dyn MetaStore>) -> Result<RecordBatch, CubeError> {
        self.table_def().scan(meta_store).await
    }
}

pub struct InfoSchemaTableProvider {
    meta_store: Arc<dyn MetaStore>,
    table: InfoSchemaTable,
}

impl InfoSchemaTableProvider {
    fn new(meta_store: Arc<dyn MetaStore>, table: InfoSchemaTable) -> InfoSchemaTableProvider {
        InfoSchemaTableProvider { meta_store, table }
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = InfoSchemaTableExec {
            meta_store: self.meta_store.clone(),
            table: self.table.clone(),
            projection: projection.clone(),
            projected_schema: project_schema(&self.schema(), projection.as_deref()),
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
    table: InfoSchemaTable,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
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
        let batch = self.table.scan(self.meta_store.clone()).await?;
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
                    let snapshots;
                    if let Some(cs) = node.as_any().downcast_ref::<ClusterSendNode>() {
                        snapshots = &cs.snapshots;
                    } else if let Some(cs) = node.as_any().downcast_ref::<ClusterAggregateTopK>() {
                        snapshots = &cs.snapshots;
                    } else {
                        return Ok(true);
                    }
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
