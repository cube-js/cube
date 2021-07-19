pub mod hll;
mod optimizations;
mod partition_filter;
mod planning;
pub mod pretty_printers;
pub mod query_executor;
pub mod serialized_plan;
mod topk;
pub use topk::MIN_TOPK_STREAM_ROWS;
mod coalesce;
mod now;
pub mod udfs;

use crate::config::injection::DIService;
use crate::config::ConfigObj;
use crate::metastore::table::{Table, TablePath};
use crate::metastore::{IdRow, MetaStore, MetaStoreTable};
use crate::queryplanner::now::MaterializeNow;
use crate::queryplanner::planning::{choose_index_ext, ClusterSendNode};
use crate::queryplanner::query_executor::{batch_to_dataframe, ClusterSendExec};
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::queryplanner::topk::ClusterAggregateTopK;
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{scalar_udf_by_kind, CubeAggregateUDFKind, CubeScalarUDFKind};
use crate::store::DataFrame;
use crate::{app_metrics, metastore, CubeError};
use arrow::array::StringArray;
use arrow::datatypes::Field;
use arrow::{array::Array, datatypes::Schema, datatypes::SchemaRef};
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use async_trait::async_trait;
use core::fmt;
use datafusion::catalog::TableReference;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, PlanVisitor, ToDFSchema};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{collect, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::prelude::ExecutionConfig;
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::{cube_ext, datasource::TableProvider, prelude::ExecutionContext};
use itertools::Itertools;
use log::{debug, trace};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use smallvec::alloc::fmt::Formatter;
use std::any::Any;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::SystemTime;

#[automock]
#[async_trait]
pub trait QueryPlanner: DIService + Send + Sync {
    async fn logical_plan(&self, statement: Statement) -> Result<QueryPlan, CubeError>;
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
    Select(SerializedPlan, /*partitions*/ Vec<Vec<u64>>),
}

#[async_trait]
impl QueryPlanner for QueryPlannerImpl {
    async fn logical_plan(&self, statement: Statement) -> Result<QueryPlan, CubeError> {
        let ctx = self.execution_context().await?;

        let schema_provider = MetaStoreSchemaProvider::new(
            self.meta_store.get_tables_with_path().await?,
            self.meta_store.clone(),
        );

        let query_planner = SqlToRel::new(&schema_provider);
        let mut logical_plan = query_planner.statement_to_plan(&statement)?;

        logical_plan = ctx.optimize(&logical_plan)?;
        trace!("Logical Plan: {:#?}", &logical_plan);

        let plan = if SerializedPlan::is_data_select_query(&logical_plan) {
            let (logical_plan, index_snapshots) = choose_index_ext(
                &logical_plan,
                &self.meta_store.as_ref(),
                self.config.enable_topk(),
            )
            .await?;
            let partitions = extract_partitions(&logical_plan)?;
            QueryPlan::Select(
                SerializedPlan::try_new(logical_plan, index_snapshots).await?,
                partitions,
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
    pub fn new(tables: Arc<Vec<TablePath>>, meta_store: Arc<dyn MetaStore>) -> Self {
        let by_name = tables.iter().map(|t| TableKey(t)).collect();
        Self {
            _data: tables,
            by_name,
            meta_store,
        }
    }
}

impl ContextProvider for MetaStoreSchemaProvider {
    fn get_table_provider(&self, name: TableReference) -> Option<Arc<dyn TableProvider>> {
        let (schema, table) = match name {
            TableReference::Partial { schema, table } => (schema, table),
            TableReference::Bare { .. } | TableReference::Full { .. } => return None,
        };
        // Mock table path for hash set access.
        let name = TablePath {
            table: IdRow::new(
                u64::MAX,
                Table::new(table.to_string(), u64::MAX, Vec::new(), None, None, false),
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
}

impl InfoSchemaTable {
    fn schema(&self) -> SchemaRef {
        match self {
            InfoSchemaTable::Tables => Arc::new(Schema::new(vec![
                Field::new("table_schema", DataType::Utf8, false),
                Field::new("table_name", DataType::Utf8, false),
            ])),
            InfoSchemaTable::Schemata => Arc::new(Schema::new(vec![Field::new(
                "schema_name",
                DataType::Utf8,
                false,
            )])),
        }
    }

    async fn scan(&self, meta_store: Arc<dyn MetaStore>) -> Result<RecordBatch, CubeError> {
        match self {
            InfoSchemaTable::Tables => {
                let tables = meta_store.get_tables_with_path().await?;
                let schema = self.schema();
                let columns: Vec<Arc<dyn Array>> = vec![
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| row.schema.get_row().get_name().as_str())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        tables
                            .iter()
                            .map(|row| row.table.get_row().get_table_name().as_str())
                            .collect::<Vec<_>>(),
                    )),
                ];
                Ok(RecordBatch::try_new(schema, columns)?)
            }
            InfoSchemaTable::Schemata => {
                let schemas = meta_store.schemas_table().all_rows().await?;
                let schema = self.schema();
                let columns: Vec<Arc<dyn Array>> = vec![Arc::new(StringArray::from(
                    schemas
                        .iter()
                        .map(|row| row.get_row().get_name().as_str())
                        .collect::<Vec<_>>(),
                ))];
                Ok(RecordBatch::try_new(schema, columns)?)
            }
        }
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
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let exec = InfoSchemaTableExec {
            meta_store: self.meta_store.clone(),
            table: self.table.clone(),
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

#[derive(Clone)]
pub struct InfoSchemaTableExec {
    meta_store: Arc<dyn MetaStore>,
    table: InfoSchemaTable,
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

    fn schema(&self) -> DFSchemaRef {
        self.table.schema().to_dfschema_ref().unwrap()
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
        let schema = batch.schema();
        let mem_exec = MemoryExec::try_new(&vec![vec![batch]], schema, None)?;
        mem_exec.execute(partition).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeTableLogical {
    table: TablePath,
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

fn extract_partitions(p: &LogicalPlan) -> Result<Vec<Vec<u64>>, CubeError> {
    struct Visitor {
        snapshots: Vec<Vec<u64>>,
    }
    impl PlanVisitor for Visitor {
        type Error = ();

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, ()> {
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

                    self.snapshots = ClusterSendExec::logical_partitions(&snapshots)
                        .into_iter()
                        .map(|ps| ps.iter().map(|p| p.get_id()).collect_vec())
                        .collect_vec();
                    Ok(false)
                }
                _ => Ok(true),
            }
        }
    }

    let mut v = Visitor {
        snapshots: Vec::new(),
    };
    match p.accept(&mut v) {
        Ok(false) => Ok(v.snapshots),
        Ok(true) => Err(CubeError::internal(
            "no cluster send node found in plan".to_string(),
        )),
        Err(_) => panic!("unexpected return value"),
    }
}
