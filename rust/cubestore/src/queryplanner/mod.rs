pub mod hll;
mod optimizations;
mod partition_filter;
mod planning;
pub mod pretty_printers;
pub mod query_executor;
pub mod serialized_plan;
pub mod udfs;

use crate::config::injection::DIService;
use crate::metastore::table::TablePath;
use crate::metastore::{MetaStore, MetaStoreTable};
use crate::queryplanner::planning::choose_index;
use crate::queryplanner::query_executor::batch_to_dataframe;
use crate::queryplanner::serialized_plan::SerializedPlan;
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{scalar_udf_by_kind, CubeAggregateUDFKind, CubeScalarUDFKind};
use crate::store::DataFrame;
use crate::CubeError;
use arrow::array::StringArray;
use arrow::datatypes::Field;
use arrow::{array::Array, datatypes::Schema, datatypes::SchemaRef};
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use async_trait::async_trait;
use core::fmt;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, ToDFSchema};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{collect, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::{datasource::TableProvider, prelude::ExecutionContext};
use log::{debug, trace};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use smallvec::alloc::fmt::Formatter;
use std::any::Any;
use std::collections::HashMap;
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
}

crate::di_service!(QueryPlannerImpl, [QueryPlanner]);

pub enum QueryPlan {
    Meta(LogicalPlan),
    Select(SerializedPlan),
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
            let (logical_plan, index_snapshots) =
                choose_index(&logical_plan, &self.meta_store.as_ref()).await?;
            QueryPlan::Select(SerializedPlan::try_new(logical_plan, index_snapshots).await?)
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
            tokio::task::spawn_blocking(move || plan_ctx.create_physical_plan(&plan_to_move))
                .await??;

        let execution_time = SystemTime::now();
        let results = collect(physical_plan).await?;
        debug!(
            "Meta query data processing time: {:?}",
            execution_time.elapsed()?
        );
        let data_frame =
            tokio::task::spawn_blocking(move || batch_to_dataframe(&results)).await??;
        Ok(data_frame)
    }
}

impl QueryPlannerImpl {
    pub fn new(meta_store: Arc<dyn MetaStore>) -> Arc<QueryPlannerImpl> {
        Arc::new(QueryPlannerImpl { meta_store })
    }
}

impl QueryPlannerImpl {
    async fn execution_context(&self) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::new()))
    }
}

struct MetaStoreSchemaProvider {
    tables: HashMap<String, TablePath>,
    meta_store: Arc<dyn MetaStore>,
}

impl MetaStoreSchemaProvider {
    pub fn new(tables: Vec<TablePath>, meta_store: Arc<dyn MetaStore>) -> Self {
        Self {
            tables: tables.into_iter().map(|t| (t.table_name(), t)).collect(),
            meta_store,
        }
    }
}

impl ContextProvider for MetaStoreSchemaProvider {
    fn get_table_provider(&self, name: &str) -> Option<Arc<dyn TableProvider + Send + Sync>> {
        let res = self
            .tables
            .get(name)
            .map(|table| -> Arc<dyn TableProvider + Send + Sync> {
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
        res.or_else(|| match name {
            "information_schema.tables" => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::Tables,
            ))),
            "information_schema.schemata" => Some(Arc::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::Schemata,
            ))),
            _ => None,
        })
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        let kind = match name {
            "cardinality" | "CARDINALITY" => CubeScalarUDFKind::HllCardinality,
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
