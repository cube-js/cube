pub mod query_executor;
pub mod serialized_plan;
pub mod udfs;

use crate::metastore::table::TablePath;
use crate::metastore::{MetaStore, MetaStoreTable};
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
use datafusion::datasource::datasource::Statistics;
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{Expr, LogicalPlan};
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::{ContextProvider, SqlToRel};
use datafusion::{datasource::MemTable, datasource::TableProvider, prelude::ExecutionContext};
use log::{debug, trace};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::runtime::Handle;

#[automock]
#[async_trait]
pub trait QueryPlanner: Send + Sync {
    async fn logical_plan(&self, statement: Statement) -> Result<QueryPlan, CubeError>;
    async fn execute_meta_plan(&self, plan: LogicalPlan) -> Result<DataFrame, CubeError>;
}

pub struct QueryPlannerImpl {
    meta_store: Arc<dyn MetaStore>,
}

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
            ctx.clone(),
        );

        let query_planner = SqlToRel::new(&schema_provider);
        let mut logical_plan = query_planner.statement_to_plan(&statement)?;

        logical_plan = ctx.optimize(&logical_plan)?;

        trace!("Logical Plan: {:#?}", &logical_plan);

        let plan = if SerializedPlan::is_data_select_query(&logical_plan) {
            QueryPlan::Select(SerializedPlan::try_new(logical_plan, self.meta_store.clone()).await?)
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
        let data_frame = batch_to_dataframe(&results)?;
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
        let mut ctx = ExecutionContext::new();

        ctx.register_table(
            "information_schema.tables",
            Box::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::Tables,
            )),
        );

        ctx.register_table(
            "information_schema.schemata",
            Box::new(InfoSchemaTableProvider::new(
                self.meta_store.clone(),
                InfoSchemaTable::Schemata,
            )),
        );

        Ok(Arc::new(ctx))
    }
}

struct MetaStoreSchemaProvider {
    tables: HashMap<String, TablePath>,
    information_schema_context: Arc<ExecutionContext>,
}

impl MetaStoreSchemaProvider {
    pub fn new(tables: Vec<TablePath>, information_schema_context: Arc<ExecutionContext>) -> Self {
        Self {
            tables: tables.into_iter().map(|t| (t.table_name(), t)).collect(),
            information_schema_context,
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
        // TODO .unwrap
        res.or_else(|| {
            self.information_schema_context
                .state
                .lock()
                .unwrap()
                .get_table_provider(name)
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

    async fn mem_table(&self) -> Result<MemTable, DataFusionError> {
        let batch = self.table.scan(self.meta_store.clone()).await?;
        MemTable::try_new(batch.schema(), vec![vec![batch]])
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
        batch_size: usize,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let handle = Handle::current();
        let mem_table = handle.block_on(async move { self.mem_table().await })?;
        mem_table.scan(projection, batch_size, filters)
    }

    fn statistics(&self) -> Statistics {
        Statistics {
            num_rows: None,
            total_byte_size: None,
            column_statistics: None,
        }
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
}
