use crate::CubeError;
use crate::{
    metastore::{Column, ColumnType, MetaStore},
    store::DataFrame,
    table::Row,
    table::TableValue,
};
use arrow::{array::{Int64Array, StringArray, UInt64Array}};
use arrow::{
    array::Array, array::Int64Builder, array::StringBuilder,
    datatypes::Schema, datatypes::SchemaRef,
};
use arrow::{datatypes::DataType, record_batch::RecordBatch};
use async_trait::async_trait;
use datafusion::{error::{DataFusionError, Result as DFResult}, physical_plan::Partitioning, physical_plan::merge::MergeExec};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use datafusion::{
    datasource::MemTable, datasource::TableProvider,
    physical_plan::parquet::ParquetExec, prelude::ExecutionContext,
};
use log::debug;
use std::{any::Any, sync::{Arc}};
use crate::table::{TimestampValue};
use itertools::Itertools;
use crate::metastore::{IdRow, Index, table::Table};
use std::time::SystemTime;
use datafusion::logical_plan::LogicalPlan;
use datafusion::sql::parser::Statement;
use datafusion::sql::planner::SqlToRel;
use crate::remotefs::RemoteFs;
use crate::store::ChunkDataStore;
use mockall::automock;
use tokio::runtime::Handle;
use arrow::datatypes::{Field, TimeUnit};
use arrow::array::{TimestampMicrosecondArray, TimestampNanosecondArray, BooleanArray, Float64Array};
use bigdecimal::BigDecimal;
use std::convert::TryFrom;
use std::pin::Pin;

#[automock]
#[async_trait]
pub trait QueryPlanner: Send + Sync {
    async fn logical_plan(&self, statement: Statement) -> Result<LogicalPlan, CubeError>;
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<DataFrame, CubeError>;
}

pub struct QueryPlannerImpl {
    meta_store: Arc<dyn MetaStore>,
    remote_fs: Arc<dyn RemoteFs>,
    chunk_store: Arc<dyn ChunkDataStore>
}

pub fn arrow_to_column_type(arrow_type: DataType) -> Result<ColumnType, CubeError> {
    match arrow_type {
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnType::String),
        DataType::Timestamp(_, _) => Ok(ColumnType::Timestamp),
        DataType::Float16 | DataType::Float64 => Ok(ColumnType::Decimal),
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(ColumnType::Int),
        x => Err(CubeError::internal(format!("unsupported type {:?}", x))),
    }
}

pub fn batch_to_dataframe(batches: &Vec<RecordBatch>) -> Result<DataFrame, CubeError> {
    let mut cols = vec![];
    let mut all_rows = vec![];

    for batch in batches.iter() {
        if cols.len() == 0 {
            let schema = batch.schema().clone();
            for (i, field) in schema.fields().iter().enumerate() {
                cols.push(Column::new(
                    field.name().clone(),
                    arrow_to_column_type(field.data_type().clone())?,
                    i,
                ));
            }
        }
        if batch.num_rows() == 0 {
            continue;
        }
        let mut rows = vec![];

        for _ in 0..batch.num_rows() {
            rows.push(Row::new(Vec::with_capacity(batch.num_columns())));
        }

        for column_index in 0..batch.num_columns() {
            let array = batch.column(column_index);
            let num_rows = batch.num_rows();
            match array.data_type() {
                DataType::UInt64 => {
                    let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Int(a.value(i) as i64) });
                    }
                }
                DataType::Int64 => {
                    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Int(a.value(i) as i64) });
                    }
                }
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Decimal(BigDecimal::try_from(a.value(i) as f64)?) });
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    let a = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Timestamp(TimestampValue::new(a.value(i) * 1000 as i64)) });
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    let a = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Timestamp(TimestampValue::new(a.value(i))) });
                    }
                }
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::String(a.value(i).to_string()) });
                    }
                }
                DataType::Boolean => {
                    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Boolean(a.value(i)) });
                    }
                }
                x => panic!("Unsupported data type: {:?}", x),
            }
        }
        all_rows.append(&mut rows);
    }
    Ok(DataFrame::new(cols, all_rows))
}

pub fn dataframe_to_batch(data_frame: &DataFrame) -> Result<RecordBatch, CubeError> {
    let schema = Schema::new(
        data_frame
            .get_columns()
            .iter()
            .map(|c| c.clone().into())
            .collect::<Vec<_>>(),
    );
    let columns = data_frame.get_columns();
    let mut column_values: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
    let data = data_frame.get_rows();
    for c in columns.iter() {
        match c.get_column_type() {
            ColumnType::String => {
                let mut column = StringBuilder::new(data.len());
                for i in 0..data.len() {
                    let value = &data[i].values()[c.get_index()];
                    if let TableValue::String(v) = value {
                        column.append_value(v.as_str()).unwrap();
                    } else {
                        panic!("Unexpected value: {:?}", value);
                    }
                }
                column_values.push(Arc::new(column.finish()));
            }
            ColumnType::Int => {
                let mut column = Int64Builder::new(data.len());
                for i in 0..data.len() {
                    let value = &data[i].values()[c.get_index()];
                    if let TableValue::Int(v) = value {
                        column.append_value(*v).unwrap();
                    } else {
                        panic!("Unexpected value: {:?}", value);
                    }
                }
                column_values.push(Arc::new(column.finish()));
            }
            _ => unimplemented!(),
        }
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), column_values).unwrap())
}


/// implement our own table provider for datafusion (parquet + chunks + BRIN)
// we need this because datafusion only allows one table name per source

pub struct CubeTable {
    schema_name: String,
    table_name: String,
    schema: SchemaRef,
    meta_store: Arc<dyn MetaStore>,
    remote_fs: Arc<dyn RemoteFs>,
    chunk_store: Arc<dyn ChunkDataStore>
}

impl CubeTable {
    pub fn try_new(
        schema_name: String, table_name: String, schema: SchemaRef,
        meta_store: Arc<dyn MetaStore>,
        remote_fs: Arc<dyn RemoteFs>,
        chunk_store: Arc<dyn ChunkDataStore>
    ) -> Result<Self, CubeError> {
        Ok(Self { schema_name, table_name, schema, meta_store, remote_fs, chunk_store })
    }

    async fn async_scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let table = self.meta_store.get_table(self.schema_name.clone(), self.table_name.clone()).await?;
        let default_index = self.meta_store.get_default_index(table.get_id()).await?;
        let index = if let Some(projection_column_indices) = projection {
            let projection_columns = CubeTable::project_to_table(&table, projection_column_indices);
            let indexes = self.meta_store.get_table_indexes(table.get_id()).await?;
            if let Some((index, _)) = indexes.into_iter().filter_map(
                |i| {
                    let projected_index_positions = CubeTable::project_to_index_positions(&projection_columns, &i);
                    let score = projected_index_positions.into_iter().fold_options(0, |a, b| a + b);
                    score.map(|s| (i, s))
                }
            ).min_by_key(|(_, s)| *s) {
                index
            } else {
                default_index
            }
        } else {
            default_index
        };
        let partitions = self.meta_store.get_active_partitions_by_index_id(index.get_id()).await?;

        let mut partition_execs = Vec::<Arc<dyn ExecutionPlan>>::new();

        let schema = Arc::new(Schema::new(index.get_row().get_columns().iter().map(|c| c.clone().into()).collect::<Vec<_>>()));
        let mapped_projection = projection.as_ref()
            .map(
                |p| CubeTable::project_to_index_positions(&CubeTable::project_to_table(&table, p), &index)
                    .into_iter()
                    .map(|i| i.unwrap())
                    .collect::<Vec<_>>()
            );

        for partition in partitions {
            if !partition.get_row().is_active() {
                continue;
            }

            let mut execs = Vec::new();

            if let Some(remote_path) = partition.get_row().get_full_name(partition.get_id()) {
                let local_path = self.remote_fs.download_file(&remote_path).await?;
                let arc: Arc<dyn ExecutionPlan> = Arc::new(ParquetExec::try_new(&local_path, mapped_projection.clone(), batch_size)?);
                execs.push(arc);
            }

            // TODO look up in not repartitioned parent chunks
            let chunks = self.meta_store.get_chunks_by_partition(partition.get_id()).await?;
            for chunk in chunks {
                let local_path = self.chunk_store.download_chunk(chunk).await?;
                let node = Arc::new(ParquetExec::try_new(&local_path, mapped_projection.clone(), batch_size)?);
                execs.push(node);
            }

            partition_execs.push(Arc::new(MergeExec::new(Arc::new(CubeTableExec {
                schema: schema.clone(),
                partition_execs: execs,
            }))))
        }

        let plan = Arc::new(MergeExec::new(Arc::new(
            CubeTableExec { schema: self.schema.clone(), partition_execs }
        )));

        Ok(plan)
    }

    fn project_to_index_positions(projection_columns: &Vec<Column>, i: &IdRow<Index>) -> Vec<Option<usize>> {
        projection_columns.iter().map(
            |pc| i.get_row().get_columns().iter().find_position(|c| c.get_name() == pc.get_name()).map(|(p, _)| p)
        ).collect::<Vec<_>>()
    }

    fn project_to_table(table: &IdRow<Table>, projection_column_indices: &Vec<usize>) -> Vec<Column> {
        projection_column_indices.iter().map(|i| table.get_row().get_columns()[*i].clone()).collect::<Vec<_>>()
    }
}

#[derive(Debug)]
pub struct CubeTableExec {
    schema: SchemaRef,
    partition_execs: Vec<Arc<dyn ExecutionPlan>>,
}

#[async_trait]
impl ExecutionPlan for CubeTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_execs.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.partition_execs.clone()
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CubeTableExec {
            schema: self.schema.clone(),
            partition_execs: children,
        }))
    }

    async fn execute(&self, partition: usize) -> Result<Pin<Box<dyn RecordBatchStream + Send>>, DataFusionError> {
        self.partition_execs[partition].execute(0).await
    }
}

impl TableProvider for CubeTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        let handle = Handle::current();
        let res = handle.block_on(async move { self.async_scan(projection, batch_size).await });
        Ok(res?)
    }
}

#[async_trait]
impl QueryPlanner for QueryPlannerImpl {
    async fn execute_plan(&self, plan: LogicalPlan) -> Result<DataFrame, CubeError> {
        let ctx = self.execution_context().await?;

        let plan_ctx = ctx.clone();
        let plan_to_move = plan.clone();
        let physical_plan = tokio::task::spawn_blocking(move || {
            plan_ctx.create_physical_plan(&plan_to_move)
        }).await??;

        if format!("{:#?}", &physical_plan).contains("ParquetExec") {
            debug!("Physical plan: {:#?}", physical_plan);
        }

        let execution_time = SystemTime::now();
        let results = ctx.collect(physical_plan).await?;
        debug!("Query data processing time: {:?}", execution_time.elapsed()?);
        let data_frame = batch_to_dataframe(&results)?;
        Ok(data_frame)
    }

    async fn logical_plan(&self, statement: Statement) -> Result<LogicalPlan, CubeError> {
        let ctx = self.execution_context().await?;

        let query_planner = SqlToRel::new(&ctx.state);
        let mut logical_plan = query_planner.statement_to_plan(&statement)?;

        logical_plan = ctx.optimize(&logical_plan)?;

        Ok(logical_plan)
    }
}


impl QueryPlannerImpl {
    pub fn new(meta_store: Arc<dyn MetaStore>, remote_fs: Arc<dyn RemoteFs>, chunk_store: Arc<dyn ChunkDataStore>) -> Arc<QueryPlannerImpl> {
        Arc::new(QueryPlannerImpl {
            meta_store,
            remote_fs,
            chunk_store
        })
    }
}

impl QueryPlannerImpl {
    async fn execution_context(&self) -> Result<Arc<ExecutionContext>, CubeError> {
        let tables = self.meta_store.get_tables_with_path().await?;

        let mut ctx = ExecutionContext::new();

        for row in tables.iter() {
            let schema_name = row.schema.get_row().get_name().clone();
            let table_name = row.table.get_row().get_table_name().clone();
            let name = format!("{}.{}", schema_name.clone(), table_name.clone());
            let table = self.meta_store.get_table(schema_name.clone(), table_name.clone()).await?;
            let schema = Arc::new(Schema::new(table.get_row().get_columns().iter().map(|c| c.clone().into()).collect::<Vec<_>>()));
            let provider = CubeTable::try_new(schema_name, table_name, schema, self.meta_store.clone(), self.remote_fs.clone(), self.chunk_store.clone())?;
            ctx.register_table(&name, Box::new(provider));
        }

        ctx.register_table(
            "information_schema.tables",
            Box::new(InfoSchemaTableProvider::new(self.meta_store.clone(), InfoSchemaTable::Tables))
        );

        ctx.register_table(
            "information_schema.schemata",
            Box::new(InfoSchemaTableProvider::new(self.meta_store.clone(), InfoSchemaTable::Schemata))
        );

        Ok(Arc::new(ctx))
    }
}

pub enum InfoSchemaTable {
    Tables,
    Schemata
}

impl InfoSchemaTable {
    fn schema(&self) -> SchemaRef {
        match self {
            InfoSchemaTable::Tables => {
                Arc::new(Schema::new(
                    vec![
                        Field::new("table_schema", DataType::Utf8, false),
                        Field::new("table_name", DataType::Utf8, false)
                    ]
                ))
            }
            InfoSchemaTable::Schemata => {
                Arc::new(Schema::new(
                    vec![
                        Field::new("schema_name", DataType::Utf8, false),
                    ]
                ))
            }
        }
    }

    async fn scan(&self, meta_store: Arc<dyn MetaStore>) -> Result<RecordBatch, CubeError> {
        match self {
            InfoSchemaTable::Tables => {
                let tables = meta_store.get_tables_with_path().await?;
                let schema = self.schema();
                let columns: Vec<Arc<dyn Array>> = vec![
                    Arc::new(StringArray::from(tables.iter().map(|row| row.schema.get_row().get_name().as_str()).collect::<Vec<_>>())),
                    Arc::new(StringArray::from(tables.iter().map(|row| row.table.get_row().get_table_name().as_str()).collect::<Vec<_>>())),
                ];
                Ok(RecordBatch::try_new(schema, columns)?)
            }
            InfoSchemaTable::Schemata => {
                let schemas = meta_store.schemas_table().all_rows().await?;
                let schema = self.schema();
                let columns: Vec<Arc<dyn Array>> = vec![
                    Arc::new(StringArray::from(schemas.iter().map(|row| row.get_row().get_name().as_str()).collect::<Vec<_>>())),
                ];
                Ok(RecordBatch::try_new(schema, columns)?)
            }
        }
    }
}

pub struct InfoSchemaTableProvider {
    meta_store: Arc<dyn MetaStore>,
    table: InfoSchemaTable
}

impl InfoSchemaTableProvider {
    fn new(meta_store: Arc<dyn MetaStore>, table: InfoSchemaTable) -> InfoSchemaTableProvider {
        InfoSchemaTableProvider { meta_store, table }
    }

    async fn mem_table(&self) -> Result<MemTable, DataFusionError> {
        let batch = self.table.scan(self.meta_store.clone()).await?;
        MemTable::new(batch.schema(), vec![vec![batch]])
    }
}

impl TableProvider for InfoSchemaTableProvider {
    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let handle = Handle::current();
        let mem_table = handle.block_on(async move { self.mem_table().await })?;
        mem_table.scan(projection, batch_size)
    }
}