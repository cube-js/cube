use crate::queryplanner::serialized_plan::{SerializedPlan, IndexSnapshot};
use crate::store::{DataFrame};
use crate::CubeError;
use std::sync::Arc;
use datafusion::execution::context::ExecutionContext;
use arrow::datatypes::{SchemaRef, Schema, DataType, TimeUnit};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, RecordBatchStream};
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::merge::MergeExec;
use std::any::Any;
use datafusion::error::DataFusionError;
use std::pin::Pin;
use datafusion::datasource::TableProvider;
use crate::metastore::{Column, Index, IdRow, ColumnType};
use itertools::Itertools;
use crate::metastore::table::Table;
use std::time::SystemTime;
use arrow::record_batch::RecordBatch;
use crate::table::{Row, TableValue, TimestampValue};
use arrow::array::{UInt64Array, Int64Array, Float64Array, TimestampMicrosecondArray, TimestampNanosecondArray, StringArray, Array, BooleanArray};
use std::collections::HashMap;
use async_trait::async_trait;
use mockall::automock;
use log::{debug};
use datafusion::{error::{Result as DFResult}};
use bigdecimal::BigDecimal;
use std::convert::TryFrom;

#[automock]
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute_plan(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<DataFrame, CubeError>;
}

pub struct QueryExecutorImpl;

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    async fn execute_plan(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<DataFrame, CubeError> {
        let plan_to_move = plan.logical_plan();
        let ctx = self.execution_context(plan, remote_to_local_names)?;
        let plan_ctx = ctx.clone();

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
}

impl QueryExecutorImpl {
    fn execution_context(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<Arc<ExecutionContext>, CubeError> {
        let index_snapshots = plan.index_snapshots();

        let mut ctx = ExecutionContext::new();

        for row in index_snapshots.iter() {
            let provider = CubeTable::try_new(row.clone(), remote_to_local_names.clone())?; // TODO Clone
            ctx.register_table(&row.table_name(), Box::new(provider));
        }

        Ok(Arc::new(ctx))
    }
}

pub struct CubeTable {
    index_snapshot: IndexSnapshot,
    remote_to_local_names: HashMap<String, String>,
    schema: SchemaRef
}

impl CubeTable {
    pub fn try_new(
        index_snapshot: IndexSnapshot,
        remote_to_local_names: HashMap<String, String>,
    ) -> Result<Self, CubeError> {
        let schema = Arc::new(Schema::new(index_snapshot.table().get_row().get_columns().iter().map(|c| c.clone().into()).collect::<Vec<_>>()));
        Ok(Self { index_snapshot, schema, remote_to_local_names })
    }

    fn async_scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let table = self.index_snapshot.table();
        let index = self.index_snapshot.index();
        let partition_snapshots = self.index_snapshot.partitions();

        let mut partition_execs = Vec::<Arc<dyn ExecutionPlan>>::new();

        let schema = Arc::new(Schema::new(index.get_row().get_columns().iter().map(|c| c.clone().into()).collect::<Vec<_>>()));
        let mapped_projection = projection.as_ref()
            .map(
                |p| CubeTable::project_to_index_positions(&CubeTable::project_to_table(&table, p), &index)
                    .into_iter()
                    .map(|i| i.unwrap())
                    .collect::<Vec<_>>()
            );

        for partition_snapshot in partition_snapshots {
            let mut execs = Vec::new();
            let partition = partition_snapshot.partition();

            if let Some(remote_path) = partition.get_row().get_full_name(partition.get_id()) {
                let local_path = self.remote_to_local_names.get(remote_path.as_str())
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let arc: Arc<dyn ExecutionPlan> = Arc::new(ParquetExec::try_new(&local_path, mapped_projection.clone(), batch_size)?);
                execs.push(arc);
            }

            // TODO look up in not repartitioned parent chunks
            let chunks = partition_snapshot.chunks();
            for chunk in chunks {
                let remote_path = chunk.get_row().get_full_name(chunk.get_id());
                let local_path = self.remote_to_local_names.get(&remote_path)
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let node = Arc::new(ParquetExec::try_new(local_path, mapped_projection.clone(), batch_size)?);
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

    pub fn project_to_index_positions(projection_columns: &Vec<Column>, i: &IdRow<Index>) -> Vec<Option<usize>> {
        projection_columns.iter().map(
            |pc| i.get_row().get_columns().iter().find_position(|c| c.get_name() == pc.get_name()).map(|(p, _)| p)
        ).collect::<Vec<_>>()
    }

    pub fn project_to_table(table: &IdRow<Table>, projection_column_indices: &Vec<usize>) -> Vec<Column> {
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
        let res = self.async_scan(projection, batch_size)?;
        Ok(res)
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
                        rows[i].push(if a.is_null(i) { TableValue::Null } else { TableValue::Decimal(BigDecimal::try_from(a.value(i) as f64)?.to_string()) });
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