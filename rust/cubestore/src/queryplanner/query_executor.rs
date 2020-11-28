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
use crate::metastore::{Column, Index, IdRow, ColumnType, Partition};
use itertools::Itertools;
use crate::metastore::table::Table;
use std::time::SystemTime;
use arrow::record_batch::RecordBatch;
use crate::table::{Row, TableValue, TimestampValue};
use arrow::array::{UInt64Array, Int64Array, Float64Array, TimestampMicrosecondArray, TimestampNanosecondArray, StringArray, Array, BooleanArray};
use std::collections::{HashMap, HashSet};
use async_trait::async_trait;
use mockall::automock;
use log::{debug, warn, trace};
use datafusion::{error::{Result as DFResult}};
use bigdecimal::BigDecimal;
use std::convert::TryFrom;
use datafusion::physical_plan::hash_aggregate::HashAggregateExec;
use crate::cluster::Cluster;
use datafusion::physical_plan::memory::{MemoryExec};
use serde_derive::{Deserialize, Serialize};
use arrow::ipc::writer::{MemStreamWriter};
use std::io::Cursor;
use arrow::ipc::reader::{StreamReader};
use core::{fmt};
use std::fmt::Formatter;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::hash_join::HashJoinExec;

#[automock]
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute_plan(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<DataFrame, CubeError>;

    async fn execute_router_plan(&self, plan: SerializedPlan, cluster: Arc<dyn Cluster>) -> Result<DataFrame, CubeError>;

    async fn execute_worker_plan(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<Vec<RecordBatch>, CubeError>;
}

pub struct QueryExecutorImpl;

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    async fn execute_plan(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<DataFrame, CubeError> {
        let plan_to_move = plan.logical_plan();
        let ctx = self.execution_context(plan.index_snapshots(), remote_to_local_names, HashSet::new())?;
        let plan_ctx = ctx.clone();

        let physical_plan = tokio::task::spawn_blocking(move || {
            plan_ctx.create_physical_plan(&plan_to_move)
        }).await??;

        let execution_time = SystemTime::now();
        let results = ctx.collect(physical_plan.clone()).await?;
        debug!("Query data processing time: {:?}", execution_time.elapsed()?);
        if execution_time.elapsed()?.as_millis() > 200 {
            warn!("Slow Query ({:?}):\n{:#?}", execution_time.elapsed()?, plan.logical_plan());
            debug!("Slow Query Physical Plan ({:?}): {:#?}", execution_time.elapsed()?, &physical_plan);
        }
        let data_frame = batch_to_dataframe(&results)?;
        Ok(data_frame)
    }

    async fn execute_router_plan(&self, plan: SerializedPlan, cluster: Arc<dyn Cluster>) -> Result<DataFrame, CubeError> {
        let plan_to_move = plan.logical_plan();
        let ctx = self.execution_context(plan.index_snapshots(), HashMap::new(), HashSet::new())?;
        let plan_ctx = ctx.clone();

        let serialized_plan = Arc::new(plan);
        let physical_plan = plan_ctx.create_physical_plan(&plan_to_move)?;
        let available_nodes = cluster.available_nodes().await?;
        let split_plan = self.get_router_split_plan(physical_plan, serialized_plan.clone(), cluster, available_nodes)?;

        let execution_time = SystemTime::now();
        let results = ctx.collect(split_plan.clone()).await?;
        debug!("Query data processing time: {:?}", execution_time.elapsed()?);
        if execution_time.elapsed()?.as_millis() > 200 {
            warn!("Slow Query ({:?}):\n{:#?}", execution_time.elapsed()?, serialized_plan.logical_plan());
            debug!("Slow Query Physical Plan ({:?}): {:#?}", execution_time.elapsed()?, &split_plan);
        } else {
            trace!("Router Query Physical Plan ({:?}): {:#?}", execution_time.elapsed()?, &split_plan);
        }
        let data_frame = batch_to_dataframe(&results)?;
        Ok(data_frame)
    }

    async fn execute_worker_plan(&self, plan: SerializedPlan, remote_to_local_names: HashMap<String, String>) -> Result<Vec<RecordBatch>, CubeError> {
        let plan_to_move = plan.logical_plan();
        let ctx = self.execution_context(plan.index_snapshots(), remote_to_local_names, plan.partition_ids_to_execute())?;
        let plan_ctx = ctx.clone();

        let physical_plan = plan_ctx.create_physical_plan(&plan_to_move)?;

        let worker_plan = self.get_worker_split_plan(physical_plan);

        let execution_time = SystemTime::now();
        let results = ctx.collect(worker_plan.clone()).await;
        debug!("Partition Query data processing time: {:?}", execution_time.elapsed()?);
        if execution_time.elapsed()?.as_millis() > 200 || results.is_err() {
            warn!("Slow Partition Query ({:?}):\n{:#?}", execution_time.elapsed()?, plan.logical_plan());
            debug!("Slow Partition Query Physical Plan ({:?}): {:#?}", execution_time.elapsed()?, &worker_plan);
        }
        Ok(results?)
    }
}

impl QueryExecutorImpl {
    fn execution_context(&self, index_snapshots: &Vec<IndexSnapshot>, remote_to_local_names: HashMap<String, String>, worker_partition_id: HashSet<u64>) -> Result<Arc<ExecutionContext>, CubeError> {
        let mut ctx = ExecutionContext::new();

        for row in index_snapshots.iter() {
            let provider = CubeTable::try_new(row.clone(), remote_to_local_names.clone(), worker_partition_id.clone())?; // TODO Clone
            ctx.register_table(&row.table_name(), Box::new(provider));
        }

        Ok(Arc::new(ctx))
    }

    fn get_router_split_plan(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        serialized_plan: Arc<SerializedPlan>,
        cluster: Arc<dyn Cluster>,
        available_nodes: Vec<String>,
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        if self.has_node::<HashAggregateExec>(execution_plan.clone()) {
            self.get_router_split_plan_at(
                execution_plan,
                serialized_plan,
                cluster,
                available_nodes,
                |h| h.as_any().downcast_ref::<HashAggregateExec>().is_some()
            )
        } else if self.has_node::<SortExec>(execution_plan.clone()) {
            self.get_router_split_plan_at(
                execution_plan,
                serialized_plan,
                cluster,
                available_nodes,
                |h| h.as_any().downcast_ref::<SortExec>().is_some()
            )
        } else if self.has_node::<GlobalLimitExec>(execution_plan.clone()) {
            self.get_router_split_plan_at(
                execution_plan,
                serialized_plan,
                cluster,
                available_nodes,
                |h| h.as_any().downcast_ref::<GlobalLimitExec>().is_some()
            )
        } else {
            self.get_router_split_plan_at(
                execution_plan,
                serialized_plan,
                cluster,
                available_nodes,
                |_| true
            )
        }
    }

    fn get_worker_split_plan(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        if self.has_node::<HashAggregateExec>(execution_plan.clone()) {
            self.get_worker_split_plan_at(
                execution_plan,
                |h| h.as_any().downcast_ref::<HashAggregateExec>().is_some()
            )
        } else if self.has_node::<SortExec>(execution_plan.clone()) {
            self.get_worker_split_plan_at(
                execution_plan,
                |h| h.as_any().downcast_ref::<SortExec>().is_some()
            )
        } else if self.has_node::<GlobalLimitExec>(execution_plan.clone()) {
            self.get_worker_split_plan_at(
                execution_plan,
                |h| h.as_any().downcast_ref::<GlobalLimitExec>().is_some()
            )
        } else {
            self.get_worker_split_plan_at(
                execution_plan,
                |_| true
            )
        }
    }

    fn get_worker_split_plan_at(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        split_at_fn: impl Fn(Arc<dyn ExecutionPlan>) -> bool
    ) -> Arc<dyn ExecutionPlan> {
        let children = execution_plan.children();
        assert!(children.len() == 1, "Only one child is expected for {:?}", &execution_plan);
        if split_at_fn(execution_plan.clone()) {
            children[0].clone()
        } else {
            self.get_worker_split_plan(children[0].clone())
        }
    }

    fn get_router_split_plan_at(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
        serialized_plan: Arc<SerializedPlan>,
        cluster: Arc<dyn Cluster>,
        available_nodes: Vec<String>,
        split_at_fn: impl Fn(Arc<dyn ExecutionPlan>) -> bool
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        if split_at_fn(execution_plan.clone()) {
            let children = execution_plan.children();
            self.wrap_with_cluster_send(execution_plan, serialized_plan, cluster, available_nodes, children)
        } else {
            let children = execution_plan.children().iter()
                .map(move |c| self.get_router_split_plan(c.clone(), serialized_plan.clone(), cluster.clone(), available_nodes.clone()))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(execution_plan.with_new_children(children)?)
        }
    }

    fn wrap_with_cluster_send(&self, execution_plan: Arc<dyn ExecutionPlan>, serialized_plan: Arc<SerializedPlan>, cluster: Arc<dyn Cluster>, available_nodes: Vec<String>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let index_snapshots = self.index_snapshots_from_cube_table(execution_plan.clone());
        if index_snapshots.len() > 0 {
            let cluster_exec = Arc::new(ClusterSendExec::new(
                children[0].schema(),
                cluster,
                serialized_plan,
                available_nodes,
                index_snapshots,
            ));
            Ok(execution_plan.with_new_children(vec![Arc::new(MergeExec::new(cluster_exec))])?)
        } else {
            Ok(execution_plan.with_new_children(vec![Arc::new(EmptyExec::new(false, children[0].schema()))])?)
        }
    }

    fn has_node<T: Any>(&self, execution_plan: Arc<dyn ExecutionPlan>) -> bool {
        if execution_plan.as_any().downcast_ref::<T>().is_some() {
            true
        } else {
            execution_plan.children().into_iter().find(|c| self.has_node::<T>(c.clone())).is_some()
        }
    }

    fn index_snapshots_from_cube_table(
        &self,
        execution_plan: Arc<dyn ExecutionPlan>,
    ) -> Vec<IndexSnapshot> {
        if let Some(cube_table) = execution_plan.as_any().downcast_ref::<CubeTableExec>() {
            vec![cube_table.index_snapshot.clone()]
        } else {
            execution_plan.children().iter().flat_map(|e| self.index_snapshots_from_cube_table(e.clone())).collect::<Vec<_>>()
        }
    }
}

pub struct CubeTable {
    index_snapshot: IndexSnapshot,
    remote_to_local_names: HashMap<String, String>,
    worker_partition_ids: HashSet<u64>,
    schema: SchemaRef,
}

impl CubeTable {
    pub fn try_new(
        index_snapshot: IndexSnapshot,
        remote_to_local_names: HashMap<String, String>,
        worker_partition_ids: HashSet<u64>,
    ) -> Result<Self, CubeError> {
        let schema = Arc::new(Schema::new(index_snapshot.table().get_row().get_columns().iter().map(|c| c.clone().into()).collect::<Vec<_>>()));
        Ok(Self { index_snapshot, schema, remote_to_local_names, worker_partition_ids })
    }

    fn async_scan(&self, projection: &Option<Vec<usize>>, batch_size: usize) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let table = self.index_snapshot.table();
        let index = self.index_snapshot.index();
        let partition_snapshots = self.index_snapshot.partitions();

        let mut partition_execs = Vec::<Arc<dyn ExecutionPlan>>::new();

        let mapped_projection = projection.as_ref()
            .map(
                |p| CubeTable::project_to_index_positions(&CubeTable::project_to_table(&table, p), &index)
                    .into_iter()
                    .map(|i| i.unwrap())
                    .collect::<Vec<_>>()
            );

        for partition_snapshot in partition_snapshots {
            if !self.worker_partition_ids.contains(&partition_snapshot.partition().get_id()) {
                continue;
            }
            let partition = partition_snapshot.partition();

            if let Some(remote_path) = partition.get_row().get_full_name(partition.get_id()) {
                let local_path = self.remote_to_local_names.get(remote_path.as_str())
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let arc: Arc<dyn ExecutionPlan> = Arc::new(ParquetExec::try_new(&local_path, mapped_projection.clone(), batch_size)?);
                partition_execs.push(arc);
            }

            // TODO look up in not repartitioned parent chunks
            let chunks = partition_snapshot.chunks();
            for chunk in chunks {
                let remote_path = chunk.get_row().get_full_name(chunk.get_id());
                let local_path = self.remote_to_local_names.get(&remote_path)
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let node = Arc::new(ParquetExec::try_new(local_path, mapped_projection.clone(), batch_size)?);
                partition_execs.push(node);
            }
        }

        if partition_execs.len() == 0 {
            partition_execs.push(Arc::new(EmptyExec::new(false, self.schema.clone())));
        }

        let projected_schema = if let Some(p) = projection {
            Arc::new(Schema::new(
                p.iter().map(|i| self.schema.field(*i).clone()).collect()
            ))
        } else {
            self.schema.clone()
        };

        let plan = Arc::new(MergeExec::new(Arc::new(
            CubeTableExec { schema: projected_schema, partition_execs, index_snapshot: self.index_snapshot.clone() }
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
    index_snapshot: IndexSnapshot,
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
            index_snapshot: self.index_snapshot.clone()
        }))
    }

    async fn execute(&self, partition: usize) -> Result<Pin<Box<dyn RecordBatchStream + Send>>, DataFusionError> {
        self.partition_execs[partition].execute(0).await
    }
}

pub struct ClusterSendExec {
    schema: SchemaRef,
    partitions: Vec<Vec<IdRow<Partition>>>,
    cluster: Arc<dyn Cluster>,
    available_nodes: Vec<String>,
    serialized_plan: Arc<SerializedPlan>,
}

impl ClusterSendExec {
    pub fn new(
        schema: SchemaRef,
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
        available_nodes: Vec<String>,
        index_snapshots: Vec<IndexSnapshot>,
    ) -> Self {
        let partitions = index_snapshots.into_iter()
            .map(|index| index.partitions().iter().map(|p| p.partition().clone()).collect::<Vec<_>>())
            .multi_cartesian_product()
            .collect::<Vec<Vec<_>>>();
        Self {
            schema,
            partitions,
            cluster,
            available_nodes,
            serialized_plan,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ClusterSendExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 0 {
            panic!("Expected to be a leaf node");
        }
        Ok(Arc::new(ClusterSendExec {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            available_nodes: self.available_nodes.clone(),
            serialized_plan: self.serialized_plan.clone(),
        }))
    }

    async fn execute(&self, partition: usize) -> Result<Pin<Box<dyn RecordBatchStream + Send>>, DataFusionError> {
        let record_batches = self.cluster.run_select(
            self.available_nodes[0].clone(), // TODO find node by partition
            self.serialized_plan.with_partition_id_to_execute(self.partitions[partition].iter().map(|p| p.get_id()).collect()),
        ).await?;
        let memory_exec = MemoryExec::try_new(&vec![record_batches], self.schema.clone(), None)?;
        memory_exec.execute(0).await
    }
}

impl fmt::Debug for ClusterSendExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "ClusterSendExec: {:?}: {:?}",
            self.schema,
            self.partitions
        ))
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
        DataType::Boolean => Ok(ColumnType::Boolean),
        DataType::Int8
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

#[derive(Serialize, Deserialize, Debug)]
pub struct SerializedRecordBatchStream {
    record_batch_file: Vec<u8>
}

impl SerializedRecordBatchStream {
    pub fn write(record_batches: Vec<RecordBatch>) -> Result<Self, CubeError> {
        let file = Vec::new();
        let mut writer = MemStreamWriter::try_new(Cursor::new(file), &record_batches[0].schema())?;
        for batch in record_batches.iter() {
            writer.write(batch)?;
        }
        let cursor = writer.finish()?;
        Ok(Self { record_batch_file: cursor.into_inner() })
    }

    pub fn read(self) -> Result<Vec<RecordBatch>, CubeError> {
        let cursor = Cursor::new(self.record_batch_file);
        let reader = StreamReader::try_new(cursor)?;
        Ok(reader.collect::<Result<Vec<_>, _>>()?)
    }
}