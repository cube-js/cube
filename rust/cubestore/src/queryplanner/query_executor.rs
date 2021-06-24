use crate::cluster::Cluster;
use crate::config::injection::DIService;
use crate::metastore::table::Table;
use crate::metastore::{Column, ColumnType, IdRow, Index, Partition};
use crate::queryplanner::optimizations::CubeQueryPlanner;
use crate::queryplanner::planning::get_worker_plan;
use crate::queryplanner::serialized_plan::{IndexSnapshot, SerializedPlan};
use crate::store::DataFrame;
use crate::table::{Row, TableValue, TimestampValue};
use crate::{app_metrics, CubeError};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, Int64Decimal0Array,
    Int64Decimal10Array, Int64Decimal1Array, Int64Decimal2Array, Int64Decimal3Array,
    Int64Decimal4Array, Int64Decimal5Array, StringArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Schema, SchemaRef, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::MemStreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use core::fmt;
use datafusion::datasource::datasource::{Statistics, TableProviderFilterPushDown};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use datafusion::logical_plan;
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, ToDFSchema};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::merge_sort::MergeSortExec;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::{
    collect, ExecutionPlan, OptimizerHints, Partitioning, SendableRecordBatchStream,
};
use itertools::Itertools;
use log::{debug, error, trace, warn};
use mockall::automock;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::cmp::min;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::Cursor;
use std::iter::FromIterator;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{instrument, Instrument};

#[automock]
#[async_trait]
pub trait QueryExecutor: DIService + Send + Sync {
    async fn execute_router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError>;

    async fn execute_worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError>;

    async fn router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError>;
    async fn worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError>;
}

crate::di_service!(MockQueryExecutor, [QueryExecutor]);

pub struct QueryExecutorImpl;

crate::di_service!(QueryExecutorImpl, [QueryExecutor]);

#[async_trait]
impl QueryExecutor for QueryExecutorImpl {
    #[instrument(level = "trace", skip(self, plan, cluster))]
    async fn execute_router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError> {
        let collect_span = tracing::span!(tracing::Level::TRACE, "collect_physical_plan");
        let (physical_plan, logical_plan) = self.router_plan(plan, cluster).await?;
        let split_plan = physical_plan;

        trace!("Router Query Physical Plan: {:#?}", &split_plan);

        let execution_time = SystemTime::now();

        let results = collect(split_plan.clone()).instrument(collect_span).await;
        let execution_time = execution_time.elapsed()?;
        debug!("Query data processing time: {:?}", execution_time,);
        app_metrics::DATA_QUERY_TIME_MS.report(execution_time.as_millis() as i64);
        if execution_time.as_millis() > 200 {
            warn!("Slow Query ({:?}):\n{:#?}", execution_time, logical_plan);
            debug!(
                "Slow Query Physical Plan ({:?}): {:#?}",
                execution_time, &split_plan
            );
        }
        if results.is_err() {
            error!("Error Query ({:?}):\n{:#?}", execution_time, logical_plan);
            error!(
                "Error Query Physical Plan ({:?}): {:#?}",
                execution_time, &split_plan
            );
        }
        Ok((split_plan.schema().to_schema_ref(), results?))
    }

    #[instrument(level = "trace", skip(self, plan, remote_to_local_names))]
    async fn execute_worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), CubeError> {
        let (physical_plan, logical_plan) = self.worker_plan(plan, remote_to_local_names).await?;

        let worker_plan;
        let max_batch_rows;
        if let Some((p, s)) = get_worker_plan(&physical_plan) {
            worker_plan = p;
            max_batch_rows = s;
        } else {
            error!("No worker marker in physical plan: {:?}", physical_plan);
            return Err(CubeError::internal(
                "Invalid physical plan on worker".to_string(),
            ));
        }

        trace!("Partition Query Physical Plan: {:#?}", &worker_plan);

        let execution_time = SystemTime::now();
        let results = collect(worker_plan.clone())
            .instrument(tracing::span!(
                tracing::Level::TRACE,
                "collect_physical_plan"
            ))
            .await;
        debug!(
            "Partition Query data processing time: {:?}",
            execution_time.elapsed()?
        );
        if execution_time.elapsed()?.as_millis() > 200 || results.is_err() {
            warn!(
                "Slow Partition Query ({:?}):\n{:#?}",
                execution_time.elapsed()?,
                logical_plan
            );
            debug!(
                "Slow Partition Query Physical Plan ({:?}): {:#?}",
                execution_time.elapsed()?,
                &worker_plan
            );
        }
        if results.is_err() {
            error!(
                "Error Partition Query ({:?}):\n{:#?}",
                execution_time.elapsed()?,
                logical_plan
            );
            error!(
                "Error Partition Query Physical Plan ({:?}): {:#?}",
                execution_time.elapsed()?,
                &worker_plan
            );
        }
        // TODO: stream results as they become available.
        let results = regroup_batches(results?, max_batch_rows)?;
        Ok((worker_plan.schema().to_schema_ref(), results))
    }

    async fn router_plan(
        &self,
        plan: SerializedPlan,
        cluster: Arc<dyn Cluster>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError> {
        let plan_to_move = plan.logical_plan(&HashMap::new())?;
        let serialized_plan = Arc::new(plan);
        let ctx = self.router_context(cluster.clone(), serialized_plan.clone())?;
        Ok((
            ctx.clone().create_physical_plan(&plan_to_move.clone())?,
            plan_to_move,
        ))
    }

    async fn worker_plan(
        &self,
        plan: SerializedPlan,
        remote_to_local_names: HashMap<String, String>,
    ) -> Result<(Arc<dyn ExecutionPlan>, LogicalPlan), CubeError> {
        let plan_to_move = plan.logical_plan(&remote_to_local_names)?;
        let plan = Arc::new(plan);
        let ctx = self.worker_context(plan.clone())?;
        let plan_ctx = ctx.clone();
        Ok((
            plan_ctx.create_physical_plan(&plan_to_move.clone())?,
            plan_to_move,
        ))
    }
}

impl QueryExecutorImpl {
    fn router_context(
        &self,
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
    ) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::with_config(
            ExecutionConfig::new()
                .with_batch_size(4096)
                .with_concurrency(1)
                .with_query_planner(Arc::new(CubeQueryPlanner::new_on_router(
                    cluster,
                    serialized_plan,
                ))),
        )))
    }

    fn worker_context(
        &self,
        serialized_plan: Arc<SerializedPlan>,
    ) -> Result<Arc<ExecutionContext>, CubeError> {
        Ok(Arc::new(ExecutionContext::with_config(
            ExecutionConfig::new()
                .with_batch_size(4096)
                .with_concurrency(1)
                .with_query_planner(Arc::new(CubeQueryPlanner::new_on_worker(serialized_plan))),
        )))
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
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
        let schema = Arc::new(Schema::new(
            index_snapshot
                .index()
                .get_row()
                .get_columns()
                .iter()
                .map(|c| c.clone().into())
                .collect::<Vec<_>>(),
        ));
        Ok(Self {
            index_snapshot,
            schema,
            remote_to_local_names,
            worker_partition_ids,
        })
    }

    #[must_use]
    pub fn to_worker_table(
        &self,
        remote_to_local_names: HashMap<String, String>,
        worker_partition_ids: HashSet<u64>,
    ) -> CubeTable {
        let mut t = self.clone();
        t.remote_to_local_names = remote_to_local_names;
        t.worker_partition_ids = worker_partition_ids;
        t
    }

    pub fn index_snapshot(&self) -> &IndexSnapshot {
        &self.index_snapshot
    }

    fn async_scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, CubeError> {
        let table = self.index_snapshot.table();
        let index = self.index_snapshot.index();
        let partition_snapshots = self.index_snapshot.partitions();

        let mut partition_execs = Vec::<Arc<dyn ExecutionPlan>>::new();

        let mapped_projection = projection.as_ref().map(|p| {
            CubeTable::project_to_index_positions(&CubeTable::project_to_table(&table, p), &index)
                .into_iter()
                .map(|i| i.unwrap())
                .collect::<Vec<_>>()
        });

        let predicate = combine_filters(filters);
        for partition_snapshot in partition_snapshots {
            if !self
                .worker_partition_ids
                .contains(&partition_snapshot.partition().get_id())
            {
                continue;
            }
            let partition = partition_snapshot.partition();

            if let Some(remote_path) = partition.get_row().get_full_name(partition.get_id()) {
                let local_path = self
                    .remote_to_local_names
                    .get(remote_path.as_str())
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let arc: Arc<dyn ExecutionPlan> = Arc::new(ParquetExec::try_from_path(
                    &local_path,
                    mapped_projection.clone(),
                    predicate.clone(),
                    batch_size,
                    1,
                    None, // TODO: propagate limit
                )?);
                partition_execs.push(arc);
            }

            let chunks = partition_snapshot.chunks();
            for chunk in chunks {
                let remote_path = chunk.get_row().get_full_name(chunk.get_id());
                let local_path = self
                    .remote_to_local_names
                    .get(&remote_path)
                    .expect(format!("Missing remote path {}", remote_path).as_str());
                let node = Arc::new(ParquetExec::try_from_path(
                    local_path,
                    mapped_projection.clone(),
                    predicate.clone(),
                    batch_size,
                    1,
                    None, // TODO: propagate limit
                )?);
                partition_execs.push(node);
            }
        }

        if partition_execs.len() == 0 {
            partition_execs.push(Arc::new(EmptyExec::new(false, self.schema.clone())));
        }

        let projected_schema = if let Some(p) = mapped_projection {
            Arc::new(Schema::new(
                self.schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter_map(|(i, f)| p.iter().find(|p_i| *p_i == &i).map(|_| f.clone()))
                    .collect(),
            ))
        } else {
            self.schema.clone()
        };

        let schema = projected_schema.to_dfschema_ref()?;
        let plan: Arc<dyn ExecutionPlan> = if let Some(join_columns) = self.index_snapshot.sort_on()
        {
            Arc::new(MergeSortExec::try_new(
                Arc::new(CubeTableExec {
                    schema,
                    partition_execs,
                    index_snapshot: self.index_snapshot.clone(),
                    filter: predicate,
                }),
                join_columns.clone(),
            )?)
        } else {
            Arc::new(MergeExec::new(Arc::new(CubeTableExec {
                schema,
                partition_execs,
                index_snapshot: self.index_snapshot.clone(),
                filter: predicate,
            })))
        };

        Ok(plan)
    }

    pub fn project_to_index_positions(
        projection_columns: &Vec<Column>,
        i: &IdRow<Index>,
    ) -> Vec<Option<usize>> {
        projection_columns
            .iter()
            .map(|pc| {
                i.get_row()
                    .get_columns()
                    .iter()
                    .find_position(|c| c.get_name() == pc.get_name())
                    .map(|(p, _)| p)
            })
            .collect::<Vec<_>>()
    }

    pub fn project_to_table(
        table: &IdRow<Table>,
        projection_column_indices: &Vec<usize>,
    ) -> Vec<Column> {
        projection_column_indices
            .iter()
            .map(|i| table.get_row().get_columns()[*i].clone())
            .collect::<Vec<_>>()
    }
}

pub struct CubeTableExec {
    schema: DFSchemaRef,
    pub(crate) index_snapshot: IndexSnapshot,
    partition_execs: Vec<Arc<dyn ExecutionPlan>>,
    pub(crate) filter: Option<Expr>,
}

impl Debug for CubeTableExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CubeTableExec")
            .field("index", self.index_snapshot.index())
            .field("partition_execs", &self.partition_execs)
            .finish()
    }
}

#[async_trait]
impl ExecutionPlan for CubeTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DFSchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partition_execs.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.partition_execs.clone()
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CubeTableExec {
            schema: self.schema.clone(),
            partition_execs: children,
            index_snapshot: self.index_snapshot.clone(),
            filter: self.filter.clone(),
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        let sort_order;
        if let Some(snapshot_sort_on) = self.index_snapshot.sort_on() {
            // Note that this returns `None` if any of the columns were not found.
            // This only happens on programming errors.
            sort_order = snapshot_sort_on
                .iter()
                .map(|c| self.schema.index_of(&c).ok())
                .collect()
        } else {
            let index = self.index_snapshot.index().get_row();
            let sort_cols = index
                .get_columns()
                .iter()
                .take(index.sort_key_size() as usize)
                .map(|sort_col| self.schema.index_of(&sort_col.get_name()).ok())
                .take_while(|i| i.is_some())
                .map(|i| i.unwrap())
                .collect_vec();
            if !sort_cols.is_empty() {
                sort_order = Some(sort_cols)
            } else {
                sort_order = None
            }
        }

        OptimizerHints {
            sort_order,
            single_value_columns: Vec::new(),
        }
    }

    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        self.partition_execs[partition].execute(0).await
    }
}

pub struct ClusterSendExec {
    schema: DFSchemaRef,
    pub partitions: Vec<(/*node*/ String, /*partition_id*/ Vec<u64>)>,
    /// Never executed, only stored to allow consistent optimization on router and worker.
    pub input_for_optimizations: Arc<dyn ExecutionPlan>,
    pub cluster: Arc<dyn Cluster>,
    pub serialized_plan: Arc<SerializedPlan>,
    pub use_streaming: bool,
}

impl ClusterSendExec {
    pub fn new(
        schema: DFSchemaRef,
        cluster: Arc<dyn Cluster>,
        serialized_plan: Arc<SerializedPlan>,
        union_snapshots: Vec<Vec<IndexSnapshot>>,
        input_for_optimizations: Arc<dyn ExecutionPlan>,
        use_streaming: bool,
    ) -> Self {
        let partitions = Self::logical_partitions(&union_snapshots);
        let partitions = Self::assign_nodes(cluster.as_ref(), partitions);
        Self {
            schema,
            partitions,
            cluster,
            serialized_plan,
            input_for_optimizations,
            use_streaming,
        }
    }

    pub fn logical_partitions(snapshots: &[Vec<IndexSnapshot>]) -> Vec<Vec<IdRow<Partition>>> {
        let to_multiply = snapshots
            .iter()
            .map(|union| {
                union
                    .iter()
                    .flat_map(|index| index.partitions().iter().map(|p| p.partition().clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let partitions = to_multiply
            .into_iter()
            .multi_cartesian_product()
            .collect::<Vec<Vec<_>>>();
        partitions
    }

    fn assign_nodes(
        c: &dyn Cluster,
        logical: Vec<Vec<IdRow<Partition>>>,
    ) -> Vec<(String, Vec<u64>)> {
        let mut m: HashMap<String, Vec<u64>> = HashMap::new();
        for ps in &logical {
            let ids = ps.iter().map(|p| p.get_id()).collect_vec();
            m.entry(c.node_name_by_partitions(&ids))
                .or_default()
                .extend(ids)
        }

        let mut r = m.into_iter().collect_vec();
        r.sort_unstable_by(|l, r| l.0.cmp(&r.0));
        r
    }

    pub fn with_changed_schema(
        &self,
        schema: DFSchemaRef,
        input_for_optimizations: Arc<dyn ExecutionPlan>,
    ) -> Self {
        ClusterSendExec {
            schema,
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            serialized_plan: self.serialized_plan.clone(),
            input_for_optimizations,
            use_streaming: self.use_streaming,
        }
    }
}

#[async_trait]
impl ExecutionPlan for ClusterSendExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> DFSchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input_for_optimizations.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            panic!("expected exactly one input");
        }
        let input_for_optimizations = children.into_iter().next().unwrap();
        Ok(Arc::new(ClusterSendExec {
            schema: self.schema.clone(),
            partitions: self.partitions.clone(),
            cluster: self.cluster.clone(),
            serialized_plan: self.serialized_plan.clone(),
            input_for_optimizations,
            use_streaming: self.use_streaming,
        }))
    }

    fn output_hints(&self) -> OptimizerHints {
        self.input_for_optimizations.output_hints()
    }

    #[instrument(level = "trace", skip(self))]
    async fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let (node_name, ids) = &self.partitions[partition];
        let plan = self
            .serialized_plan
            .with_partition_id_to_execute(HashSet::from_iter(ids.iter().cloned()));
        if self.use_streaming {
            Ok(self.cluster.run_select_stream(node_name, plan).await?)
        } else {
            let record_batches = self.cluster.run_select(node_name, plan).await?;
            // TODO .to_schema_ref()
            let memory_exec =
                MemoryExec::try_new(&vec![record_batches], self.schema.to_schema_ref(), None)?;
            memory_exec.execute(0).await
        }
    }
}

impl fmt::Debug for ClusterSendExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        f.write_fmt(format_args!(
            "ClusterSendExec: {:?}: {:?}",
            self.schema, self.partitions
        ))
    }
}

impl TableProvider for CubeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        _limit: Option<usize>, // TODO: propagate limit
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let res = self.async_scan(projection, batch_size, filters)?;
        Ok(res)
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

macro_rules! convert_array_cast_native {
    ($V: expr, (Vec<u8>)) => {{
        $V.to_vec()
    }};
    ($V: expr, (Decimal)) => {{
        crate::util::decimal::Decimal::new($V)
    }};
    ($V: expr, $T: ty) => {{
        $V as $T
    }};
}

macro_rules! convert_array {
    ($ARRAY:expr, $NUM_ROWS:expr, $ROWS:expr, $ARRAY_TYPE: ident, $TABLE_TYPE: ident, $NATIVE: tt) => {{
        let a = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        for i in 0..$NUM_ROWS {
            $ROWS[i].push(if a.is_null(i) {
                TableValue::Null
            } else {
                TableValue::$TABLE_TYPE(convert_array_cast_native!(a.value(i), $NATIVE))
            });
        }
    }};
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
                DataType::UInt64 => convert_array!(array, num_rows, rows, UInt64Array, Int, i64),
                DataType::Int64 => convert_array!(array, num_rows, rows, Int64Array, Int, i64),
                DataType::Float64 => {
                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            let decimal = a.value(i) as f64;
                            TableValue::Float(decimal.into())
                        });
                    }
                }
                DataType::Int64Decimal(0) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal0Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(1) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal1Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(2) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal2Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(3) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal3Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(4) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal4Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(5) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal5Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Int64Decimal(10) => convert_array!(
                    array,
                    num_rows,
                    rows,
                    Int64Decimal10Array,
                    Decimal,
                    (Decimal)
                ),
                DataType::Timestamp(TimeUnit::Microsecond, None) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i) * 1000 as i64))
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i)))
                        });
                    }
                }
                DataType::Binary => {
                    convert_array!(array, num_rows, rows, BinaryArray, Bytes, (Vec<u8>))
                }
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::String(a.value(i).to_string())
                        });
                    }
                }
                DataType::Boolean => {
                    let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Boolean(a.value(i))
                        });
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
        DataType::Binary => Ok(ColumnType::Bytes),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnType::String),
        DataType::Timestamp(_, _) => Ok(ColumnType::Timestamp),
        DataType::Float16 | DataType::Float64 => Ok(ColumnType::Float),
        DataType::Int64Decimal(scale) => Ok(ColumnType::Decimal {
            scale: scale as i32,
            precision: 18,
        }),
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
    #[serde(with = "serde_bytes")] // serde_bytes makes serialization efficient.
    record_batch_file: Vec<u8>,
}

impl SerializedRecordBatchStream {
    pub fn write(
        schema: &Schema,
        record_batches: Vec<RecordBatch>,
    ) -> Result<Vec<Self>, CubeError> {
        let mut results = Vec::with_capacity(record_batches.len());
        for batch in record_batches {
            let file = Vec::new();
            let mut writer = MemStreamWriter::try_new(Cursor::new(file), schema)?;
            writer.write(&batch)?;
            let cursor = writer.finish()?;
            results.push(Self {
                record_batch_file: cursor.into_inner(),
            })
        }
        Ok(results)
    }

    pub fn read(self) -> Result<RecordBatch, CubeError> {
        let cursor = Cursor::new(self.record_batch_file);
        let mut reader = StreamReader::try_new(cursor)?;
        let batch = reader.next();
        if batch.is_none() {
            return Err(CubeError::internal("zero batches deserialized".to_string()));
        }
        let batch = batch.unwrap()?;
        if !reader.next().is_none() {
            return Err(CubeError::internal(
                "more than one batch deserialized".to_string(),
            ));
        }
        Ok(batch)
    }
}
/// Note: copy of the function in 'datafusion/src/datasource/parquet.rs'.
///
/// Combines an array of filter expressions into a single filter expression
/// consisting of the input filter expressions joined with logical AND.
/// Returns None if the filters array is empty.
fn combine_filters(filters: &[Expr]) -> Option<Expr> {
    if filters.is_empty() {
        return None;
    }
    let combined_filter = filters
        .iter()
        .skip(1)
        .fold(filters[0].clone(), |acc, filter| {
            logical_plan::and(acc, filter.clone())
        });
    Some(combined_filter)
}

fn regroup_batches(
    batches: Vec<RecordBatch>,
    max_rows: usize,
) -> Result<Vec<RecordBatch>, CubeError> {
    let mut r = Vec::with_capacity(batches.len());
    for b in batches {
        let mut row = 0;
        while row != b.num_rows() {
            let slice_len = min(b.num_rows() - row, max_rows);
            r.push(RecordBatch::try_new(
                b.schema(),
                b.columns()
                    .iter()
                    .map(|c| slice_copy(c.as_ref(), row, slice_len))
                    .collect::<Result<Vec<_>, _>>()?,
            )?);
            row += slice_len
        }
    }
    Ok(r)
}

fn slice_copy(a: &dyn Array, start: usize, len: usize) -> Result<ArrayRef, CubeError> {
    // If we use [Array::slice], serialization will still copy the whole contents.
    Ok(take(
        a,
        &UInt64Array::from_iter_values(start as u64..(start + len) as u64),
        None,
    )?)
}
