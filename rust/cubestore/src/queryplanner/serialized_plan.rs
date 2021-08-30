use crate::metastore::table::{Table, TablePath};
use crate::metastore::{Chunk, IdRow, Index, Partition};
use crate::queryplanner::planning::ClusterSendNode;
use crate::queryplanner::query_executor::CubeTable;
use crate::queryplanner::topk::{ClusterAggregateTopK, SortColumn};
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{
    aggregate_kind_by_name, scalar_kind_by_name, scalar_udf_by_kind, CubeAggregateUDFKind,
    CubeScalarUDFKind,
};
use crate::CubeError;
use arrow::datatypes::DataType;
use datafusion::cube_ext::alias::LogicalAlias;
use datafusion::cube_ext::join::SkewedLeftCrossJoin;
use datafusion::cube_ext::joinagg::CrossJoinAgg;
use datafusion::cube_ext::rolling::RollingWindowAggregate;
use datafusion::logical_plan::window_frames::WindowFrameBound;
use datafusion::logical_plan::{
    Column, DFSchemaRef, Expr, JoinConstraint, JoinType, LogicalPlan, Operator, Partitioning,
    PlanVisitor,
};
use datafusion::physical_plan::{aggregates, functions};
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::RollingOffset;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SerializedPlan {
    logical_plan: Arc<SerializedLogicalPlan>,
    schema_snapshot: Arc<SchemaSnapshot>,
    partition_ids_to_execute: HashSet<u64>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SchemaSnapshot {
    index_snapshots: Vec<IndexSnapshot>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct IndexSnapshot {
    pub table_path: TablePath,
    pub index: IdRow<Index>,
    pub partitions: Vec<PartitionSnapshot>,
    pub sort_on: Option<Vec<String>>,
}

impl IndexSnapshot {
    pub fn table_name(&self) -> String {
        self.table_path.table_name()
    }

    pub fn table(&self) -> &IdRow<Table> {
        &self.table_path.table
    }

    pub fn index(&self) -> &IdRow<Index> {
        &self.index
    }

    pub fn partitions(&self) -> &Vec<PartitionSnapshot> {
        &self.partitions
    }

    pub fn sort_on(&self) -> Option<&Vec<String>> {
        self.sort_on.as_ref()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct PartitionSnapshot {
    pub partition: IdRow<Partition>,
    pub chunks: Vec<IdRow<Chunk>>,
}

impl PartitionSnapshot {
    pub fn partition(&self) -> &IdRow<Partition> {
        &self.partition
    }

    pub fn chunks(&self) -> &Vec<IdRow<Chunk>> {
        &self.chunks
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedLogicalPlan {
    Projection {
        expr: Vec<SerializedExpr>,
        input: Arc<SerializedLogicalPlan>,
        schema: DFSchemaRef,
    },
    Filter {
        predicate: SerializedExpr,
        input: Arc<SerializedLogicalPlan>,
    },
    Aggregate {
        input: Arc<SerializedLogicalPlan>,
        group_expr: Vec<SerializedExpr>,
        aggr_expr: Vec<SerializedExpr>,
        schema: DFSchemaRef,
    },
    Sort {
        expr: Vec<SerializedExpr>,
        input: Arc<SerializedLogicalPlan>,
    },
    Union {
        inputs: Vec<Arc<SerializedLogicalPlan>>,
        schema: DFSchemaRef,
        alias: Option<String>,
    },
    Join {
        left: Arc<SerializedLogicalPlan>,
        right: Arc<SerializedLogicalPlan>,
        on: Vec<(Column, Column)>,
        join_type: JoinType,
        join_constraint: JoinConstraint,
        schema: DFSchemaRef,
    },
    TableScan {
        table_name: String,
        source: SerializedTableSource,
        projection: Option<Vec<usize>>,
        projected_schema: DFSchemaRef,
        filters: Vec<SerializedExpr>,
        alias: Option<String>,
        limit: Option<usize>,
    },
    EmptyRelation {
        produce_one_row: bool,
        schema: DFSchemaRef,
    },
    Limit {
        n: usize,
        input: Arc<SerializedLogicalPlan>,
    },
    Skip {
        n: usize,
        input: Arc<SerializedLogicalPlan>,
    },
    Repartition {
        input: Arc<SerializedLogicalPlan>,
        partitioning_scheme: SerializePartitioning,
    },
    Alias {
        input: Arc<SerializedLogicalPlan>,
        alias: String,
        schema: DFSchemaRef,
    },
    ClusterSend {
        input: Arc<SerializedLogicalPlan>,
        snapshots: Vec<Vec<IndexSnapshot>>,
    },
    ClusterAggregateTopK {
        limit: usize,
        input: Arc<SerializedLogicalPlan>,
        group_expr: Vec<SerializedExpr>,
        aggregate_expr: Vec<SerializedExpr>,
        sort_columns: Vec<SortColumn>,
        schema: DFSchemaRef,
        snapshots: Vec<Vec<IndexSnapshot>>,
    },
    CrossJoin {
        left: Arc<SerializedLogicalPlan>,
        right: Arc<SerializedLogicalPlan>,
        on: SerializedExpr,
        join_schema: DFSchemaRef,
    },
    CrossJoinAgg {
        left: Arc<SerializedLogicalPlan>,
        right: Arc<SerializedLogicalPlan>,
        on: SerializedExpr,
        join_schema: DFSchemaRef,

        group_expr: Vec<SerializedExpr>,
        agg_expr: Vec<SerializedExpr>,
        schema: DFSchemaRef,
    },
    RollingWindowAgg {
        schema: DFSchemaRef,
        input: Arc<SerializedLogicalPlan>,
        dimension: Column,
        partition_by: Vec<Column>,
        from: SerializedExpr,
        to: SerializedExpr,
        every: SerializedExpr,
        rolling_aggs: Vec<SerializedExpr>,
        group_by_dimension: Option<SerializedExpr>,
        aggs: Vec<SerializedExpr>,
    },
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializePartitioning {
    RoundRobinBatch(usize),
    Hash(Vec<SerializedExpr>, usize),
}

impl SerializedLogicalPlan {
    fn logical_plan(
        &self,
        remote_to_local_names: &HashMap<String, String>,
        worker_partition_ids: &HashSet<u64>,
    ) -> Result<LogicalPlan, CubeError> {
        Ok(match self {
            SerializedLogicalPlan::Projection {
                expr,
                input,
                schema,
            } => LogicalPlan::Projection {
                expr: expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
                predicate: predicate.expr(),
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
            },
            SerializedLogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => LogicalPlan::Aggregate {
                group_expr: group_expr.iter().map(|e| e.expr()).collect(),
                aggr_expr: aggr_expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Sort { expr, input } => LogicalPlan::Sort {
                expr: expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
            },
            SerializedLogicalPlan::Union {
                inputs,
                schema,
                alias,
            } => LogicalPlan::Union {
                inputs: inputs
                    .iter()
                    .map(|p| -> Result<LogicalPlan, CubeError> {
                        Ok(p.logical_plan(remote_to_local_names, worker_partition_ids)?)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                schema: schema.clone(),
                alias: alias.clone(),
            },
            SerializedLogicalPlan::TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                alias: _,
                limit,
            } => LogicalPlan::TableScan {
                table_name: table_name.clone(),
                source: match source {
                    SerializedTableSource::CubeTable(v) => Arc::new(v.to_worker_table(
                        remote_to_local_names.clone(),
                        worker_partition_ids.clone(),
                    )),
                },
                projection: projection.clone(),
                projected_schema: projected_schema.clone(),
                filters: filters.iter().map(|e| e.expr()).collect(),
                limit: limit.clone(),
            },
            SerializedLogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => LogicalPlan::EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Limit { n, input } => LogicalPlan::Limit {
                n: *n,
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
            },
            SerializedLogicalPlan::Skip { n, input } => LogicalPlan::Skip {
                n: *n,
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
            },
            SerializedLogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                schema,
            } => LogicalPlan::Join {
                left: Arc::new(left.logical_plan(remote_to_local_names, worker_partition_ids)?),
                right: Arc::new(right.logical_plan(remote_to_local_names, worker_partition_ids)?),
                on: on.clone(),
                join_type: join_type.clone(),
                join_constraint: *join_constraint,
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => LogicalPlan::Repartition {
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
                partitioning_scheme: match partitioning_scheme {
                    SerializePartitioning::RoundRobinBatch(s) => Partitioning::RoundRobinBatch(*s),
                    SerializePartitioning::Hash(e, s) => {
                        Partitioning::Hash(e.iter().map(|e| e.expr()).collect(), *s)
                    }
                },
            },
            SerializedLogicalPlan::Alias {
                input,
                alias,
                schema,
            } => LogicalPlan::Extension {
                node: Arc::new(LogicalAlias {
                    input: input.logical_plan(remote_to_local_names, worker_partition_ids)?,
                    alias: alias.clone(),
                    schema: schema.clone(),
                }),
            },
            SerializedLogicalPlan::ClusterSend { input, snapshots } => ClusterSendNode {
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
                snapshots: snapshots.clone(),
            }
            .into_plan(),
            SerializedLogicalPlan::ClusterAggregateTopK {
                limit,
                input,
                group_expr,
                aggregate_expr,
                sort_columns,
                schema,
                snapshots,
            } => ClusterAggregateTopK {
                limit: *limit,
                input: Arc::new(input.logical_plan(remote_to_local_names, worker_partition_ids)?),
                group_expr: group_expr.iter().map(|e| e.expr()).collect(),
                aggregate_expr: aggregate_expr.iter().map(|e| e.expr()).collect(),
                order_by: sort_columns.clone(),
                schema: schema.clone(),
                snapshots: snapshots.clone(),
            }
            .into_plan(),
            SerializedLogicalPlan::CrossJoin {
                left,
                right,
                on,
                join_schema,
            } => LogicalPlan::Extension {
                node: Arc::new(SkewedLeftCrossJoin {
                    left: left.logical_plan(remote_to_local_names, worker_partition_ids)?,
                    right: right.logical_plan(remote_to_local_names, worker_partition_ids)?,
                    on: on.expr(),
                    schema: join_schema.clone(),
                }),
            },
            SerializedLogicalPlan::CrossJoinAgg {
                left,
                right,
                on,
                join_schema,
                group_expr,
                agg_expr,
                schema,
            } => LogicalPlan::Extension {
                node: Arc::new(CrossJoinAgg {
                    join: SkewedLeftCrossJoin {
                        left: left.logical_plan(remote_to_local_names, worker_partition_ids)?,
                        right: right.logical_plan(remote_to_local_names, worker_partition_ids)?,
                        on: on.expr(),
                        schema: join_schema.clone(),
                    },
                    group_expr: group_expr.iter().map(|e| e.expr()).collect(),
                    agg_expr: agg_expr.iter().map(|e| e.expr()).collect(),
                    schema: schema.clone(),
                }),
            },
            SerializedLogicalPlan::RollingWindowAgg {
                schema,
                input,
                dimension,
                partition_by,
                from,
                to,
                every,
                rolling_aggs,
                group_by_dimension,
                aggs,
            } => LogicalPlan::Extension {
                node: Arc::new(RollingWindowAggregate {
                    schema: schema.clone(),
                    input: input.logical_plan(remote_to_local_names, worker_partition_ids)?,
                    dimension: dimension.clone(),
                    from: from.expr(),
                    to: to.expr(),
                    every: every.expr(),
                    partition_by: partition_by.clone(),
                    rolling_aggs: exprs(&rolling_aggs),
                    group_by_dimension: group_by_dimension.as_ref().map(|d| d.expr()),
                    aggs: exprs(&aggs),
                }),
            },
        })
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedExpr {
    Alias(Box<SerializedExpr>, String),
    Column(String, Option<String>),
    ScalarVariable(Vec<String>),
    Literal(ScalarValue),
    BinaryExpr {
        left: Box<SerializedExpr>,
        op: Operator,
        right: Box<SerializedExpr>,
    },
    Not(Box<SerializedExpr>),
    IsNotNull(Box<SerializedExpr>),
    IsNull(Box<SerializedExpr>),
    Negative(Box<SerializedExpr>),
    Between {
        expr: Box<SerializedExpr>,
        negated: bool,
        low: Box<SerializedExpr>,
        high: Box<SerializedExpr>,
    },
    Case {
        /// Optional base expression that can be compared to literal values in the "when" expressions
        expr: Option<Box<SerializedExpr>>,
        /// One or more when/then expressions
        when_then_expr: Vec<(Box<SerializedExpr>, Box<SerializedExpr>)>,
        /// Optional "else" expression
        else_expr: Option<Box<SerializedExpr>>,
    },
    Cast {
        expr: Box<SerializedExpr>,
        data_type: DataType,
    },
    TryCast {
        expr: Box<SerializedExpr>,
        data_type: DataType,
    },
    Sort {
        expr: Box<SerializedExpr>,
        asc: bool,
        nulls_first: bool,
    },
    ScalarFunction {
        fun: functions::BuiltinScalarFunction,
        args: Vec<SerializedExpr>,
    },
    ScalarUDF {
        fun: CubeScalarUDFKind,
        args: Vec<SerializedExpr>,
    },
    AggregateFunction {
        fun: aggregates::AggregateFunction,
        args: Vec<SerializedExpr>,
        distinct: bool,
    },
    AggregateUDF {
        fun: CubeAggregateUDFKind,
        args: Vec<SerializedExpr>,
    },
    RollingAggregate {
        agg: Box<SerializedExpr>,
        start: WindowFrameBound,
        end: WindowFrameBound,
        offset_to_end: bool,
    },
    InList {
        expr: Box<SerializedExpr>,
        list: Vec<SerializedExpr>,
        negated: bool,
    },
    Wildcard,
}

impl SerializedExpr {
    fn expr(&self) -> Expr {
        match self {
            SerializedExpr::Alias(e, a) => Expr::Alias(Box::new(e.expr()), a.to_string()),
            SerializedExpr::Column(c, a) => Expr::Column(Column {
                name: c.clone(),
                relation: a.clone(),
            }),
            SerializedExpr::ScalarVariable(v) => Expr::ScalarVariable(v.clone()),
            SerializedExpr::Literal(v) => Expr::Literal(v.clone()),
            SerializedExpr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
                left: Box::new(left.expr()),
                op: op.clone(),
                right: Box::new(right.expr()),
            },
            SerializedExpr::Not(e) => Expr::Not(Box::new(e.expr())),
            SerializedExpr::IsNotNull(e) => Expr::IsNotNull(Box::new(e.expr())),
            SerializedExpr::IsNull(e) => Expr::IsNull(Box::new(e.expr())),
            SerializedExpr::Cast { expr, data_type } => Expr::Cast {
                expr: Box::new(expr.expr()),
                data_type: data_type.clone(),
            },
            SerializedExpr::TryCast { expr, data_type } => Expr::TryCast {
                expr: Box::new(expr.expr()),
                data_type: data_type.clone(),
            },
            SerializedExpr::Sort {
                expr,
                asc,
                nulls_first,
            } => Expr::Sort {
                expr: Box::new(expr.expr()),
                asc: *asc,
                nulls_first: *nulls_first,
            },
            SerializedExpr::ScalarFunction { fun, args } => Expr::ScalarFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| e.expr()).collect(),
            },
            SerializedExpr::ScalarUDF { fun, args } => Expr::ScalarUDF {
                fun: Arc::new(scalar_udf_by_kind(*fun).descriptor()),
                args: args.iter().map(|e| e.expr()).collect(),
            },
            SerializedExpr::AggregateFunction {
                fun,
                args,
                distinct,
            } => Expr::AggregateFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| e.expr()).collect(),
                distinct: *distinct,
            },
            SerializedExpr::AggregateUDF { fun, args } => Expr::AggregateUDF {
                fun: Arc::new(aggregate_udf_by_kind(*fun).descriptor()),
                args: args.iter().map(|e| e.expr()).collect(),
            },
            SerializedExpr::Case {
                expr,
                else_expr,
                when_then_expr,
            } => Expr::Case {
                expr: expr.as_ref().map(|e| Box::new(e.expr())),
                else_expr: else_expr.as_ref().map(|e| Box::new(e.expr())),
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(w, t)| (Box::new(w.expr()), Box::new(t.expr())))
                    .collect(),
            },
            SerializedExpr::Wildcard => Expr::Wildcard,
            SerializedExpr::Negative(value) => Expr::Negative(Box::new(value.expr())),
            SerializedExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(expr.expr()),
                negated: *negated,
                low: Box::new(low.expr()),
                high: Box::new(high.expr()),
            },
            SerializedExpr::RollingAggregate {
                agg,
                start,
                end,
                offset_to_end,
            } => Expr::RollingAggregate {
                agg: Box::new(agg.expr()),
                start: start.clone(),
                end: end.clone(),
                offset: match offset_to_end {
                    false => RollingOffset::Start,
                    true => RollingOffset::End,
                },
            },
            SerializedExpr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(expr.expr()),
                list: list.iter().map(|e| e.expr()).collect(),
                negated: *negated,
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedTableSource {
    CubeTable(CubeTable),
}

impl SerializedPlan {
    pub async fn try_new(
        plan: LogicalPlan,
        index_snapshots: Vec<IndexSnapshot>,
    ) -> Result<Self, CubeError> {
        let serialized_logical_plan = Self::serialized_logical_plan(&plan);
        Ok(SerializedPlan {
            logical_plan: Arc::new(serialized_logical_plan),
            schema_snapshot: Arc::new(SchemaSnapshot { index_snapshots }),
            partition_ids_to_execute: HashSet::new(),
        })
    }

    pub fn with_partition_id_to_execute(&self, partition_ids_to_execute: HashSet<u64>) -> Self {
        Self {
            logical_plan: self.logical_plan.clone(),
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute,
        }
    }

    pub fn partition_ids_to_execute(&self) -> HashSet<u64> {
        self.partition_ids_to_execute.clone()
    }

    pub fn logical_plan(
        &self,
        remote_to_local_names: &HashMap<String, String>,
    ) -> Result<LogicalPlan, CubeError> {
        self.logical_plan
            .logical_plan(remote_to_local_names, &self.partition_ids_to_execute())
    }

    pub fn index_snapshots(&self) -> &Vec<IndexSnapshot> {
        &self.schema_snapshot.index_snapshots
    }

    pub fn files_to_download(&self) -> Vec<String> {
        self.list_files_to_download(|id| self.partition_ids_to_execute.contains(&id))
    }

    /// Note: avoid during normal execution, workers must filter the partitions they execute.
    pub fn all_required_files(&self) -> Vec<String> {
        self.list_files_to_download(|_| true)
    }

    fn list_files_to_download(&self, include_partition: impl Fn(u64) -> bool) -> Vec<String> {
        let indexes = self.index_snapshots();

        let mut files = Vec::new();

        for index in indexes.iter() {
            for partition in index.partitions() {
                if !include_partition(partition.partition.get_id()) {
                    continue;
                }
                if let Some(file) = partition
                    .partition
                    .get_row()
                    .get_full_name(partition.partition.get_id())
                {
                    files.push(file);
                }

                for chunk in partition.chunks() {
                    files.push(chunk.get_row().get_full_name(chunk.get_id()))
                }
            }
        }

        files
    }

    pub fn is_data_select_query(plan: &LogicalPlan) -> bool {
        struct Visitor {
            seen_data_scans: bool,
        }
        impl PlanVisitor for Visitor {
            type Error = ();

            fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
                if let LogicalPlan::TableScan { table_name, .. } = plan {
                    let name_split = table_name.split(".").collect::<Vec<_>>();
                    if name_split[0].to_string() != "information_schema" {
                        self.seen_data_scans = true;
                        return Ok(false);
                    }
                }
                Ok(true)
            }
        }

        let mut v = Visitor {
            seen_data_scans: false,
        };
        plan.accept(&mut v).expect("no failures possible");
        return v.seen_data_scans;
    }

    fn serialized_logical_plan(plan: &LogicalPlan) -> SerializedLogicalPlan {
        match plan {
            LogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => SerializedLogicalPlan::EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: schema.clone(),
            },
            LogicalPlan::TableScan {
                table_name,
                source,
                projected_schema,
                projection,
                filters,
                limit,
            } => SerializedLogicalPlan::TableScan {
                table_name: table_name.clone(),
                source: if let Some(cube_table) = source.as_any().downcast_ref::<CubeTable>() {
                    SerializedTableSource::CubeTable(cube_table.clone())
                } else {
                    panic!("Unexpected table source");
                },
                alias: None,
                projected_schema: projected_schema.clone(),
                projection: projection.clone(),
                filters: filters.iter().map(|e| Self::serialized_expr(e)).collect(),
                limit: limit.clone(),
            },
            LogicalPlan::Projection {
                input,
                expr,
                schema,
            } => SerializedLogicalPlan::Projection {
                input: Arc::new(Self::serialized_logical_plan(input)),
                expr: expr.iter().map(|e| Self::serialized_expr(e)).collect(),
                schema: schema.clone(),
            },
            LogicalPlan::Filter { predicate, input } => SerializedLogicalPlan::Filter {
                input: Arc::new(Self::serialized_logical_plan(input)),
                predicate: Self::serialized_expr(predicate),
            },
            LogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => SerializedLogicalPlan::Aggregate {
                input: Arc::new(Self::serialized_logical_plan(input)),
                group_expr: group_expr
                    .iter()
                    .map(|e| Self::serialized_expr(e))
                    .collect(),
                aggr_expr: aggr_expr.iter().map(|e| Self::serialized_expr(e)).collect(),
                schema: schema.clone(),
            },
            LogicalPlan::Sort { expr, input } => SerializedLogicalPlan::Sort {
                input: Arc::new(Self::serialized_logical_plan(input)),
                expr: expr.iter().map(|e| Self::serialized_expr(e)).collect(),
            },
            LogicalPlan::Limit { n, input } => SerializedLogicalPlan::Limit {
                input: Arc::new(Self::serialized_logical_plan(input)),
                n: *n,
            },
            LogicalPlan::Skip { n, input } => SerializedLogicalPlan::Skip {
                input: Arc::new(Self::serialized_logical_plan(input)),
                n: *n,
            },
            LogicalPlan::CreateExternalTable { .. } => unimplemented!(),
            LogicalPlan::Explain { .. } => unimplemented!(),
            LogicalPlan::Extension { node } => {
                if let Some(cs) = node.as_any().downcast_ref::<ClusterSendNode>() {
                    SerializedLogicalPlan::ClusterSend {
                        input: Arc::new(Self::serialized_logical_plan(&cs.input)),
                        snapshots: cs.snapshots.clone(),
                    }
                } else if let Some(topk) = node.as_any().downcast_ref::<ClusterAggregateTopK>() {
                    SerializedLogicalPlan::ClusterAggregateTopK {
                        limit: topk.limit,
                        input: Arc::new(Self::serialized_logical_plan(&topk.input)),
                        group_expr: topk
                            .group_expr
                            .iter()
                            .map(|e| Self::serialized_expr(e))
                            .collect(),
                        aggregate_expr: topk
                            .aggregate_expr
                            .iter()
                            .map(|e| Self::serialized_expr(e))
                            .collect(),
                        sort_columns: topk.order_by.clone(),
                        schema: topk.schema.clone(),
                        snapshots: topk.snapshots.clone(),
                    }
                } else if let Some(j) = node.as_any().downcast_ref::<CrossJoinAgg>() {
                    SerializedLogicalPlan::CrossJoinAgg {
                        left: Arc::new(Self::serialized_logical_plan(&j.join.left)),
                        right: Arc::new(Self::serialized_logical_plan(&j.join.right)),
                        on: Self::serialized_expr(&j.join.on),
                        join_schema: j.join.schema.clone(),
                        group_expr: Self::exprs(&j.group_expr),
                        agg_expr: Self::exprs(&j.agg_expr),
                        schema: j.schema.clone(),
                    }
                } else if let Some(join) = node.as_any().downcast_ref::<SkewedLeftCrossJoin>() {
                    SerializedLogicalPlan::CrossJoin {
                        left: Arc::new(Self::serialized_logical_plan(&join.left)),
                        right: Arc::new(Self::serialized_logical_plan(&join.right)),
                        on: Self::serialized_expr(&join.on),
                        join_schema: join.schema.clone(),
                    }
                } else if let Some(alias) = node.as_any().downcast_ref::<LogicalAlias>() {
                    SerializedLogicalPlan::Alias {
                        input: Arc::new(Self::serialized_logical_plan(&alias.input)),
                        alias: alias.alias.clone(),
                        schema: alias.schema.clone(),
                    }
                } else if let Some(r) = node.as_any().downcast_ref::<RollingWindowAggregate>() {
                    SerializedLogicalPlan::RollingWindowAgg {
                        schema: r.schema.clone(),
                        input: Arc::new(Self::serialized_logical_plan(&r.input)),
                        dimension: r.dimension.clone(),
                        partition_by: r.partition_by.clone(),
                        from: Self::serialized_expr(&r.from),
                        to: Self::serialized_expr(&r.to),
                        every: Self::serialized_expr(&r.every),
                        rolling_aggs: Self::serialized_exprs(&r.rolling_aggs),
                        group_by_dimension: r
                            .group_by_dimension
                            .as_ref()
                            .map(|d| Self::serialized_expr(d)),
                        aggs: Self::serialized_exprs(&r.aggs),
                    }
                } else {
                    panic!("unknown extension");
                }
            }
            LogicalPlan::Union {
                inputs,
                schema,
                alias,
            } => SerializedLogicalPlan::Union {
                inputs: inputs
                    .iter()
                    .map(|input| Arc::new(Self::serialized_logical_plan(&input)))
                    .collect::<Vec<_>>(),
                schema: schema.clone(),
                alias: alias.clone(),
            },
            LogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                schema,
            } => SerializedLogicalPlan::Join {
                left: Arc::new(Self::serialized_logical_plan(&left)),
                right: Arc::new(Self::serialized_logical_plan(&right)),
                on: on.clone(),
                join_type: join_type.clone(),
                join_constraint: *join_constraint,
                schema: schema.clone(),
            },
            LogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => SerializedLogicalPlan::Repartition {
                input: Arc::new(Self::serialized_logical_plan(&input)),
                partitioning_scheme: match partitioning_scheme {
                    Partitioning::RoundRobinBatch(s) => SerializePartitioning::RoundRobinBatch(*s),
                    Partitioning::Hash(e, s) => SerializePartitioning::Hash(
                        e.iter().map(|e| Self::serialized_expr(e)).collect(),
                        *s,
                    ),
                },
            },
            LogicalPlan::Window { .. } | LogicalPlan::CrossJoin { .. } => {
                panic!("unsupported plan node")
            }
        }
    }

    fn exprs<'a>(es: impl IntoIterator<Item = &'a Expr>) -> Vec<SerializedExpr> {
        es.into_iter().map(|e| Self::serialized_expr(e)).collect()
    }

    fn serialized_expr(expr: &Expr) -> SerializedExpr {
        match expr {
            Expr::Alias(expr, alias) => {
                SerializedExpr::Alias(Box::new(Self::serialized_expr(expr)), alias.to_string())
            }
            Expr::Column(c) => SerializedExpr::Column(c.name.clone(), c.relation.clone()),
            Expr::ScalarVariable(v) => SerializedExpr::ScalarVariable(v.clone()),
            Expr::Literal(v) => SerializedExpr::Literal(v.clone()),
            Expr::BinaryExpr { left, op, right } => SerializedExpr::BinaryExpr {
                left: Box::new(Self::serialized_expr(left)),
                op: op.clone(),
                right: Box::new(Self::serialized_expr(right)),
            },
            Expr::Not(e) => SerializedExpr::Not(Box::new(Self::serialized_expr(&e))),
            Expr::IsNotNull(e) => SerializedExpr::IsNotNull(Box::new(Self::serialized_expr(&e))),
            Expr::IsNull(e) => SerializedExpr::IsNull(Box::new(Self::serialized_expr(&e))),
            Expr::Cast { expr, data_type } => SerializedExpr::Cast {
                expr: Box::new(Self::serialized_expr(&expr)),
                data_type: data_type.clone(),
            },
            Expr::TryCast { expr, data_type } => SerializedExpr::TryCast {
                expr: Box::new(Self::serialized_expr(&expr)),
                data_type: data_type.clone(),
            },
            Expr::Sort {
                expr,
                asc,
                nulls_first,
            } => SerializedExpr::Sort {
                expr: Box::new(Self::serialized_expr(&expr)),
                asc: *asc,
                nulls_first: *nulls_first,
            },
            Expr::ScalarFunction { fun, args } => SerializedExpr::ScalarFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| Self::serialized_expr(&e)).collect(),
            },
            Expr::ScalarUDF { fun, args } => SerializedExpr::ScalarUDF {
                fun: scalar_kind_by_name(&fun.name).unwrap(),
                args: args.iter().map(|e| Self::serialized_expr(&e)).collect(),
            },
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
            } => SerializedExpr::AggregateFunction {
                fun: fun.clone(),
                args: args.iter().map(|e| Self::serialized_expr(&e)).collect(),
                distinct: *distinct,
            },
            Expr::AggregateUDF { fun, args } => SerializedExpr::AggregateUDF {
                fun: aggregate_kind_by_name(&fun.name).unwrap(),
                args: args.iter().map(|e| Self::serialized_expr(&e)).collect(),
            },
            Expr::Case {
                expr,
                when_then_expr,
                else_expr,
            } => SerializedExpr::Case {
                expr: expr.as_ref().map(|e| Box::new(Self::serialized_expr(&e))),
                else_expr: else_expr
                    .as_ref()
                    .map(|e| Box::new(Self::serialized_expr(&e))),
                when_then_expr: when_then_expr
                    .iter()
                    .map(|(w, t)| {
                        (
                            Box::new(Self::serialized_expr(&w)),
                            Box::new(Self::serialized_expr(&t)),
                        )
                    })
                    .collect(),
            },
            Expr::Wildcard => SerializedExpr::Wildcard,
            Expr::Negative(value) => {
                SerializedExpr::Negative(Box::new(Self::serialized_expr(&value)))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => SerializedExpr::Between {
                expr: Box::new(Self::serialized_expr(&expr)),
                negated: *negated,
                low: Box::new(Self::serialized_expr(&low)),
                high: Box::new(Self::serialized_expr(&high)),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => SerializedExpr::InList {
                expr: Box::new(Self::serialized_expr(&expr)),
                list: list.iter().map(|e| Self::serialized_expr(&e)).collect(),
                negated: *negated,
            },
            Expr::RollingAggregate {
                agg,
                start: start_bound,
                end: end_bound,
                offset,
            } => SerializedExpr::RollingAggregate {
                agg: Box::new(Self::serialized_expr(&agg)),
                start: start_bound.clone(),
                end: end_bound.clone(),
                offset_to_end: match offset {
                    RollingOffset::Start => false,
                    RollingOffset::End => true,
                },
            },
            Expr::WindowFunction { .. } => panic!("window functions are not supported"),
        }
    }

    fn serialized_exprs(e: &[Expr]) -> Vec<SerializedExpr> {
        e.iter().map(|e| Self::serialized_expr(e)).collect()
    }
}

fn exprs(e: &[SerializedExpr]) -> Vec<Expr> {
    e.iter().map(|e| e.expr()).collect()
}
