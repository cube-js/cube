use crate::metastore::table::{Table, TablePath};
use crate::metastore::{Chunk, IdRow, Index, Partition};
use crate::queryplanner::panic::PanicWorkerNode;
use crate::queryplanner::planning::{ClusterSendNode, PlanningMeta, Snapshots};
use crate::queryplanner::query_executor::{CubeTable, InlineTableId, InlineTableProvider};
use crate::queryplanner::topk::{ClusterAggregateTopK, SortColumn};
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{
    aggregate_kind_by_name, scalar_kind_by_name, scalar_udf_by_kind, CubeAggregateUDFKind,
    CubeScalarUDFKind,
};
use crate::table::Row;
use crate::CubeError;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::cube_ext::alias::LogicalAlias;
use datafusion::cube_ext::join::SkewedLeftCrossJoin;
use datafusion::cube_ext::joinagg::CrossJoinAgg;
use datafusion::cube_ext::rolling::RollingWindowAggregate;
use datafusion::logical_plan::window_frames::WindowFrameBound;
use datafusion::logical_plan::{
    Column, DFSchemaRef, Expr, JoinConstraint, JoinType, LogicalPlan, Operator, Partitioning,
    PlanVisitor,
};
use datafusion::physical_plan::parquet::ParquetMetadataCache;
use datafusion::physical_plan::{aggregates, functions};
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
use sqlparser::ast::RollingOffset;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize, Debug, Default, Eq, PartialEq)]
pub struct RowRange {
    /// Inclusive lower bound.
    pub start: Option<Row>,
    /// Exclusive upper bound.
    pub end: Option<Row>,
}

impl RowRange {
    pub fn matches_all_rows(&self) -> bool {
        self.start.is_none() && self.end.is_none()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Default)]
pub struct RowFilter {
    pub or_filters: Vec<RowRange>,
}

impl RowFilter {
    pub fn append_or(&mut self, r: RowRange) {
        if self.matches_all_rows() {
            return;
        }
        if r.matches_all_rows() {
            self.or_filters.clear();
            self.or_filters.push(r);
        } else {
            self.or_filters.push(r);
        }
    }

    pub fn matches_all_rows(&self) -> bool {
        self.or_filters.len() == 1 && self.or_filters[0].matches_all_rows()
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SerializedPlan {
    logical_plan: Arc<SerializedLogicalPlan>,
    schema_snapshot: Arc<SchemaSnapshot>,
    partition_ids_to_execute: Vec<(u64, RowFilter)>,
    inline_table_ids_to_execute: Vec<InlineTableId>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SchemaSnapshot {
    index_snapshots: PlanningMeta,
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
pub struct InlineSnapshot {
    pub id: u64,
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
        snapshots: Vec<Snapshots>,
        #[serde(default)]
        limit_and_reverse: Option<(usize, bool)>,
    },
    ClusterAggregateTopK {
        limit: usize,
        input: Arc<SerializedLogicalPlan>,
        group_expr: Vec<SerializedExpr>,
        aggregate_expr: Vec<SerializedExpr>,
        sort_columns: Vec<SortColumn>,
        having_expr: Option<SerializedExpr>,
        schema: DFSchemaRef,
        snapshots: Vec<Snapshots>,
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
    Panic {},
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializePartitioning {
    RoundRobinBatch(usize),
    Hash(Vec<SerializedExpr>, usize),
}

pub struct WorkerContext {
    remote_to_local_names: HashMap<String, String>,
    worker_partition_ids: Vec<(u64, RowFilter)>,
    inline_table_ids_to_execute: Vec<InlineTableId>,
    chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    parquet_metadata_cache: Arc<dyn ParquetMetadataCache>,
}

impl SerializedLogicalPlan {
    fn logical_plan(&self, worker_context: &WorkerContext) -> Result<LogicalPlan, CubeError> {
        debug_assert!(worker_context
            .worker_partition_ids
            .iter()
            .is_sorted_by_key(|(id, _)| id));
        Ok(match self {
            SerializedLogicalPlan::Projection {
                expr,
                input,
                schema,
            } => LogicalPlan::Projection {
                expr: expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan(worker_context)?),
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
                predicate: predicate.expr(),
                input: Arc::new(input.logical_plan(worker_context)?),
            },
            SerializedLogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => LogicalPlan::Aggregate {
                group_expr: group_expr.iter().map(|e| e.expr()).collect(),
                aggr_expr: aggr_expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan(worker_context)?),
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Sort { expr, input } => LogicalPlan::Sort {
                expr: expr.iter().map(|e| e.expr()).collect(),
                input: Arc::new(input.logical_plan(worker_context)?),
            },
            SerializedLogicalPlan::Union {
                inputs,
                schema,
                alias,
            } => LogicalPlan::Union {
                inputs: inputs
                    .iter()
                    .map(|p| -> Result<LogicalPlan, CubeError> {
                        Ok(p.logical_plan(worker_context)?)
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
                        worker_context.remote_to_local_names.clone(),
                        worker_context.worker_partition_ids.clone(),
                        worker_context.chunk_id_to_record_batches.clone(),
                        worker_context.parquet_metadata_cache.clone(),
                    )),
                    SerializedTableSource::InlineTable(v) => Arc::new(
                        v.to_worker_table(worker_context.inline_table_ids_to_execute.clone()),
                    ),
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
                input: Arc::new(input.logical_plan(worker_context)?),
            },
            SerializedLogicalPlan::Skip { n, input } => LogicalPlan::Skip {
                n: *n,
                input: Arc::new(input.logical_plan(worker_context)?),
            },
            SerializedLogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                schema,
            } => LogicalPlan::Join {
                left: Arc::new(left.logical_plan(worker_context)?),
                right: Arc::new(right.logical_plan(worker_context)?),
                on: on.clone(),
                join_type: join_type.clone(),
                join_constraint: *join_constraint,
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => LogicalPlan::Repartition {
                input: Arc::new(input.logical_plan(worker_context)?),
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
                    input: input.logical_plan(worker_context)?,
                    alias: alias.clone(),
                    schema: schema.clone(),
                }),
            },
            SerializedLogicalPlan::ClusterSend {
                input,
                snapshots,
                limit_and_reverse,
            } => ClusterSendNode {
                input: Arc::new(input.logical_plan(worker_context)?),
                snapshots: snapshots.clone(),
                limit_and_reverse: limit_and_reverse.clone(),
            }
            .into_plan(),
            SerializedLogicalPlan::ClusterAggregateTopK {
                limit,
                input,
                group_expr,
                aggregate_expr,
                sort_columns,
                having_expr,
                schema,
                snapshots,
            } => ClusterAggregateTopK {
                limit: *limit,
                input: Arc::new(input.logical_plan(worker_context)?),
                group_expr: group_expr.iter().map(|e| e.expr()).collect(),
                aggregate_expr: aggregate_expr.iter().map(|e| e.expr()).collect(),
                order_by: sort_columns.clone(),
                having_expr: having_expr.as_ref().map(|e| e.expr()),
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
                    left: left.logical_plan(worker_context)?,
                    right: right.logical_plan(worker_context)?,
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
                        left: left.logical_plan(worker_context)?,
                        right: right.logical_plan(worker_context)?,
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
                    input: input.logical_plan(worker_context)?,
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
            SerializedLogicalPlan::Panic {} => LogicalPlan::Extension {
                node: Arc::new(PanicWorkerNode {}),
            },
        })
    }
    fn is_empty_relation(&self) -> Option<DFSchemaRef> {
        match self {
            SerializedLogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => {
                if !produce_one_row {
                    Some(schema.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn remove_unused_tables(
        &self,
        partition_ids_to_execute: &Vec<(u64, RowFilter)>,
        inline_tables_to_execute: &Vec<InlineTableId>,
    ) -> SerializedLogicalPlan {
        debug_assert!(partition_ids_to_execute
            .iter()
            .is_sorted_by_key(|(id, _)| id));
        match self {
            SerializedLogicalPlan::Projection {
                expr,
                input,
                schema,
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                if input.is_empty_relation().is_some() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Projection {
                        expr: expr.clone(),
                        input: Arc::new(input),
                        schema: schema.clone(),
                    }
                }
            }
            SerializedLogicalPlan::Filter { predicate, input } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                if let Some(schema) = input.is_empty_relation() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Filter {
                        predicate: predicate.clone(),
                        input: Arc::new(input),
                    }
                }
            }
            SerializedLogicalPlan::Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                SerializedLogicalPlan::Aggregate {
                    input: Arc::new(input),
                    group_expr: group_expr.clone(),
                    aggr_expr: aggr_expr.clone(),
                    schema: schema.clone(),
                }
            }
            SerializedLogicalPlan::Sort { expr, input } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                if let Some(schema) = input.is_empty_relation() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Sort {
                        expr: expr.clone(),
                        input: Arc::new(input),
                    }
                }
            }
            SerializedLogicalPlan::Union {
                inputs,
                schema,
                alias,
            } => {
                let inputs = inputs
                    .iter()
                    .filter_map(|i| {
                        let i = i.remove_unused_tables(
                            partition_ids_to_execute,
                            inline_tables_to_execute,
                        );
                        if i.is_empty_relation().is_some() {
                            None
                        } else {
                            Some(Arc::new(i))
                        }
                    })
                    .collect::<Vec<_>>();

                SerializedLogicalPlan::Union {
                    inputs,
                    schema: schema.clone(),
                    alias: alias.clone(),
                }
            }
            SerializedLogicalPlan::TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                alias,
                limit,
            } => {
                let is_empty = match source {
                    SerializedTableSource::CubeTable(table) => {
                        !table.has_partitions(partition_ids_to_execute)
                    }
                    SerializedTableSource::InlineTable(table) => {
                        !table.has_inline_table_id(inline_tables_to_execute)
                    }
                };
                if is_empty {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: projected_schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::TableScan {
                        table_name: table_name.clone(),
                        source: source.clone(),
                        projection: projection.clone(),
                        projected_schema: projected_schema.clone(),
                        filters: filters.clone(),
                        alias: alias.clone(),
                        limit: limit.clone(),
                    }
                }
            }
            SerializedLogicalPlan::EmptyRelation {
                produce_one_row,
                schema,
            } => SerializedLogicalPlan::EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: schema.clone(),
            },
            SerializedLogicalPlan::Limit { n, input } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                if let Some(schema) = input.is_empty_relation() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Limit {
                        n: *n,
                        input: Arc::new(input),
                    }
                }
            }
            SerializedLogicalPlan::Skip { n, input } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                if let Some(schema) = input.is_empty_relation() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Skip {
                        n: *n,
                        input: Arc::new(input),
                    }
                }
            }
            SerializedLogicalPlan::Join {
                left,
                right,
                on,
                join_type,
                join_constraint,
                schema,
            } => {
                let left =
                    left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                let right =
                    right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                SerializedLogicalPlan::Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on: on.clone(),
                    join_type: join_type.clone(),
                    join_constraint: *join_constraint,
                    schema: schema.clone(),
                }
            }
            SerializedLogicalPlan::Repartition {
                input,
                partitioning_scheme,
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                if let Some(schema) = input.is_empty_relation() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Repartition {
                        input: Arc::new(input),
                        partitioning_scheme: partitioning_scheme.clone(),
                    }
                }
            }
            SerializedLogicalPlan::Alias {
                input,
                alias,
                schema,
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                if input.is_empty_relation().is_some() {
                    SerializedLogicalPlan::EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }
                } else {
                    SerializedLogicalPlan::Alias {
                        input: Arc::new(input),
                        alias: alias.clone(),
                        schema: schema.clone(),
                    }
                }
            }
            SerializedLogicalPlan::ClusterSend {
                input,
                snapshots,
                limit_and_reverse,
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                SerializedLogicalPlan::ClusterSend {
                    input: Arc::new(input),
                    snapshots: snapshots.clone(),
                    limit_and_reverse: limit_and_reverse.clone(),
                }
            }
            SerializedLogicalPlan::ClusterAggregateTopK {
                limit,
                input,
                group_expr,
                aggregate_expr,
                sort_columns,
                having_expr,
                schema,
                snapshots,
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                SerializedLogicalPlan::ClusterAggregateTopK {
                    limit: *limit,
                    input: Arc::new(input),
                    group_expr: group_expr.clone(),
                    aggregate_expr: aggregate_expr.clone(),
                    sort_columns: sort_columns.clone(),
                    having_expr: having_expr.clone(),
                    schema: schema.clone(),
                    snapshots: snapshots.clone(),
                }
            }
            SerializedLogicalPlan::CrossJoin {
                left,
                right,
                on,
                join_schema,
            } => {
                let left =
                    left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                let right =
                    right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                SerializedLogicalPlan::CrossJoin {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on: on.clone(),
                    join_schema: join_schema.clone(),
                }
            }
            SerializedLogicalPlan::CrossJoinAgg {
                left,
                right,
                on,
                join_schema,
                group_expr,
                agg_expr,
                schema,
            } => {
                let left =
                    left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                let right =
                    right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

                SerializedLogicalPlan::CrossJoinAgg {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on: on.clone(),
                    join_schema: join_schema.clone(),
                    group_expr: group_expr.clone(),
                    agg_expr: agg_expr.clone(),
                    schema: schema.clone(),
                }
            }
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
            } => {
                let input =
                    input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
                SerializedLogicalPlan::RollingWindowAgg {
                    schema: schema.clone(),
                    input: Arc::new(input),
                    dimension: dimension.clone(),
                    partition_by: partition_by.clone(),
                    from: from.clone(),
                    to: to.clone(),
                    every: every.clone(),
                    rolling_aggs: rolling_aggs.clone(),
                    group_by_dimension: group_by_dimension.clone(),
                    aggs: aggs.clone(),
                }
            }
            SerializedLogicalPlan::Panic {} => SerializedLogicalPlan::Panic {},
        }
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
    InlineTable(InlineTableProvider),
}

impl SerializedPlan {
    pub async fn try_new(
        plan: LogicalPlan,
        index_snapshots: PlanningMeta,
    ) -> Result<Self, CubeError> {
        let serialized_logical_plan = Self::serialized_logical_plan(&plan);
        Ok(SerializedPlan {
            logical_plan: Arc::new(serialized_logical_plan),
            schema_snapshot: Arc::new(SchemaSnapshot { index_snapshots }),
            partition_ids_to_execute: Vec::new(),
            inline_table_ids_to_execute: Vec::new(),
        })
    }

    pub fn with_partition_id_to_execute(
        &self,
        partition_ids_to_execute: Vec<(u64, RowFilter)>,
        inline_table_ids_to_execute: Vec<InlineTableId>,
    ) -> Self {
        Self {
            logical_plan: Arc::new(
                self.logical_plan
                    .remove_unused_tables(&partition_ids_to_execute, &inline_table_ids_to_execute),
            ),
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute,
            inline_table_ids_to_execute,
        }
    }

    pub fn logical_plan(
        &self,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        parquet_metadata_cache: Arc<dyn ParquetMetadataCache>,
    ) -> Result<LogicalPlan, CubeError> {
        self.logical_plan.logical_plan(&WorkerContext {
            remote_to_local_names,
            worker_partition_ids: self.partition_ids_to_execute.clone(),
            inline_table_ids_to_execute: self.inline_table_ids_to_execute.clone(),
            chunk_id_to_record_batches,
            parquet_metadata_cache,
        })
    }

    pub fn index_snapshots(&self) -> &Vec<IndexSnapshot> {
        &self.schema_snapshot.index_snapshots.indices
    }

    pub fn planning_meta(&self) -> &PlanningMeta {
        &self.schema_snapshot.index_snapshots
    }

    pub fn files_to_download(&self) -> Vec<(IdRow<Partition>, String, Option<u64>)> {
        self.list_files_to_download(|id| {
            self.partition_ids_to_execute
                .binary_search_by_key(&id, |(id, _)| *id)
                .is_ok()
        })
    }

    /// Note: avoid during normal execution, workers must filter the partitions they execute.
    pub fn all_required_files(&self) -> Vec<(IdRow<Partition>, String, Option<u64>)> {
        self.list_files_to_download(|_| true)
    }

    fn list_files_to_download(
        &self,
        include_partition: impl Fn(u64) -> bool,
    ) -> Vec<(
        IdRow<Partition>,
        /* file_name */ String,
        /* size */ Option<u64>,
    )> {
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
                    files.push((
                        partition.partition.clone(),
                        file,
                        partition.partition.get_row().file_size(),
                    ));
                }

                for chunk in partition.chunks() {
                    if !chunk.get_row().in_memory() {
                        files.push((
                            partition.partition.clone(),
                            chunk.get_row().get_full_name(chunk.get_id()),
                            chunk.get_row().file_size(),
                        ))
                    }
                }
            }
        }

        files
    }

    pub fn in_memory_chunks_to_load(&self) -> Vec<(IdRow<Chunk>, IdRow<Partition>, IdRow<Index>)> {
        self.list_in_memory_chunks_to_load(|id| {
            self.partition_ids_to_execute
                .binary_search_by_key(&id, |(id, _)| *id)
                .is_ok()
        })
    }

    fn list_in_memory_chunks_to_load(
        &self,
        include_partition: impl Fn(u64) -> bool,
    ) -> Vec<(IdRow<Chunk>, IdRow<Partition>, IdRow<Index>)> {
        let indexes = self.index_snapshots();

        let mut chunk_ids = Vec::new();

        for index in indexes.iter() {
            for partition in index.partitions() {
                if !include_partition(partition.partition.get_id()) {
                    continue;
                }

                for chunk in partition.chunks() {
                    if chunk.get_row().in_memory() {
                        chunk_ids.push((
                            chunk.clone(),
                            partition.partition.clone(),
                            index.index.clone(),
                        ));
                    }
                }
            }
        }

        chunk_ids
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
                    if name_split[0] != "information_schema" && name_split[0] != "system" {
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
                } else if let Some(inline_table) =
                    source.as_any().downcast_ref::<InlineTableProvider>()
                {
                    SerializedTableSource::InlineTable(inline_table.clone())
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
                        limit_and_reverse: cs.limit_and_reverse.clone(),
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
                        having_expr: topk.having_expr.as_ref().map(|e| Self::serialized_expr(&e)),
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
                } else if let Some(_) = node.as_any().downcast_ref::<PanicWorkerNode>() {
                    SerializedLogicalPlan::Panic {}
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
