use crate::metastore::table::{Table, TablePath};
use crate::metastore::{Chunk, IdRow, Index, Partition};
use crate::queryplanner::panic::PanicWorkerNode;
use crate::queryplanner::planning::{
    ClusterSendNode, ExtensionNodeSerialized, PlanningMeta, Snapshots,
};
use crate::queryplanner::providers::InfoSchemaQueryCacheTableProvider;
use crate::queryplanner::query_executor::{CubeTable, InlineTableId, InlineTableProvider};
use crate::queryplanner::topk::{ClusterAggregateTopK, SortColumn};
use crate::queryplanner::udfs::aggregate_udf_by_kind;
use crate::queryplanner::udfs::{
    aggregate_kind_by_name, scalar_kind_by_name, scalar_udf_by_kind, CubeAggregateUDFKind,
    CubeScalarUDFKind,
};
use crate::queryplanner::{CubeTableLogical, InfoSchemaTableProvider};
use crate::table::Row;
use crate::CubeError;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::aggregates;
use datafusion::scalar::ScalarValue;
use serde_derive::{Deserialize, Serialize};
//TODO
// use sqlparser::ast::RollingOffset;
use bytes::Bytes;
use datafusion::catalog::TableProvider;
use datafusion::catalog_common::TableReference;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::{Column, DFSchemaRef, JoinConstraint, JoinType};
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, TableScan};
use datafusion::prelude::SessionContext;
use datafusion_proto::bytes::{
    logical_plan_from_bytes, logical_plan_from_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use flexbuffers::FlexbufferSerializer;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
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
    logical_plan: Arc<Vec<u8>>,
    schema_snapshot: Arc<SchemaSnapshot>,
    partition_ids_to_execute: Vec<(u64, RowFilter)>,
    inline_table_ids_to_execute: Vec<InlineTableId>,
    trace_obj: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SchemaSnapshot {
    index_snapshots: PlanningMeta,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash)]
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

#[derive(Clone, Serialize, Deserialize, Debug, Hash, PartialEq, Eq)]
pub struct InlineSnapshot {
    pub id: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SerializedLogicalPlan {
    serialized_bytes: Arc<Vec<u8>>,
    // TODO upgrade DF
    // Projection {
    //     expr: Vec<SerializedExpr>,
    //     input: Arc<SerializedLogicalPlan>,
    //     schema: DFSchemaRef,
    // },
    // Filter {
    //     predicate: SerializedExpr,
    //     input: Arc<SerializedLogicalPlan>,
    // },
    // Aggregate {
    //     input: Arc<SerializedLogicalPlan>,
    //     group_expr: Vec<SerializedExpr>,
    //     aggr_expr: Vec<SerializedExpr>,
    //     schema: DFSchemaRef,
    // },
    // Sort {
    //     expr: Vec<SerializedExpr>,
    //     input: Arc<SerializedLogicalPlan>,
    // },
    // Union {
    //     inputs: Vec<Arc<SerializedLogicalPlan>>,
    //     schema: DFSchemaRef,
    //     alias: Option<String>,
    // },
    // Join {
    //     left: Arc<SerializedLogicalPlan>,
    //     right: Arc<SerializedLogicalPlan>,
    //     on: Vec<(Column, Column)>,
    //     join_type: JoinType,
    //     join_constraint: JoinConstraint,
    //     schema: DFSchemaRef,
    // },
    // TableScan {
    //     table_name: String,
    //     source: SerializedTableSource,
    //     projection: Option<Vec<usize>>,
    //     projected_schema: DFSchemaRef,
    //     filters: Vec<SerializedExpr>,
    //     alias: Option<String>,
    //     limit: Option<usize>,
    // },
    // EmptyRelation {
    //     produce_one_row: bool,
    //     schema: DFSchemaRef,
    // },
    // Limit {
    //     n: usize,
    //     input: Arc<SerializedLogicalPlan>,
    // },
    // Skip {
    //     n: usize,
    //     input: Arc<SerializedLogicalPlan>,
    // },
    // Repartition {
    //     input: Arc<SerializedLogicalPlan>,
    //     partitioning_scheme: SerializePartitioning,
    // },
    // Alias {
    //     input: Arc<SerializedLogicalPlan>,
    //     alias: String,
    //     schema: DFSchemaRef,
    // },
    // ClusterSend {
    //     input: Arc<SerializedLogicalPlan>,
    //     snapshots: Vec<Snapshots>,
    //     #[serde(default)]
    //     limit_and_reverse: Option<(usize, bool)>,
    // },
    // ClusterAggregateTopK {
    //     limit: usize,
    //     input: Arc<SerializedLogicalPlan>,
    //     group_expr: Vec<SerializedExpr>,
    //     aggregate_expr: Vec<SerializedExpr>,
    //     sort_columns: Vec<SortColumn>,
    //     having_expr: Option<SerializedExpr>,
    //     schema: DFSchemaRef,
    //     snapshots: Vec<Snapshots>,
    // },
    // CrossJoin {
    //     left: Arc<SerializedLogicalPlan>,
    //     right: Arc<SerializedLogicalPlan>,
    //     on: SerializedExpr,
    //     join_schema: DFSchemaRef,
    // },
    // CrossJoinAgg {
    //     left: Arc<SerializedLogicalPlan>,
    //     right: Arc<SerializedLogicalPlan>,
    //     on: SerializedExpr,
    //     join_schema: DFSchemaRef,
    //
    //     group_expr: Vec<SerializedExpr>,
    //     agg_expr: Vec<SerializedExpr>,
    //     schema: DFSchemaRef,
    // },
    // RollingWindowAgg {
    //     schema: DFSchemaRef,
    //     input: Arc<SerializedLogicalPlan>,
    //     dimension: Column,
    //     partition_by: Vec<Column>,
    //     from: SerializedExpr,
    //     to: SerializedExpr,
    //     every: SerializedExpr,
    //     rolling_aggs: Vec<SerializedExpr>,
    //     group_by_dimension: Option<SerializedExpr>,
    //     aggs: Vec<SerializedExpr>,
    // },
    // Panic {},
}

// #[derive(Clone, Serialize, Deserialize, Debug)]
// pub enum SerializePartitioning {
//     RoundRobinBatch(usize),
//     Hash(Vec<SerializedExpr>, usize),
// }

pub struct WorkerContext {
    remote_to_local_names: HashMap<String, String>,
    worker_partition_ids: Vec<(u64, RowFilter)>,
    inline_table_ids_to_execute: Vec<InlineTableId>,
    chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
}

// TODO upgrade DF
// impl SerializedLogicalPlan {
//     fn logical_plan(&self, worker_context: &WorkerContext) -> Result<LogicalPlan, CubeError> {
//         debug_assert!(worker_context
//             .worker_partition_ids
//             .iter()
//             .is_sorted_by_key(|(id, _)| id));
//         Ok(match self {
//             SerializedLogicalPlan::Projection {
//                 expr,
//                 input,
//                 schema,
//             } => LogicalPlan::Projection {
//                 expr: expr.iter().map(|e| e.expr()).collect(),
//                 input: Arc::new(input.logical_plan(worker_context)?),
//                 schema: schema.clone(),
//             },
//             SerializedLogicalPlan::Filter { predicate, input } => LogicalPlan::Filter {
//                 predicate: predicate.expr(),
//                 input: Arc::new(input.logical_plan(worker_context)?),
//             },
//             SerializedLogicalPlan::Aggregate {
//                 input,
//                 group_expr,
//                 aggr_expr,
//                 schema,
//             } => LogicalPlan::Aggregate {
//                 group_expr: group_expr.iter().map(|e| e.expr()).collect(),
//                 aggr_expr: aggr_expr.iter().map(|e| e.expr()).collect(),
//                 input: Arc::new(input.logical_plan(worker_context)?),
//                 schema: schema.clone(),
//             },
//             SerializedLogicalPlan::Sort { expr, input } => LogicalPlan::Sort {
//                 expr: expr.iter().map(|e| e.expr()).collect(),
//                 input: Arc::new(input.logical_plan(worker_context)?),
//             },
//             SerializedLogicalPlan::Union {
//                 inputs,
//                 schema,
//                 alias,
//             } => LogicalPlan::Union {
//                 inputs: inputs
//                     .iter()
//                     .map(|p| -> Result<LogicalPlan, CubeError> {
//                         Ok(p.logical_plan(worker_context)?)
//                     })
//                     .collect::<Result<Vec<_>, _>>()?,
//                 schema: schema.clone(),
//                 alias: alias.clone(),
//             },
//             SerializedLogicalPlan::TableScan {
//                 table_name,
//                 source,
//                 projection,
//                 projected_schema,
//                 filters,
//                 alias: _,
//                 limit,
//             } => LogicalPlan::TableScan {
//                 table_name: table_name.clone(),
//                 source: match source {
//                     SerializedTableSource::CubeTable(v) => Arc::new(v.to_worker_table(
//                         worker_context.remote_to_local_names.clone(),
//                         worker_context.worker_partition_ids.clone(),
//                         worker_context.chunk_id_to_record_batches.clone(),
//                         worker_context.parquet_metadata_cache.clone(),
//                     )),
//                     SerializedTableSource::InlineTable(v) => Arc::new(
//                         v.to_worker_table(worker_context.inline_table_ids_to_execute.clone()),
//                     ),
//                 },
//                 projection: projection.clone(),
//                 projected_schema: projected_schema.clone(),
//                 filters: filters.iter().map(|e| e.expr()).collect(),
//                 limit: limit.clone(),
//             },
//             SerializedLogicalPlan::EmptyRelation {
//                 produce_one_row,
//                 schema,
//             } => LogicalPlan::EmptyRelation {
//                 produce_one_row: *produce_one_row,
//                 schema: schema.clone(),
//             },
//             SerializedLogicalPlan::Limit { n, input } => LogicalPlan::Limit {
//                 n: *n,
//                 input: Arc::new(input.logical_plan(worker_context)?),
//             },
//             SerializedLogicalPlan::Skip { n, input } => LogicalPlan::Skip {
//                 n: *n,
//                 input: Arc::new(input.logical_plan(worker_context)?),
//             },
//             SerializedLogicalPlan::Join {
//                 left,
//                 right,
//                 on,
//                 join_type,
//                 join_constraint,
//                 schema,
//             } => LogicalPlan::Join {
//                 left: Arc::new(left.logical_plan(worker_context)?),
//                 right: Arc::new(right.logical_plan(worker_context)?),
//                 on: on.clone(),
//                 join_type: join_type.clone(),
//                 join_constraint: *join_constraint,
//                 schema: schema.clone(),
//             },
//             SerializedLogicalPlan::Repartition {
//                 input,
//                 partitioning_scheme,
//             } => LogicalPlan::Repartition {
//                 input: Arc::new(input.logical_plan(worker_context)?),
//                 partitioning_scheme: match partitioning_scheme {
//                     SerializePartitioning::RoundRobinBatch(s) => Partitioning::RoundRobinBatch(*s),
//                     SerializePartitioning::Hash(e, s) => {
//                         Partitioning::Hash(e.iter().map(|e| e.expr()).collect(), *s)
//                     }
//                 },
//             },
//             SerializedLogicalPlan::Alias {
//                 input,
//                 alias,
//                 schema,
//             } => LogicalPlan::Extension {
//                 node: Arc::new(LogicalAlias {
//                     input: input.logical_plan(worker_context)?,
//                     alias: alias.clone(),
//                     schema: schema.clone(),
//                 }),
//             },
//             SerializedLogicalPlan::ClusterSend {
//                 input,
//                 snapshots,
//                 limit_and_reverse,
//             } => ClusterSendNode {
//                 input: Arc::new(input.logical_plan(worker_context)?),
//                 snapshots: snapshots.clone(),
//                 limit_and_reverse: limit_and_reverse.clone(),
//             }
//             .into_plan(),
//             SerializedLogicalPlan::ClusterAggregateTopK {
//                 limit,
//                 input,
//                 group_expr,
//                 aggregate_expr,
//                 sort_columns,
//                 having_expr,
//                 schema,
//                 snapshots,
//             } => ClusterAggregateTopK {
//                 limit: *limit,
//                 input: Arc::new(input.logical_plan(worker_context)?),
//                 group_expr: group_expr.iter().map(|e| e.expr()).collect(),
//                 aggregate_expr: aggregate_expr.iter().map(|e| e.expr()).collect(),
//                 order_by: sort_columns.clone(),
//                 having_expr: having_expr.as_ref().map(|e| e.expr()),
//                 schema: schema.clone(),
//                 snapshots: snapshots.clone(),
//             }
//             .into_plan(),
//             SerializedLogicalPlan::CrossJoin {
//                 left,
//                 right,
//                 on,
//                 join_schema,
//             } => LogicalPlan::Extension {
//                 node: Arc::new(SkewedLeftCrossJoin {
//                     left: left.logical_plan(worker_context)?,
//                     right: right.logical_plan(worker_context)?,
//                     on: on.expr(),
//                     schema: join_schema.clone(),
//                 }),
//             },
//             SerializedLogicalPlan::CrossJoinAgg {
//                 left,
//                 right,
//                 on,
//                 join_schema,
//                 group_expr,
//                 agg_expr,
//                 schema,
//             } => LogicalPlan::Extension {
//                 node: Arc::new(CrossJoinAgg {
//                     join: SkewedLeftCrossJoin {
//                         left: left.logical_plan(worker_context)?,
//                         right: right.logical_plan(worker_context)?,
//                         on: on.expr(),
//                         schema: join_schema.clone(),
//                     },
//                     group_expr: group_expr.iter().map(|e| e.expr()).collect(),
//                     agg_expr: agg_expr.iter().map(|e| e.expr()).collect(),
//                     schema: schema.clone(),
//                 }),
//             },
//             SerializedLogicalPlan::RollingWindowAgg {
//                 schema,
//                 input,
//                 dimension,
//                 partition_by,
//                 from,
//                 to,
//                 every,
//                 rolling_aggs,
//                 group_by_dimension,
//                 aggs,
//             } => LogicalPlan::Extension {
//                 node: Arc::new(RollingWindowAggregate {
//                     schema: schema.clone(),
//                     input: input.logical_plan(worker_context)?,
//                     dimension: dimension.clone(),
//                     from: from.expr(),
//                     to: to.expr(),
//                     every: every.expr(),
//                     partition_by: partition_by.clone(),
//                     rolling_aggs: exprs(&rolling_aggs),
//                     group_by_dimension: group_by_dimension.as_ref().map(|d| d.expr()),
//                     aggs: exprs(&aggs),
//                 }),
//             },
//             SerializedLogicalPlan::Panic {} => LogicalPlan::Extension {
//                 node: Arc::new(PanicWorkerNode {}),
//             },
//         })
//     }
//     fn is_empty_relation(&self) -> Option<DFSchemaRef> {
//         match self {
//             SerializedLogicalPlan::EmptyRelation {
//                 produce_one_row,
//                 schema,
//             } => {
//                 if !produce_one_row {
//                     Some(schema.clone())
//                 } else {
//                     None
//                 }
//             }
//             _ => None,
//         }
//     }
//
//     fn remove_unused_tables(
//         &self,
//         partition_ids_to_execute: &Vec<(u64, RowFilter)>,
//         inline_tables_to_execute: &Vec<InlineTableId>,
//     ) -> SerializedLogicalPlan {
//         debug_assert!(partition_ids_to_execute
//             .iter()
//             .is_sorted_by_key(|(id, _)| id));
//         match self {
//             SerializedLogicalPlan::Projection {
//                 expr,
//                 input,
//                 schema,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 if input.is_empty_relation().is_some() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Projection {
//                         expr: expr.clone(),
//                         input: Arc::new(input),
//                         schema: schema.clone(),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::Filter { predicate, input } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 if let Some(schema) = input.is_empty_relation() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Filter {
//                         predicate: predicate.clone(),
//                         input: Arc::new(input),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::Aggregate {
//                 input,
//                 group_expr,
//                 aggr_expr,
//                 schema,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 SerializedLogicalPlan::Aggregate {
//                     input: Arc::new(input),
//                     group_expr: group_expr.clone(),
//                     aggr_expr: aggr_expr.clone(),
//                     schema: schema.clone(),
//                 }
//             }
//             SerializedLogicalPlan::Sort { expr, input } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 if let Some(schema) = input.is_empty_relation() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Sort {
//                         expr: expr.clone(),
//                         input: Arc::new(input),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::Union {
//                 inputs,
//                 schema,
//                 alias,
//             } => {
//                 let inputs = inputs
//                     .iter()
//                     .filter_map(|i| {
//                         let i = i.remove_unused_tables(
//                             partition_ids_to_execute,
//                             inline_tables_to_execute,
//                         );
//                         if i.is_empty_relation().is_some() {
//                             None
//                         } else {
//                             Some(Arc::new(i))
//                         }
//                     })
//                     .collect::<Vec<_>>();
//
//                 if inputs.is_empty() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Union {
//                         inputs,
//                         schema: schema.clone(),
//                         alias: alias.clone(),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::TableScan {
//                 table_name,
//                 source,
//                 projection,
//                 projected_schema,
//                 filters,
//                 alias,
//                 limit,
//             } => {
//                 let is_empty = match source {
//                     SerializedTableSource::CubeTable(table) => {
//                         !table.has_partitions(partition_ids_to_execute)
//                     }
//                     SerializedTableSource::InlineTable(table) => {
//                         !table.has_inline_table_id(inline_tables_to_execute)
//                     }
//                 };
//                 if is_empty {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: projected_schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::TableScan {
//                         table_name: table_name.clone(),
//                         source: source.clone(),
//                         projection: projection.clone(),
//                         projected_schema: projected_schema.clone(),
//                         filters: filters.clone(),
//                         alias: alias.clone(),
//                         limit: limit.clone(),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::EmptyRelation {
//                 produce_one_row,
//                 schema,
//             } => SerializedLogicalPlan::EmptyRelation {
//                 produce_one_row: *produce_one_row,
//                 schema: schema.clone(),
//             },
//             SerializedLogicalPlan::Limit { n, input } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 if let Some(schema) = input.is_empty_relation() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Limit {
//                         n: *n,
//                         input: Arc::new(input),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::Skip { n, input } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 if let Some(schema) = input.is_empty_relation() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Skip {
//                         n: *n,
//                         input: Arc::new(input),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::Join {
//                 left,
//                 right,
//                 on,
//                 join_type,
//                 join_constraint,
//                 schema,
//             } => {
//                 let left =
//                     left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 let right =
//                     right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 SerializedLogicalPlan::Join {
//                     left: Arc::new(left),
//                     right: Arc::new(right),
//                     on: on.clone(),
//                     join_type: join_type.clone(),
//                     join_constraint: *join_constraint,
//                     schema: schema.clone(),
//                 }
//             }
//             SerializedLogicalPlan::Repartition {
//                 input,
//                 partitioning_scheme,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 if let Some(schema) = input.is_empty_relation() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Repartition {
//                         input: Arc::new(input),
//                         partitioning_scheme: partitioning_scheme.clone(),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::Alias {
//                 input,
//                 alias,
//                 schema,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 if input.is_empty_relation().is_some() {
//                     SerializedLogicalPlan::EmptyRelation {
//                         produce_one_row: false,
//                         schema: schema.clone(),
//                     }
//                 } else {
//                     SerializedLogicalPlan::Alias {
//                         input: Arc::new(input),
//                         alias: alias.clone(),
//                         schema: schema.clone(),
//                     }
//                 }
//             }
//             SerializedLogicalPlan::ClusterSend {
//                 input,
//                 snapshots,
//                 limit_and_reverse,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 SerializedLogicalPlan::ClusterSend {
//                     input: Arc::new(input),
//                     snapshots: snapshots.clone(),
//                     limit_and_reverse: limit_and_reverse.clone(),
//                 }
//             }
//             SerializedLogicalPlan::ClusterAggregateTopK {
//                 limit,
//                 input,
//                 group_expr,
//                 aggregate_expr,
//                 sort_columns,
//                 having_expr,
//                 schema,
//                 snapshots,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 SerializedLogicalPlan::ClusterAggregateTopK {
//                     limit: *limit,
//                     input: Arc::new(input),
//                     group_expr: group_expr.clone(),
//                     aggregate_expr: aggregate_expr.clone(),
//                     sort_columns: sort_columns.clone(),
//                     having_expr: having_expr.clone(),
//                     schema: schema.clone(),
//                     snapshots: snapshots.clone(),
//                 }
//             }
//             SerializedLogicalPlan::CrossJoin {
//                 left,
//                 right,
//                 on,
//                 join_schema,
//             } => {
//                 let left =
//                     left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 let right =
//                     right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 SerializedLogicalPlan::CrossJoin {
//                     left: Arc::new(left),
//                     right: Arc::new(right),
//                     on: on.clone(),
//                     join_schema: join_schema.clone(),
//                 }
//             }
//             SerializedLogicalPlan::CrossJoinAgg {
//                 left,
//                 right,
//                 on,
//                 join_schema,
//                 group_expr,
//                 agg_expr,
//                 schema,
//             } => {
//                 let left =
//                     left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 let right =
//                     right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//
//                 SerializedLogicalPlan::CrossJoinAgg {
//                     left: Arc::new(left),
//                     right: Arc::new(right),
//                     on: on.clone(),
//                     join_schema: join_schema.clone(),
//                     group_expr: group_expr.clone(),
//                     agg_expr: agg_expr.clone(),
//                     schema: schema.clone(),
//                 }
//             }
//             SerializedLogicalPlan::RollingWindowAgg {
//                 schema,
//                 input,
//                 dimension,
//                 partition_by,
//                 from,
//                 to,
//                 every,
//                 rolling_aggs,
//                 group_by_dimension,
//                 aggs,
//             } => {
//                 let input =
//                     input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
//                 SerializedLogicalPlan::RollingWindowAgg {
//                     schema: schema.clone(),
//                     input: Arc::new(input),
//                     dimension: dimension.clone(),
//                     partition_by: partition_by.clone(),
//                     from: from.clone(),
//                     to: to.clone(),
//                     every: every.clone(),
//                     rolling_aggs: rolling_aggs.clone(),
//                     group_by_dimension: group_by_dimension.clone(),
//                     aggs: aggs.clone(),
//                 }
//             }
//             SerializedLogicalPlan::Panic {} => SerializedLogicalPlan::Panic {},
//         }
//     }
// }

// TODO upgrade DF
// #[derive(Clone, Serialize, Deserialize, Debug)]
// pub enum SerializedExpr {
//     Alias(Box<SerializedExpr>, String),
//     Column(String, Option<String>),
//     ScalarVariable(Vec<String>),
//     Literal(ScalarValue),
//     BinaryExpr {
//         left: Box<SerializedExpr>,
//         op: Operator,
//         right: Box<SerializedExpr>,
//     },
//     Not(Box<SerializedExpr>),
//     IsNotNull(Box<SerializedExpr>),
//     IsNull(Box<SerializedExpr>),
//     Negative(Box<SerializedExpr>),
//     Between {
//         expr: Box<SerializedExpr>,
//         negated: bool,
//         low: Box<SerializedExpr>,
//         high: Box<SerializedExpr>,
//     },
//     Case {
//         /// Optional base expression that can be compared to literal values in the "when" expressions
//         expr: Option<Box<SerializedExpr>>,
//         /// One or more when/then expressions
//         when_then_expr: Vec<(Box<SerializedExpr>, Box<SerializedExpr>)>,
//         /// Optional "else" expression
//         else_expr: Option<Box<SerializedExpr>>,
//     },
//     Cast {
//         expr: Box<SerializedExpr>,
//         data_type: DataType,
//     },
//     TryCast {
//         expr: Box<SerializedExpr>,
//         data_type: DataType,
//     },
//     Sort {
//         expr: Box<SerializedExpr>,
//         asc: bool,
//         nulls_first: bool,
//     },
//     ScalarFunction {
//         fun: functions::BuiltinScalarFunction,
//         args: Vec<SerializedExpr>,
//     },
//     ScalarUDF {
//         fun: CubeScalarUDFKind,
//         args: Vec<SerializedExpr>,
//     },
//     AggregateFunction {
//         fun: aggregates::AggregateFunction,
//         args: Vec<SerializedExpr>,
//         distinct: bool,
//     },
//     AggregateUDF {
//         fun: CubeAggregateUDFKind,
//         args: Vec<SerializedExpr>,
//     },
//     RollingAggregate {
//         agg: Box<SerializedExpr>,
//         start: WindowFrameBound,
//         end: WindowFrameBound,
//         offset_to_end: bool,
//     },
//     InList {
//         expr: Box<SerializedExpr>,
//         list: Vec<SerializedExpr>,
//         negated: bool,
//     },
//     Wildcard,
// }
//
// impl SerializedExpr {
//     fn expr(&self) -> Expr {
//         match self {
//             SerializedExpr::Alias(e, a) => Expr::Alias(Box::new(e.expr()), a.to_string()),
//             SerializedExpr::Column(c, a) => Expr::Column(Column {
//                 name: c.clone(),
//                 relation: a.clone(),
//             }),
//             SerializedExpr::ScalarVariable(v) => Expr::ScalarVariable(v.clone()),
//             SerializedExpr::Literal(v) => Expr::Literal(v.clone()),
//             SerializedExpr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
//                 left: Box::new(left.expr()),
//                 op: op.clone(),
//                 right: Box::new(right.expr()),
//             },
//             SerializedExpr::Not(e) => Expr::Not(Box::new(e.expr())),
//             SerializedExpr::IsNotNull(e) => Expr::IsNotNull(Box::new(e.expr())),
//             SerializedExpr::IsNull(e) => Expr::IsNull(Box::new(e.expr())),
//             SerializedExpr::Cast { expr, data_type } => Expr::Cast {
//                 expr: Box::new(expr.expr()),
//                 data_type: data_type.clone(),
//             },
//             SerializedExpr::TryCast { expr, data_type } => Expr::TryCast {
//                 expr: Box::new(expr.expr()),
//                 data_type: data_type.clone(),
//             },
//             SerializedExpr::Sort {
//                 expr,
//                 asc,
//                 nulls_first,
//             } => Expr::Sort {
//                 expr: Box::new(expr.expr()),
//                 asc: *asc,
//                 nulls_first: *nulls_first,
//             },
//             SerializedExpr::ScalarFunction { fun, args } => Expr::ScalarFunction {
//                 fun: fun.clone(),
//                 args: args.iter().map(|e| e.expr()).collect(),
//             },
//             SerializedExpr::ScalarUDF { fun, args } => Expr::ScalarUDF {
//                 fun: Arc::new(scalar_udf_by_kind(*fun).descriptor()),
//                 args: args.iter().map(|e| e.expr()).collect(),
//             },
//             SerializedExpr::AggregateFunction {
//                 fun,
//                 args,
//                 distinct,
//             } => Expr::AggregateFunction {
//                 fun: fun.clone(),
//                 args: args.iter().map(|e| e.expr()).collect(),
//                 distinct: *distinct,
//             },
//             SerializedExpr::AggregateUDF { fun, args } => Expr::AggregateUDF {
//                 fun: Arc::new(aggregate_udf_by_kind(*fun).descriptor()),
//                 args: args.iter().map(|e| e.expr()).collect(),
//             },
//             SerializedExpr::Case {
//                 expr,
//                 else_expr,
//                 when_then_expr,
//             } => Expr::Case {
//                 expr: expr.as_ref().map(|e| Box::new(e.expr())),
//                 else_expr: else_expr.as_ref().map(|e| Box::new(e.expr())),
//                 when_then_expr: when_then_expr
//                     .iter()
//                     .map(|(w, t)| (Box::new(w.expr()), Box::new(t.expr())))
//                     .collect(),
//             },
//             SerializedExpr::Wildcard => Expr::Wildcard,
//             SerializedExpr::Negative(value) => Expr::Negative(Box::new(value.expr())),
//             SerializedExpr::Between {
//                 expr,
//                 negated,
//                 low,
//                 high,
//             } => Expr::Between {
//                 expr: Box::new(expr.expr()),
//                 negated: *negated,
//                 low: Box::new(low.expr()),
//                 high: Box::new(high.expr()),
//             },
//             SerializedExpr::RollingAggregate {
//                 agg,
//                 start,
//                 end,
//                 offset_to_end,
//             } => Expr::RollingAggregate {
//                 agg: Box::new(agg.expr()),
//                 start: start.clone(),
//                 end: end.clone(),
//                 offset: match offset_to_end {
//                     false => RollingOffset::Start,
//                     true => RollingOffset::End,
//                 },
//             },
//             SerializedExpr::InList {
//                 expr,
//                 list,
//                 negated,
//             } => Expr::InList {
//                 expr: Box::new(expr.expr()),
//                 list: list.iter().map(|e| e.expr()).collect(),
//                 negated: *negated,
//             },
//         }
//     }
// }

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedTableSource {
    CubeTable(CubeTable),
    InlineTable(InlineTableProvider),
}

impl SerializedPlan {
    pub async fn try_new(
        plan: LogicalPlan,
        index_snapshots: PlanningMeta,
        trace_obj: Option<String>,
    ) -> Result<Self, CubeError> {
        let serialized_logical_plan =
            datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec(
                &plan,
                &CubeExtensionCodec {
                    worker_context: None,
                },
            )?;
        Ok(SerializedPlan {
            logical_plan: Arc::new(serialized_logical_plan.to_vec()),
            schema_snapshot: Arc::new(SchemaSnapshot { index_snapshots }),
            partition_ids_to_execute: Vec::new(),
            inline_table_ids_to_execute: Vec::new(),
            trace_obj,
        })
    }

    pub fn with_partition_id_to_execute(
        &self,
        partition_ids_to_execute: Vec<(u64, RowFilter)>,
        inline_table_ids_to_execute: Vec<InlineTableId>,
    ) -> Self {
        Self {
            // TODO upgrade DF
            // logical_plan: Arc::new(
            //     self.logical_plan
            //         .remove_unused_tables(&partition_ids_to_execute, &inline_table_ids_to_execute),
            // ),
            logical_plan: self.logical_plan.clone(),
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute,
            inline_table_ids_to_execute,
            trace_obj: self.trace_obj.clone(),
        }
    }

    pub fn logical_plan(
        &self,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
    ) -> Result<LogicalPlan, CubeError> {
        // TODO DF upgrade SessionContext::new()
        let logical_plan = logical_plan_from_bytes_with_extension_codec(
            self.logical_plan.as_slice(),
            &SessionContext::new(),
            &CubeExtensionCodec {
                worker_context: Some(WorkerContext {
                    remote_to_local_names,
                    worker_partition_ids: self.partition_ids_to_execute.clone(),
                    inline_table_ids_to_execute: self.inline_table_ids_to_execute.clone(),
                    chunk_id_to_record_batches,
                    parquet_metadata_cache,
                }),
            },
        )?;
        Ok(logical_plan)
    }

    pub fn trace_obj(&self) -> Option<String> {
        self.trace_obj.clone()
    }

    pub fn index_snapshots(&self) -> &Vec<IndexSnapshot> {
        &self.schema_snapshot.index_snapshots.indices
    }

    pub fn planning_meta(&self) -> &PlanningMeta {
        &self.schema_snapshot.index_snapshots
    }

    pub fn files_to_download(&self) -> Vec<(IdRow<Partition>, String, Option<u64>, Option<u64>)> {
        self.list_files_to_download(|id| {
            self.partition_ids_to_execute
                .binary_search_by_key(&id, |(id, _)| *id)
                .is_ok()
        })
    }

    /// Note: avoid during normal execution, workers must filter the partitions they execute.
    pub fn all_required_files(&self) -> Vec<(IdRow<Partition>, String, Option<u64>, Option<u64>)> {
        self.list_files_to_download(|_| true)
    }

    fn list_files_to_download(
        &self,
        include_partition: impl Fn(u64) -> bool,
    ) -> Vec<(
        IdRow<Partition>,
        /* file_name */ String,
        /* size */ Option<u64>,
        /* chunk_id */ Option<u64>,
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
                        None,
                    ));
                }

                for chunk in partition.chunks() {
                    if !chunk.get_row().in_memory() {
                        files.push((
                            partition.partition.clone(),
                            chunk.get_row().get_full_name(chunk.get_id()),
                            chunk.get_row().file_size(),
                            Some(chunk.get_id()),
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

    pub fn is_data_select_query<'a>(plan: &'a LogicalPlan) -> bool {
        struct Visitor {
            seen_data_scans: bool,
        }
        impl<'n> TreeNodeVisitor<'n> for Visitor {
            type Node = LogicalPlan;

            fn f_down(
                &mut self,
                plan: &'n Self::Node,
            ) -> datafusion::common::Result<TreeNodeRecursion> {
                if let LogicalPlan::TableScan(TableScan {
                    source, table_name, ..
                }) = plan
                {
                    let table_provider = &source
                        .as_any()
                        .downcast_ref::<DefaultTableSource>()
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Non DefaultTableSource source found for {}",
                                table_name
                            ))
                        })?
                        .table_provider;
                    if table_provider
                        .as_any()
                        .downcast_ref::<InfoSchemaTableProvider>()
                        .is_none()
                        && table_provider
                            .as_any()
                            .downcast_ref::<InfoSchemaQueryCacheTableProvider>()
                            .is_none()
                    {
                        self.seen_data_scans = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            }

            fn f_up(
                &mut self,
                _node: &'n Self::Node,
            ) -> datafusion::common::Result<TreeNodeRecursion> {
                Ok(TreeNodeRecursion::Continue)
            }
        }

        let mut v = Visitor {
            seen_data_scans: false,
        };
        plan.visit(&mut v).expect("no failures possible");
        return v.seen_data_scans;
    }

    fn serialized_logical_plan(
        plan: &LogicalPlan,
    ) -> Result<SerializedLogicalPlan, DataFusionError> {
        Ok(SerializedLogicalPlan {
            serialized_bytes: Arc::new(
                datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec(
                    &plan,
                    &CubeExtensionCodec {
                        worker_context: None,
                    },
                )?
                .to_vec(),
            ),
        })
    }
}

impl Debug for CubeExtensionCodec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CubeExtensionCodec")
    }
}

struct CubeExtensionCodec {
    worker_context: Option<WorkerContext>,
}

impl LogicalExtensionCodec for CubeExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> datafusion::common::Result<Extension> {
        use serde::Deserialize;
        let r = flexbuffers::Reader::get_root(buf)
            .map_err(|e| DataFusionError::Execution(format!("try_decode: {}", e)))?;
        let serialized = ExtensionNodeSerialized::deserialize(r)
            .map_err(|e| DataFusionError::Execution(format!("try_decode: {}", e)))?;
        Ok(Extension {
            node: Arc::new(match serialized {
                ExtensionNodeSerialized::ClusterSend(serialized) => {
                    ClusterSendNode::from_serialized(inputs, serialized)
                }
            }),
        })
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
        use serde::Serialize;
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        let to_serialize =
            if let Some(cluster_send) = node.node.as_any().downcast_ref::<ClusterSendNode>() {
                ExtensionNodeSerialized::ClusterSend(cluster_send.to_serialized())
            } else {
                todo!("{:?}", node)
            };
        to_serialize
            .serialize(&mut ser)
            .map_err(|e| DataFusionError::Execution(format!("try_encode: {}", e)))?;
        buf.extend(ser.take_buffer());
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        ctx: &SessionContext,
    ) -> datafusion::common::Result<Arc<dyn TableProvider>> {
        use serde::Deserialize;
        let mut r = flexbuffers::Reader::get_root(buf)
            .map_err(|e| DataFusionError::Execution(format!("try_decode_table_provider: {}", e)))?;
        let serialized = SerializedTableProvider::deserialize(r)
            .map_err(|e| DataFusionError::Execution(format!("try_decode_table_provider: {}", e)))?;
        let provider: Arc<dyn TableProvider> = match serialized {
            SerializedTableProvider::CubeTable(table) => {
                let worker_context = self
                    .worker_context
                    .as_ref()
                    .expect("WorkerContext isn't set for try_decode_table_provider");
                Arc::new(table.to_worker_table(
                    worker_context.remote_to_local_names.clone(),
                    worker_context.worker_partition_ids.clone(),
                    worker_context.chunk_id_to_record_batches.clone(),
                    worker_context.parquet_metadata_cache.clone(),
                ))
            }
            SerializedTableProvider::CubeTableLogical(logical) => Arc::new(logical),
            SerializedTableProvider::InlineTableProvider(inline) => {
                let worker_context = self
                    .worker_context
                    .as_ref()
                    .expect("WorkerContext isn't set for try_decode_table_provider");
                Arc::new(inline.to_worker_table(worker_context.inline_table_ids_to_execute.clone()))
            }
        };
        Ok(provider)
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let to_serialize = if let Some(cube_table) = node.as_any().downcast_ref::<CubeTable>() {
            SerializedTableProvider::CubeTable(cube_table.clone())
        } else if let Some(cube_table_logical) = node.as_any().downcast_ref::<CubeTableLogical>() {
            SerializedTableProvider::CubeTableLogical(cube_table_logical.clone())
        } else if let Some(inline_table) = node.as_any().downcast_ref::<InlineTableProvider>() {
            SerializedTableProvider::InlineTableProvider(inline_table.clone())
        } else {
            return Err(DataFusionError::Execution(format!(
                "Can't encode table provider for {}",
                table_ref
            )));
        };

        use serde::Serialize;
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        to_serialize
            .serialize(&mut ser)
            .map_err(|e| DataFusionError::Execution(format!("try_encode_table_provider: {}", e)))?;
        buf.extend(ser.take_buffer());
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SerializedTableProvider {
    CubeTable(CubeTable),
    CubeTableLogical(CubeTableLogical),
    InlineTableProvider(InlineTableProvider),
}

// TODO upgrade DF
// fn exprs(e: &[SerializedExpr]) -> Vec<Expr> {
//     e.iter().map(|e| e.expr()).collect()
// }
