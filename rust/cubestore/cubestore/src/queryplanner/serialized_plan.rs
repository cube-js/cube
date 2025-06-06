use super::udfs::{registerable_aggregate_udfs, registerable_scalar_udfs};
use crate::metastore::table::{Table, TablePath};
use crate::metastore::{Chunk, IdRow, Index, Partition};
use crate::queryplanner::panic::PanicWorkerNode;
use crate::queryplanner::planning::{ClusterSendNode, ExtensionNodeSerialized, PlanningMeta};
use crate::queryplanner::providers::InfoSchemaQueryCacheTableProvider;
use crate::queryplanner::query_executor::{CubeTable, InlineTableId, InlineTableProvider};
use crate::queryplanner::rolling::RollingWindowAggregate;
use crate::queryplanner::topk::{ClusterAggregateTopKLower, ClusterAggregateTopKUpper};
use crate::queryplanner::{pretty_printers, CubeTableLogical, InfoSchemaTableProvider};
use crate::table::Row;
use crate::CubeError;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use serde_derive::{Deserialize, Serialize};

use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::DFSchemaRef;
use datafusion::common::TableReference;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    wrap_projection_for_join_if_necessary, Aggregate, Distinct, DistinctOn, EmptyRelation, Expr,
    Extension, Filter, Join, Limit, LogicalPlan, Projection, RecursiveQuery, Repartition, Sort,
    Subquery, SubqueryAlias, TableScan, Union, Unnest, Values, Window,
};
use datafusion::prelude::SessionContext;
use datafusion_proto::bytes::logical_plan_from_bytes_with_extension_codec;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
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

/// SerializedPlan, but before we actually serialize the LogicalPlan.
#[derive(Debug)]
pub struct PreSerializedPlan {
    logical_plan: LogicalPlan,
    schema_snapshot: Arc<SchemaSnapshot>,
    partition_ids_to_execute: Vec<(u64, RowFilter)>,
    inline_table_ids_to_execute: Vec<InlineTableId>,
    trace_obj: Option<String>,
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd)]
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq, Hash, PartialOrd)]
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

#[derive(Clone, Serialize, Deserialize, Debug, Hash, PartialEq, Eq, PartialOrd)]
pub struct InlineSnapshot {
    pub id: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct SerializedLogicalPlan {
    serialized_bytes: Arc<Vec<u8>>,
}

pub struct WorkerContext {
    remote_to_local_names: HashMap<String, String>,
    worker_partition_ids: Vec<(u64, RowFilter)>,
    inline_table_ids_to_execute: Vec<InlineTableId>,
    chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
    parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
}

fn is_empty_relation(plan: &LogicalPlan) -> Option<DFSchemaRef> {
    match plan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema,
        }) => {
            if !produce_one_row {
                Some(schema.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Takes an inner LogicalPlan, whose schema has the same length and names as
/// `union_schema`, but (perhaps) different table qualifiers.  Assumes the
/// DataTypes are the same.  Wraps the inner LogicalPlan with a Projection
/// having the correct alias expressions for the output schema.
fn wrap_pruned_union_if_necessary(
    inner: LogicalPlan,
    union_schema: &DFSchemaRef,
) -> Result<LogicalPlan, CubeError> {
    let inner_schema = inner.schema();
    if inner_schema.fields().len() != union_schema.fields().len() {
        return Err(CubeError::internal(format!("inner schema incompatible with union_schema (len): inner_schema = {:?}; union_schema = {:?}", inner_schema, union_schema)));
    }

    let mut expr_list = Vec::<Expr>::with_capacity(inner_schema.fields().len());
    let mut projection_needed = false;
    for (i, ((union_table_reference, union_field), ip @ (inner_table_reference, inner_field))) in
        union_schema.iter().zip(inner_schema.iter()).enumerate()
    {
        if union_field.name() != inner_field.name() {
            return Err(CubeError::internal(format!("inner schema incompatible with union schema (name mismatch at index {}): inner_schema = {:?}; union_schema = {:?}", i, inner_schema, union_schema)));
        }

        let expr = Expr::from(ip);

        if union_table_reference != inner_table_reference {
            projection_needed = true;
            expr_list.push(expr.alias_qualified(
                union_table_reference.map(|tr| tr.clone()),
                union_field.name(),
            ));
        } else {
            expr_list.push(expr);
        }
    }

    if projection_needed {
        Ok(LogicalPlan::Projection(Projection::try_new(
            expr_list,
            Arc::new(inner),
        )?))
    } else {
        Ok(inner)
    }
}

impl PreSerializedPlan {
    fn remove_unused_tables(
        plan: &LogicalPlan,
        partition_ids_to_execute: &Vec<(u64, RowFilter)>,
        inline_tables_to_execute: &Vec<InlineTableId>,
    ) -> Result<LogicalPlan, CubeError> {
        debug_assert!(partition_ids_to_execute
            .iter()
            .is_sorted_by_key(|(id, _)| id));
        let res = match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    &input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                if is_empty_relation(&input).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Projection(Projection::try_new_with_schema(
                        expr.clone(),
                        Arc::new(input),
                        schema.clone(),
                    )?)
                }
            }
            LogicalPlan::Filter(Filter {
                predicate,
                input,
                having,
                ..
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    &input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                if let Some(schema) = is_empty_relation(&input) {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Filter(if *having {
                        Filter::try_new_with_having(predicate.clone(), Arc::new(input))
                    } else {
                        Filter::try_new(predicate.clone(), Arc::new(input))
                    }?)
                }
            }
            LogicalPlan::Aggregate(Aggregate {
                input,
                group_expr,
                aggr_expr,
                schema,
                ..
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    &input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                    Arc::new(input),
                    group_expr.clone(),
                    aggr_expr.clone(),
                    schema.clone(),
                )?)
            }
            LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    &input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                if let Some(schema) = is_empty_relation(&input) {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Sort(Sort {
                        expr: expr.clone(),
                        input: Arc::new(input),
                        fetch: *fetch,
                    })
                }
            }
            LogicalPlan::Union(Union { inputs, schema }) => {
                let mut new_inputs: Vec<LogicalPlan> = Vec::with_capacity(inputs.len());
                for input in inputs {
                    let i = PreSerializedPlan::remove_unused_tables(
                        &input,
                        partition_ids_to_execute,
                        inline_tables_to_execute,
                    )?;
                    if !is_empty_relation(&i).is_some() {
                        new_inputs.push(i);
                    }
                }

                let res = match new_inputs.len() {
                    0 => LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    }),
                    1 => {
                        // Union _requires_ 2 or more inputs.
                        let plan = new_inputs.pop().unwrap();
                        wrap_pruned_union_if_necessary(plan, schema)?
                    }
                    _ => {
                        let plan = LogicalPlan::Union(Union {
                            inputs: new_inputs.into_iter().map(Arc::new).collect(),
                            schema: schema.clone(),
                        });
                        wrap_pruned_union_if_necessary(plan, schema)?
                    }
                };
                res
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
            }) => {
                let is_empty = if let Some(default_source) =
                    source.as_any().downcast_ref::<DefaultTableSource>()
                {
                    if let Some(table) = default_source
                        .table_provider
                        .as_any()
                        .downcast_ref::<CubeTable>()
                    {
                        !table.has_partitions(partition_ids_to_execute)
                    } else if let Some(table) = default_source
                        .table_provider
                        .as_any()
                        .downcast_ref::<InlineTableProvider>()
                    {
                        !table.has_inline_table_id(inline_tables_to_execute)
                    } else {
                        return Err(CubeError::internal(
                            "remove_unused_tables called with unexpected table provider"
                                .to_string(),
                        ));
                    }
                } else {
                    return Err(CubeError::internal(
                        "remove_unused_tables called with unexpected table source".to_string(),
                    ));
                };
                if is_empty {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: projected_schema.clone(),
                    })
                } else {
                    LogicalPlan::TableScan(TableScan {
                        table_name: table_name.clone(),
                        source: source.clone(),
                        projection: projection.clone(),
                        projected_schema: projected_schema.clone(),
                        filters: filters.clone(),
                        fetch: *fetch,
                    })
                }
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row,
                schema,
            }) => LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: *produce_one_row,
                schema: schema.clone(),
            }),
            LogicalPlan::Limit(Limit { skip, fetch, input }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                if let Some(schema) = is_empty_relation(&input) {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Limit(Limit {
                        skip: skip.clone(),
                        fetch: fetch.clone(),
                        input: Arc::new(input),
                    })
                }
            }
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equals_null,
            }) => {
                let left = PreSerializedPlan::remove_unused_tables(
                    left,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                let right = PreSerializedPlan::remove_unused_tables(
                    right,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                LogicalPlan::Join(Join {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on: on.clone(),
                    filter: filter.clone(),
                    join_type: join_type.clone(),
                    join_constraint: *join_constraint,
                    schema: schema.clone(),
                    null_equals_null: *null_equals_null,
                })
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                if let Some(schema) = is_empty_relation(&input) {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Repartition(Repartition {
                        input: Arc::new(input),
                        partitioning_scheme: partitioning_scheme.clone(),
                    })
                }
            }
            LogicalPlan::Subquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => {
                let subquery: LogicalPlan = PreSerializedPlan::remove_unused_tables(
                    subquery,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                if is_empty_relation(&subquery).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: subquery.schema().clone(),
                    })
                } else {
                    LogicalPlan::Subquery(Subquery {
                        subquery: Arc::new(subquery),
                        outer_ref_columns: outer_ref_columns.clone(),
                    })
                }
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
                ..
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;

                if is_empty_relation(&input).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                        Arc::new(input),
                        alias.clone(),
                    )?)
                }
            }
            // TODO upgrade DF: Figure out where CrossJoin went.
            // LogicalPlan::CrossJoin(CrossJoin {
            //     left,
            //     right,
            //     schema,
            // }) => {
            //     let left = PreSerializedPlan::remove_unused_tables(
            //         left,
            //         partition_ids_to_execute,
            //         inline_tables_to_execute,
            //     )?;
            //     let right = PreSerializedPlan::remove_unused_tables(
            //         right,
            //         partition_ids_to_execute,
            //         inline_tables_to_execute,
            //     )?;

            //     LogicalPlan::CrossJoin(CrossJoin {
            //         left: Arc::new(left),
            //         right: Arc::new(right),
            //         schema: schema.clone(),
            //     })
            // }
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                if is_empty_relation(&input).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Window(Window {
                        input: Arc::new(input),
                        window_expr: window_expr.clone(),
                        schema: schema.clone(),
                    })
                }
            }
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let schema = input.schema();
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                if is_empty_relation(&input).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Distinct(Distinct::All(Arc::new(input)))
                }
            }
            LogicalPlan::Distinct(Distinct::On(DistinctOn {
                on_expr,
                select_expr,
                sort_expr,
                input,
                schema,
            })) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                if is_empty_relation(&input).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Distinct(Distinct::On(DistinctOn {
                        on_expr: on_expr.clone(),
                        select_expr: select_expr.clone(),
                        sort_expr: sort_expr.clone(),
                        input: Arc::new(input),
                        schema: schema.clone(),
                    }))
                }
            }
            LogicalPlan::RecursiveQuery(RecursiveQuery {
                name,
                static_term,
                recursive_term,
                is_distinct,
            }) => {
                let static_term = PreSerializedPlan::remove_unused_tables(
                    static_term,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                let recursive_term = PreSerializedPlan::remove_unused_tables(
                    recursive_term,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                LogicalPlan::RecursiveQuery(RecursiveQuery {
                    name: name.clone(),
                    static_term: Arc::new(static_term),
                    recursive_term: Arc::new(recursive_term),
                    is_distinct: *is_distinct,
                })
            }
            LogicalPlan::Values(Values { schema, values }) => LogicalPlan::Values(Values {
                schema: schema.clone(),
                values: values.clone(),
            }),
            LogicalPlan::Unnest(Unnest {
                input,
                exec_columns,
                list_type_columns,
                struct_type_columns,
                dependency_indices,
                schema,
                options,
            }) => {
                let input = PreSerializedPlan::remove_unused_tables(
                    input,
                    partition_ids_to_execute,
                    inline_tables_to_execute,
                )?;
                if is_empty_relation(&input).is_some() {
                    LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema: schema.clone(),
                    })
                } else {
                    LogicalPlan::Unnest(Unnest {
                        input: Arc::new(input),
                        exec_columns: exec_columns.clone(),
                        list_type_columns: list_type_columns.clone(),
                        struct_type_columns: struct_type_columns.clone(),
                        dependency_indices: dependency_indices.clone(),
                        schema: schema.clone(),
                        options: options.clone(),
                    })
                }
            }
            LogicalPlan::Extension(Extension { node }) => {
                if let Some(cluster_send) = node.as_any().downcast_ref::<ClusterSendNode>() {
                    let ClusterSendNode {
                        id,
                        input,
                        snapshots,
                        limit_and_reverse,
                    } = cluster_send;
                    let input = PreSerializedPlan::remove_unused_tables(
                        &input,
                        partition_ids_to_execute,
                        inline_tables_to_execute,
                    )?;
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(ClusterSendNode {
                            id: *id,
                            input: Arc::new(input),
                            snapshots: snapshots.clone(),
                            limit_and_reverse: *limit_and_reverse,
                        }),
                    })
                } else if let Some(panic_worker) = node.as_any().downcast_ref::<PanicWorkerNode>() {
                    let PanicWorkerNode {} = panic_worker; // (No fields to recurse; just clone the existing Arc `node`.)
                    LogicalPlan::Extension(Extension { node: node.clone() })
                } else if let Some(cluster_agg_topk) =
                    node.as_any().downcast_ref::<ClusterAggregateTopKUpper>()
                {
                    let ClusterAggregateTopKUpper {
                        limit,
                        input,
                        order_by,
                        having_expr,
                    } = cluster_agg_topk;
                    let input = PreSerializedPlan::remove_unused_tables(
                        input,
                        partition_ids_to_execute,
                        inline_tables_to_execute,
                    )?;
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(ClusterAggregateTopKUpper {
                            limit: *limit,
                            input: Arc::new(input),
                            order_by: order_by.clone(),
                            having_expr: having_expr.clone(),
                        }),
                    })
                } else if let Some(cluster_agg_topk) =
                    node.as_any().downcast_ref::<ClusterAggregateTopKLower>()
                {
                    let ClusterAggregateTopKLower {
                        input,
                        group_expr,
                        aggregate_expr,
                        schema,
                        snapshots,
                    } = cluster_agg_topk;
                    let input = PreSerializedPlan::remove_unused_tables(
                        input,
                        partition_ids_to_execute,
                        inline_tables_to_execute,
                    )?;
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(ClusterAggregateTopKLower {
                            input: Arc::new(input),
                            group_expr: group_expr.clone(),
                            aggregate_expr: aggregate_expr.clone(),
                            schema: schema.clone(),
                            snapshots: snapshots.clone(),
                        }),
                    })
                } else if let Some(rolling_window) =
                    node.as_any().downcast_ref::<RollingWindowAggregate>()
                {
                    let RollingWindowAggregate {
                        schema,
                        input,
                        dimension,
                        dimension_alias,
                        partition_by,
                        from,
                        to,
                        every,
                        rolling_aggs,
                        rolling_aggs_alias,
                        group_by_dimension,
                        aggs,
                        lower_bound,
                        upper_bound,
                        offset_to_end,
                    } = rolling_window;
                    let input = PreSerializedPlan::remove_unused_tables(
                        input,
                        partition_ids_to_execute,
                        inline_tables_to_execute,
                    )?;
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(RollingWindowAggregate {
                            schema: schema.clone(),
                            input: Arc::new(input),
                            dimension: dimension.clone(),
                            partition_by: partition_by.clone(),
                            from: from.clone(),
                            to: to.clone(),
                            every: every.clone(),
                            rolling_aggs: rolling_aggs.clone(),
                            rolling_aggs_alias: rolling_aggs_alias.clone(),
                            group_by_dimension: group_by_dimension.clone(),
                            aggs: aggs.clone(),
                            lower_bound: lower_bound.clone(),
                            upper_bound: upper_bound.clone(),
                            dimension_alias: dimension_alias.clone(),
                            offset_to_end: *offset_to_end,
                        }),
                    })
                } else {
                    // TODO upgrade DF: Ensure any uture backported plan extensions are implemented.
                    return Err(CubeError::internal(format!(
                        "remove_unused_tables not handling Extension case: {:?}",
                        node
                    )));
                }
            }
            LogicalPlan::Explain(_)
            | LogicalPlan::Statement(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::Ddl(_)
            | LogicalPlan::Copy(_)
            | LogicalPlan::DescribeTable(_) => {
                return Err(CubeError::internal(format!(
                    "remove_unused_tables not handling case: {}",
                    pretty_printers::pp_plan(plan)
                )));
            } // TODO upgrade DF
              // SerializedLogicalPlan::CrossJoinAgg {
              //     left,
              //     right,
              //     on,
              //     join_schema,
              //     group_expr,
              //     agg_expr,
              //     schema,
              // } => {
              //     let left =
              //         left.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
              //     let right =
              //         right.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);

              //     SerializedLogicalPlan::CrossJoinAgg {
              //         left: Arc::new(left),
              //         right: Arc::new(right),
              //         on: on.clone(),
              //         join_schema: join_schema.clone(),
              //         group_expr: group_expr.clone(),
              //         agg_expr: agg_expr.clone(),
              //         schema: schema.clone(),
              //     }
              // }
              // SerializedLogicalPlan::RollingWindowAgg {
              //     schema,
              //     input,
              //     dimension,
              //     partition_by,
              //     from,
              //     to,
              //     every,
              //     rolling_aggs,
              //     group_by_dimension,
              //     aggs,
              // } => {
              //     let input =
              //         input.remove_unused_tables(partition_ids_to_execute, inline_tables_to_execute);
              //     SerializedLogicalPlan::RollingWindowAgg {
              //         schema: schema.clone(),
              //         input: Arc::new(input),
              //         dimension: dimension.clone(),
              //         partition_by: partition_by.clone(),
              //         from: from.clone(),
              //         to: to.clone(),
              //         every: every.clone(),
              //         rolling_aggs: rolling_aggs.clone(),
              //         group_by_dimension: group_by_dimension.clone(),
              //         aggs: aggs.clone(),
              //     }
              // }
        };
        // Now, for this node, we go through every Expr in the node and remove unused tables from the Subquery.
        // This wraps a LogicalPlan::Subquery node and expects the same result.
        let res: LogicalPlan = res
            .map_subqueries(|node: LogicalPlan| {
                match node {
                    LogicalPlan::Subquery(Subquery {
                        subquery,
                        outer_ref_columns,
                    }) => {
                        let subquery: LogicalPlan = PreSerializedPlan::remove_unused_tables(
                            &subquery,
                            partition_ids_to_execute,
                            inline_tables_to_execute,
                        )?;

                        // We must return a LogicalPlan::Subquery.
                        Ok(Transformed::yes(LogicalPlan::Subquery(Subquery {
                            subquery: Arc::new(subquery),
                            outer_ref_columns,
                        })))
                    }
                    _ => Err(DataFusionError::Internal(
                        "map_subqueries should pass a subquery node".to_string(),
                    )),
                }
            })?
            .data;
        Ok(res)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub enum SerializedTableSource {
    CubeTable(CubeTable),
    InlineTable(InlineTableProvider),
}

impl PreSerializedPlan {
    pub fn to_serialized_plan(&self) -> Result<SerializedPlan, CubeError> {
        let serialized_logical_plan =
            datafusion_proto::bytes::logical_plan_to_bytes_with_extension_codec(
                &self.logical_plan,
                &CubeExtensionCodec {
                    worker_context: None,
                },
            )?;
        Ok(SerializedPlan {
            logical_plan: Arc::new(serialized_logical_plan.to_vec()),
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute: self.partition_ids_to_execute.clone(),
            inline_table_ids_to_execute: self.inline_table_ids_to_execute.clone(),
            trace_obj: self.trace_obj.clone(),
        })
    }

    pub fn try_new(
        plan: LogicalPlan,
        index_snapshots: PlanningMeta,
        trace_obj: Option<String>,
    ) -> Result<Self, CubeError> {
        Ok(PreSerializedPlan {
            logical_plan: plan,
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
    ) -> Result<Self, CubeError> {
        let logical_plan = PreSerializedPlan::remove_unused_tables(
            &self.logical_plan,
            &partition_ids_to_execute,
            &inline_table_ids_to_execute,
        )?;
        Ok(Self {
            logical_plan,
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute,
            inline_table_ids_to_execute,
            trace_obj: self.trace_obj.clone(),
        })
    }

    pub fn replace_logical_plan(&self, logical_plan: LogicalPlan) -> Result<Self, CubeError> {
        Ok(Self {
            logical_plan,
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute: self.partition_ids_to_execute.clone(),
            inline_table_ids_to_execute: self.inline_table_ids_to_execute.clone(),
            trace_obj: self.trace_obj.clone(),
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
        Self::list_files_to_download_given_index_snapshots(indexes, include_partition)
    }

    fn list_files_to_download_given_index_snapshots(
        indexes: &Vec<IndexSnapshot>,
        include_partition: impl Fn(u64) -> bool,
    ) -> Vec<(
        IdRow<Partition>,
        /* file_name */ String,
        /* size */ Option<u64>,
        /* chunk_id */ Option<u64>,
    )> {
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

    pub fn index_snapshots(&self) -> &Vec<IndexSnapshot> {
        &self.schema_snapshot.index_snapshots.indices
    }

    pub fn planning_meta(&self) -> &PlanningMeta {
        &self.schema_snapshot.index_snapshots
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.logical_plan
    }
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

    pub fn to_pre_serialized(
        &self,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
    ) -> Result<PreSerializedPlan, CubeError> {
        let plan = self.logical_plan(
            remote_to_local_names,
            chunk_id_to_record_batches,
            parquet_metadata_cache,
        )?;
        Ok(PreSerializedPlan {
            logical_plan: plan,
            schema_snapshot: self.schema_snapshot.clone(),
            partition_ids_to_execute: self.partition_ids_to_execute.clone(),
            inline_table_ids_to_execute: self.inline_table_ids_to_execute.clone(),
            trace_obj: self.trace_obj.clone(),
        })
    }

    pub fn logical_plan(
        &self,
        remote_to_local_names: HashMap<String, String>,
        chunk_id_to_record_batches: HashMap<u64, Vec<RecordBatch>>,
        parquet_metadata_cache: Arc<dyn ParquetFileReaderFactory>,
    ) -> Result<LogicalPlan, CubeError> {
        // TODO DF upgrade SessionContext::new()
        // After this comment was made, we now register_udaf... what else?
        let session_context = SessionContext::new();
        // TODO DF upgrade: consistently build SessionContexts/register udafs/udfs.
        for udaf in registerable_aggregate_udfs() {
            session_context.register_udaf(udaf);
        }
        for udf in registerable_scalar_udfs() {
            session_context.register_udf(udf);
        }

        let logical_plan = logical_plan_from_bytes_with_extension_codec(
            self.logical_plan.as_slice(),
            &session_context,
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
        let indexes: &Vec<IndexSnapshot> = self.index_snapshots();
        PreSerializedPlan::list_files_to_download_given_index_snapshots(indexes, |id| {
            self.partition_ids_to_execute
                .binary_search_by_key(&id, |(id, _)| *id)
                .is_ok()
        })
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
            node: match serialized {
                ExtensionNodeSerialized::ClusterSend(serialized) => {
                    Arc::new(ClusterSendNode::from_serialized(inputs, serialized))
                }
                ExtensionNodeSerialized::PanicWorker(serialized) => {
                    Arc::new(PanicWorkerNode::from_serialized(inputs, serialized))
                }
                ExtensionNodeSerialized::RollingWindowAggregate(serialized) => Arc::new(
                    RollingWindowAggregate::from_serialized(serialized, inputs, ctx)?,
                ),
                ExtensionNodeSerialized::ClusterAggregateTopKUpper(serialized) => Arc::new(
                    ClusterAggregateTopKUpper::from_serialized(serialized, inputs, ctx)?,
                ),
                ExtensionNodeSerialized::ClusterAggregateTopKLower(serialized) => Arc::new(
                    ClusterAggregateTopKLower::from_serialized(serialized, inputs, ctx)?,
                ),
            },
        })
    }

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
        use serde::Serialize;
        let mut ser = flexbuffers::FlexbufferSerializer::new();
        let to_serialize = if let Some(cluster_send) =
            node.node.as_any().downcast_ref::<ClusterSendNode>()
        {
            ExtensionNodeSerialized::ClusterSend(cluster_send.to_serialized())
        } else if let Some(panic_worker) = node.node.as_any().downcast_ref::<PanicWorkerNode>() {
            ExtensionNodeSerialized::PanicWorker(panic_worker.to_serialized())
        } else if let Some(rolling_window_aggregate) =
            node.node.as_any().downcast_ref::<RollingWindowAggregate>()
        {
            ExtensionNodeSerialized::RollingWindowAggregate(
                rolling_window_aggregate.to_serialized()?,
            )
        } else if let Some(topk_aggregate) = node
            .node
            .as_any()
            .downcast_ref::<ClusterAggregateTopKUpper>()
        {
            ExtensionNodeSerialized::ClusterAggregateTopKUpper(topk_aggregate.to_serialized()?)
        } else if let Some(topk_aggregate) = node
            .node
            .as_any()
            .downcast_ref::<ClusterAggregateTopKLower>()
        {
            ExtensionNodeSerialized::ClusterAggregateTopKLower(topk_aggregate.to_serialized()?)
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
        let r = flexbuffers::Reader::get_root(buf)
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
