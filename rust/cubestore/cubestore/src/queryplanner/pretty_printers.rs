//! Presentation of query plans for use in tests.

use bigdecimal::ToPrimitive;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::DFSchema;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    Aggregate, EmptyRelation, Explain, Extension, FetchType, Filter, Join, Limit, LogicalPlan,
    Projection, Repartition, SkipType, Sort, TableScan, Union, Window,
};
use datafusion::physical_expr::{AcrossPartitions, ConstExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode, PlanProperties};
use datafusion::prelude::Expr;
use datafusion_datasource::memory::MemoryExec;
use itertools::{repeat_n, Itertools};
use std::sync::Arc;

use crate::queryplanner::check_memory::CheckMemoryExec;
use crate::queryplanner::filter_by_key_range::FilterByKeyRangeExec;
use crate::queryplanner::merge_sort::LastRowByUniqueKeyExec;
use crate::queryplanner::panic::{PanicWorkerExec, PanicWorkerNode};
use crate::queryplanner::planning::{ClusterSendNode, Snapshot, WorkerExec};
use crate::queryplanner::providers::InfoSchemaQueryCacheTableProvider;
use crate::queryplanner::query_executor::{
    ClusterSendExec, CubeTable, CubeTableExec, InlineTableProvider,
};
use crate::queryplanner::rolling::RollingWindowAggregate;
use crate::queryplanner::serialized_plan::{IndexSnapshot, RowRange};
use crate::queryplanner::tail_limit::TailLimitExec;
use crate::queryplanner::topk::SortColumn;
use crate::queryplanner::topk::{
    AggregateTopKExec, ClusterAggregateTopKLower, ClusterAggregateTopKUpper,
};
use crate::queryplanner::trace_data_loaded::TraceDataLoadedExec;
use crate::queryplanner::{CubeTableLogical, InfoSchemaTableProvider, QueryPlan};
use crate::streaming::topic_table_provider::TopicTableProvider;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::joins::{HashJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;

#[derive(Default, Clone, Copy)]
pub struct PPOptions {
    pub show_filters: bool,
    pub show_sort_by: bool,
    pub show_aggregations: bool,
    pub show_schema: bool,
    // Applies only to physical plan.
    pub show_output_hints: bool,
    pub show_check_memory_nodes: bool,
    pub show_partitions: bool,
}

impl PPOptions {
    #[allow(unused)]
    pub fn everything() -> PPOptions {
        PPOptions {
            show_filters: true,
            show_sort_by: true,
            show_aggregations: true,
            show_schema: true,
            show_output_hints: true,
            show_check_memory_nodes: true,
            show_partitions: true,
        }
    }

    pub fn none() -> PPOptions {
        PPOptions::default()
    }
}

pub fn pp_phys_plan(p: &dyn ExecutionPlan) -> String {
    pp_phys_plan_ext(p, &PPOptions::default())
}

pub fn pp_phys_plan_ext(p: &dyn ExecutionPlan, o: &PPOptions) -> String {
    let mut r = String::new();
    pp_phys_plan_indented(p, 0, o, &mut r);
    r
}

pub fn pp_plan(p: &LogicalPlan) -> String {
    pp_plan_ext(p, &PPOptions::none())
}

pub fn pp_query_plan_ext(qp: &QueryPlan, o: &PPOptions) -> String {
    pp_plan_ext(
        match qp {
            QueryPlan::Meta(p) => p,
            QueryPlan::Select(pre_serialized_plan, _) => pre_serialized_plan.logical_plan(),
        },
        o,
    )
}

pub fn pp_query_plan(p: &QueryPlan) -> String {
    pp_query_plan_ext(p, &PPOptions::none())
}

pub fn pp_plan_ext(p: &LogicalPlan, opts: &PPOptions) -> String {
    let mut v = Printer {
        level: 0,
        expecting_topk_lower: false,
        output: String::new(),
        level_stack: Vec::new(),
        opts,
    };
    p.visit(&mut v).unwrap();
    return v.output;

    pub struct Printer<'a> {
        level: usize,
        expecting_topk_lower: bool,
        output: String,
        // We pop a stack of levels instead of decrementing the level, because with topk upper/lower
        // node pairs, we skip a level.
        level_stack: Vec<usize>,
        opts: &'a PPOptions,
    }

    impl<'a> TreeNodeVisitor<'a> for Printer<'a> {
        type Node = LogicalPlan;

        fn f_down(&mut self, plan: &LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError> {
            self.level_stack.push(self.level);

            let initial_output_len = self.output.len();
            if self.level != 0 {
                self.output += "\n";
            }

            let was_expecting_topk_lower = self.expecting_topk_lower;
            self.expecting_topk_lower = false;
            let mut saw_expected_topk_lower = false;

            self.output.extend(repeat_n(' ', 2 * self.level));
            match plan {
                LogicalPlan::Projection(Projection {
                    expr,
                    schema,
                    input: _,
                    ..
                }) => {
                    self.output += &format!(
                        "Projection, [{}]",
                        expr.iter()
                            .enumerate()
                            .map(|(i, e)| {
                                let in_name = e.schema_name().to_string();
                                let out_name = schema.field(i).name();
                                if &in_name != out_name {
                                    format!("{}:{}", in_name, out_name)
                                } else {
                                    in_name
                                }
                            })
                            .join(", ")
                    );
                }
                LogicalPlan::Filter(Filter { predicate, .. }) => {
                    self.output += "Filter";
                    if self.opts.show_filters {
                        self.output += &format!(", predicate: {:?}", predicate)
                    }
                }
                LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) => {
                    self.output += "Aggregate";
                    if self.opts.show_aggregations {
                        self.output += &format!(", aggs: {}", pp_exprs(aggr_expr))
                    }
                }
                LogicalPlan::Sort(Sort { expr, fetch, .. }) => {
                    self.output += "Sort";
                    if self.opts.show_sort_by {
                        self.output += &format!(", by: {:?}", expr)
                    }
                    if let Some(fetch) = fetch {
                        self.output += &format!(", fetch: {}", fetch)
                    }
                }
                LogicalPlan::Union(Union { schema, .. }) => {
                    self.output += &format!("Union, schema: {}", pp_df_schema(schema.as_ref()))
                }
                LogicalPlan::Join(Join { on, .. }) => {
                    self.output += &format!(
                        "Join on: [{}]",
                        on.iter().map(|(l, r)| format!("{} = {}", l, r)).join(", ")
                    )
                }
                LogicalPlan::Repartition(Repartition { .. }) => self.output += "Repartition",
                LogicalPlan::TableScan(TableScan {
                    table_name,
                    source,
                    projected_schema,
                    filters,
                    fetch,
                    ..
                }) => {
                    self.output += &format!(
                        "Scan {}, source: {}",
                        table_name,
                        pp_source(
                            source
                                .as_any()
                                .downcast_ref::<DefaultTableSource>()
                                .expect("Non DefaultTableSource table found")
                                .table_provider
                                .clone()
                        )
                    );
                    if projected_schema.fields().len() != source.schema().fields().len() {
                        self.output += &format!(
                            ", fields: [{}]",
                            projected_schema
                                .fields()
                                .iter()
                                .map(|f| f.name())
                                .join(", ")
                        );
                    } else {
                        self.output += ", fields: *";
                    };

                    if self.opts.show_filters && !filters.is_empty() {
                        self.output += &format!(", filters: {:?}", filters)
                    }
                    if let Some(fetch) = fetch {
                        self.output += &format!(", fetch: {}", fetch)
                    }
                }
                LogicalPlan::EmptyRelation(EmptyRelation { .. }) => self.output += "Empty",
                LogicalPlan::Limit(
                    limit @ Limit {
                        skip: _,
                        fetch: _,
                        input: _,
                    },
                ) => {
                    let fetch: Result<FetchType, DataFusionError> = limit.get_fetch_type();
                    let skip: Result<SkipType, DataFusionError> = limit.get_skip_type();
                    let mut sep = ", ";
                    let mut silent_infinite_fetch = false;
                    match skip {
                        Ok(SkipType::Literal(0)) => {
                            sep = "";
                        }
                        Ok(SkipType::Literal(n)) => {
                            silent_infinite_fetch = true;
                            self.output += "Skip";
                        }
                        Ok(SkipType::UnsupportedExpr) => self.output += "Skip UnsupportedExpr",
                        Err(e) => self.output += &format!("Skip Err({})", e),
                    };
                    match fetch {
                        Ok(FetchType::Literal(Some(_))) => self.output += &format!("{}Limit", sep),
                        Ok(FetchType::Literal(None)) => {
                            if !silent_infinite_fetch {
                                self.output += &format!("{}Limit infinity", sep)
                            }
                        }
                        Ok(FetchType::UnsupportedExpr) => {
                            self.output += &format!("{}Limit UnsupportedExpr", sep)
                        }
                        Err(e) => self.output += &format!("{}Limit Err({})", sep, e),
                    };
                }
                // LogicalPlan::CreateExternalTable(CreateExternalTable { .. }) => self.output += "CreateExternalTable",
                LogicalPlan::Explain(Explain { .. }) => self.output += "Explain",
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(cs) = node.as_any().downcast_ref::<ClusterSendNode>() {
                        self.output += &format!(
                            "ClusterSend, indices: {:?}",
                            cs.snapshots
                                .iter()
                                .map(|is| is
                                    .iter()
                                    .map(|s| match s {
                                        Snapshot::Index(i) => i.index.get_id(),
                                        Snapshot::Inline(i) => i.id,
                                    }
                                    .to_i64()
                                    .map_or(-1, |i| i))
                                    .collect_vec())
                                .collect_vec()
                        )
                    } else if let Some(topk) =
                        node.as_any().downcast_ref::<ClusterAggregateTopKUpper>()
                    {
                        // We have some cute, or ugly, code here, to avoid having separate upper and
                        // lower nodes in the pretty-printing.  Maybe this is to create fewer
                        // differences in the tests in the upgrade DF and non-upgrade DF branch.

                        self.output += &format!("ClusterAggregateTopK, limit: {}", topk.limit);
                        let lower_node: Option<&ClusterAggregateTopKLower> =
                            match topk.input.as_ref() {
                                LogicalPlan::Extension(Extension { node }) => {
                                    if let Some(lower_node) =
                                        node.as_any().downcast_ref::<ClusterAggregateTopKLower>()
                                    {
                                        Some(lower_node)
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };

                        if let Some(lower_node) = lower_node {
                            if self.opts.show_aggregations {
                                self.output +=
                                    &format!(", aggs: {}", pp_exprs(&lower_node.aggregate_expr))
                            }
                            if self.opts.show_sort_by {
                                self.output += &format!(
                                    ", sortBy: {}",
                                    pp_sort_columns(lower_node.group_expr.len(), &topk.order_by)
                                );
                            }
                            if self.opts.show_filters {
                                if let Some(having) = &topk.having_expr {
                                    self.output += &format!(", having: {:?}", having)
                                }
                            }
                            self.expecting_topk_lower = true;
                        } else {
                            self.output += ", (ERROR: no matching lower node)";
                        }
                        self.expecting_topk_lower = true;
                    } else if let Some(_) =
                        node.as_any().downcast_ref::<ClusterAggregateTopKLower>()
                    {
                        if !was_expecting_topk_lower {
                            self.output +=
                                &format!("ClusterAggregateTopKLower (ERROR: unexpected)");
                        } else {
                            // Pop the newline and indentation we just pushed.
                            self.output.truncate(initial_output_len);
                            // And then note that we shouldn't increment the level.
                            saw_expected_topk_lower = true;
                        }
                    } else if let Some(_) = node.as_any().downcast_ref::<PanicWorkerNode>() {
                        self.output += &format!("PanicWorker")
                    } else if let Some(_) = node.as_any().downcast_ref::<RollingWindowAggregate>() {
                        self.output += &format!("RollingWindowAggreagate");
                    // TODO upgrade DF
                    // } else if let Some(alias) = node.as_any().downcast_ref::<LogicalAlias>() {
                    //     self.output += &format!("LogicalAlias, alias: {}", alias.alias);
                    } else {
                        log::error!("unknown extension node")
                    }
                }
                LogicalPlan::Window(Window { .. }) => {
                    self.output += "Window";
                }
                // TODO upgrade DF: There may be some join printable as "Cross" in DF.
                // LogicalPlan::CrossJoin(CrossJoin { .. }) => {
                //     self.output += "CrossJoin";
                // }
                LogicalPlan::Subquery(_) => {
                    self.output += "Subquery";
                }
                LogicalPlan::SubqueryAlias(_) => {
                    self.output += "SubqueryAlias";
                }
                LogicalPlan::Statement(_) => {
                    self.output += "Statement";
                }
                LogicalPlan::Values(_) => {
                    self.output += "Values";
                }
                LogicalPlan::Analyze(_) => {
                    self.output += "Analyze";
                }
                LogicalPlan::Distinct(_) => {
                    self.output += "Distinct";
                }
                LogicalPlan::Dml(_) => {
                    self.output += "Dml";
                }
                LogicalPlan::Ddl(_) => {
                    self.output += "Ddl";
                }
                LogicalPlan::Copy(_) => {
                    self.output += "Copy";
                }
                LogicalPlan::DescribeTable(_) => {
                    self.output += "DescribeTable";
                }
                LogicalPlan::Unnest(_) => {
                    self.output += "Unnest";
                }
                LogicalPlan::RecursiveQuery(_) => {
                    self.output += "RecursiveQuery";
                }
            }

            if self.opts.show_schema {
                self.output += &format!(", schema: {}", pp_df_schema(plan.schema().as_ref()));
            }

            if !saw_expected_topk_lower {
                self.level += 1;
            } else if !was_expecting_topk_lower {
                // Not the cleanest place to put this message, but it's not supposed to happen.
                self.output += ", ERROR: no topk lower node";
            }

            Ok(TreeNodeRecursion::Continue)
        }

        fn f_up(&mut self, _plan: &LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError> {
            // The level_stack shouldn't be empty, fwiw.
            self.level = self.level_stack.pop().unwrap_or_default();
            Ok(TreeNodeRecursion::Continue)
        }
    }
}

fn pp_index(index: &IndexSnapshot) -> String {
    let mut r = format!(
        "{}:{}:{:?}",
        index.index.get_row().get_name(),
        index.index.get_id(),
        index
            .partitions
            .iter()
            .map(|p| p.partition.get_id())
            .collect_vec()
    );
    if let Some(so) = &index.sort_on {
        r += &format!(":sort_on[{}]", so.join(", "))
    }
    r
}

fn pp_source(t: Arc<dyn TableProvider>) -> String {
    if t.as_any().is::<CubeTableLogical>() {
        "CubeTableLogical".to_string()
    } else if let Some(t) = t.as_any().downcast_ref::<CubeTable>() {
        format!("CubeTable(index: {})", pp_index(t.index_snapshot()))
    } else if let Some(t) = t.as_any().downcast_ref::<InlineTableProvider>() {
        format!("InlineTableProvider(data: {} rows)", t.get_data().len())
    } else if let Some(t) = t.as_any().downcast_ref::<InfoSchemaTableProvider>() {
        format!("InfoSchemaTableProvider(table: {:?})", t.table)
    } else if let Some(_) = t
        .as_any()
        .downcast_ref::<InfoSchemaQueryCacheTableProvider>()
    {
        "InfoSchemaQueryCacheTableProvider".to_string()
    } else if let Some(_) = t.as_any().downcast_ref::<TopicTableProvider>() {
        "TopicTableProvider".to_string()
    } else {
        panic!("unknown table provider");
    }
}

pub fn pp_sort_columns(first_agg: usize, cs: &[SortColumn]) -> String {
    format!(
        "[{}]",
        cs.iter()
            .map(|c| {
                let mut r = (first_agg + c.agg_index + 1).to_string();
                if !c.asc {
                    r += " desc";
                }
                if !c.nulls_first {
                    r += " null last";
                }
                r
            })
            .join(", ")
    )
}

fn pp_phys_plan_indented(p: &dyn ExecutionPlan, indent: usize, o: &PPOptions, out: &mut String) {
    if (p.as_any().is::<CheckMemoryExec>() || p.as_any().is::<TraceDataLoadedExec>())
        && !o.show_check_memory_nodes
    {
        //We don't show CheckMemoryExec in plan by default
        if let Some(child) = p.children().first() {
            pp_phys_plan_indented(child.as_ref(), indent, o, out)
        }
        return;
    }
    pp_instance(p, indent, o, out);
    if p.as_any().is::<ClusterSendExec>() {
        // Do not show children of ClusterSend. This is a hack to avoid rewriting all tests.
        return;
    }
    for c in p.children() {
        pp_phys_plan_indented(c.as_ref(), indent + 2, o, out);
    }

    fn pp_instance(p: &dyn ExecutionPlan, indent: usize, o: &PPOptions, out: &mut String) {
        if indent != 0 {
            *out += "\n";
        }
        out.extend(repeat_n(' ', indent));

        let mut skip_show_partitions = false;

        let a = p.as_any();
        if let Some(t) = a.downcast_ref::<CubeTableExec>() {
            *out += &format!("Scan, index: {}", pp_index(&t.index_snapshot));
            if t.index_snapshot.index.get_row().columns().len() == t.schema().fields().len() {
                *out += ", fields: *";
            } else {
                *out += &format!(
                    ", fields: [{}]",
                    t.schema().fields().iter().map(|f| f.name()).join(", ")
                );
            }
            if o.show_filters && t.filter.is_some() {
                *out += &format!(", predicate: {:?}", t.filter.as_ref().unwrap())
            }
        } else if let Some(_) = a.downcast_ref::<EmptyExec>() {
            *out += "Empty";
        } else if let Some(p) = a.downcast_ref::<ProjectionExec>() {
            *out += &format!(
                "Projection, [{}]",
                p.expr()
                    .iter()
                    .map(|(e, out_name)| {
                        if let Some(c) = e.as_any().downcast_ref::<Column>() {
                            if c.name() == out_name {
                                return c.name().to_string();
                            }
                        }
                        format!("{}:{}", e.to_string(), out_name)
                    })
                    .join(", ")
            );
        } else if let Some(agg) = a.downcast_ref::<AggregateExec>() {
            let strat = match agg.input_order_mode() {
                InputOrderMode::Sorted => "Sorted",
                InputOrderMode::Linear => "Linear",
                InputOrderMode::PartiallySorted(_) => "PartiallySorted",
            };
            let mode = match agg.mode() {
                AggregateMode::Partial => "Partial",
                AggregateMode::Final => "Final",
                AggregateMode::FinalPartitioned => "FinalPartitioned",
                AggregateMode::Single => "Single",
                AggregateMode::SinglePartitioned => "SinglePartitioned",
            };
            *out += &format!("{}{}Aggregate", strat, mode);
            if o.show_aggregations {
                *out += &format!(", aggs: {:?}", agg.aggr_expr())
            }
            if let Some(limit) = agg.limit() {
                *out += &format!(", limit: {}", limit)
            }
        } else if let Some(l) = a.downcast_ref::<LocalLimitExec>() {
            *out += &format!("LocalLimit, n: {}", l.fetch());
        } else if let Some(l) = a.downcast_ref::<GlobalLimitExec>() {
            *out += &format!(
                "GlobalLimit, n: {}",
                l.fetch()
                    .map(|l| l.to_string())
                    .unwrap_or("None".to_string())
            );
            if l.skip() > 0 {
                *out += &format!(", skip: {}", l.skip());
            }
        } else if let Some(l) = a.downcast_ref::<TailLimitExec>() {
            *out += &format!("TailLimit, n: {}", l.limit);
        } else if let Some(f) = a.downcast_ref::<FilterExec>() {
            *out += "Filter";
            if o.show_filters {
                *out += &format!(", predicate: {}", f.predicate())
            }
        } else if let Some(s) = a.downcast_ref::<SortExec>() {
            *out += "Sort";
            if o.show_sort_by {
                *out += &format!(
                    ", by: [{}]",
                    s.expr()
                        .iter()
                        .map(|e| {
                            let mut r = format!("{}", e.expr);
                            if e.options.descending {
                                r += " desc";
                            }
                            if !e.options.nulls_first {
                                r += " nulls last";
                            }
                            r
                        })
                        .join(", ")
                );
            }
            if let Some(fetch) = s.fetch() {
                *out += &format!(", fetch: {}", fetch);
            }
        } else if let Some(_) = a.downcast_ref::<HashJoinExec>() {
            *out += "HashJoin";
        } else if let Some(cs) = a.downcast_ref::<ClusterSendExec>() {
            *out += &format!(
                "ClusterSend, partitions: [{}]",
                cs.partitions
                    .iter()
                    .map(|(_, (ps, inline))| {
                        let ps = ps
                            .iter()
                            .map(|(id, range)| format!("{}{}", id, pp_row_range(range)))
                            .join(", ");
                        if !inline.is_empty() {
                            format!("[{}, inline: {}]", ps, inline.iter().join(", "))
                        } else {
                            format!("[{}]", ps)
                        }
                    })
                    .join(", ")
            );
            skip_show_partitions = true;
        } else if let Some(topk) = a.downcast_ref::<AggregateTopKExec>() {
            *out += &format!("AggregateTopK, limit: {:?}", topk.limit);
            if o.show_aggregations {
                *out += &format!(", aggs: {:?}", topk.agg_expr);
            }
            if o.show_sort_by {
                *out += &format!(
                    ", sortBy: {}",
                    pp_sort_columns(topk.key_len, &topk.order_by)
                );
            }
            if o.show_filters {
                if let Some(having) = &topk.having {
                    *out += &format!(", having: {}", having);
                }
            }
        } else if let Some(_) = a.downcast_ref::<PanicWorkerExec>() {
            *out += "PanicWorker";
        } else if let Some(_) = a.downcast_ref::<WorkerExec>() {
            *out += &format!("Worker");
        } else if let Some(_) = a.downcast_ref::<CoalescePartitionsExec>() {
            *out += "CoalescePartitions";
        } else if let Some(s) = a.downcast_ref::<SortPreservingMergeExec>() {
            *out += "MergeSort";
            // } else if let Some(_) = a.downcast_ref::<MergeReSortExec>() {
            //     *out += "MergeResort";
            if let Some(fetch) = s.fetch() {
                *out += &format!(", fetch: {}", fetch);
            }
        } else if let Some(j) = a.downcast_ref::<SortMergeJoinExec>() {
            *out += &format!(
                "MergeJoin, on: [{}]",
                j.on.iter()
                    .map(|(l, r)| format!("{} = {}", l, r))
                    .join(", ")
            );
            // } else if let Some(j) = a.downcast_ref::<CrossJoinExec>() {
            //     *out += &format!("CrossJoin, on: {}", j.on)
            // } else if let Some(j) = a.downcast_ref::<CrossJoinAggExec>() {
            //     *out += &format!("CrossJoinAgg, on: {}", j.join.on);
            //     if o.show_aggregations {
            //         *out += &format!(", aggs: {:?}", j.agg_expr)
            //     }
        } else if let Some(_) = a.downcast_ref::<UnionExec>() {
            *out += "Union";
        } else if let Some(_) = a.downcast_ref::<FilterByKeyRangeExec>() {
            *out += "FilterByKeyRange";
        } else if let Some(p) = a.downcast_ref::<ParquetExec>() {
            *out += &format!(
                "ParquetScan, files: {}",
                p.base_config()
                    .file_groups
                    .iter()
                    .flatten()
                    .map(|p| p.object_meta.location.to_string())
                    .join(",")
            );
            // TODO upgrade DF
            // } else if let Some(_) = a.downcast_ref::<SkipExec>() {
            //     *out += "SkipRows";
            // } else if let Some(_) = a.downcast_ref::<RollingWindowAggExec>() {
            //     *out += "RollingWindowAgg";
        } else if let Some(_) = a.downcast_ref::<LastRowByUniqueKeyExec>() {
            *out += "LastRowByUniqueKey";
        } else if let Some(_) = a.downcast_ref::<MemoryExec>() {
            *out += "MemoryScan";
        } else if let Some(r) = a.downcast_ref::<RepartitionExec>() {
            *out += &format!("Repartition, partitioning: {}", r.partitioning());
        } else {
            let to_string = format!("{:?}", p);
            *out += &to_string.split(" ").next().unwrap_or(&to_string);
        }

        if o.show_output_hints {
            let properties: &PlanProperties = p.properties();

            // What show_output_hints shows is previous Cubestore's output hints.  We convert from
            // DF's existing properties() to the old output format (and what the old output_hints()
            // function returned).
            //
            // So the choice to show the particular sort_order and single_vals in terms of column
            // indices is solely based on that past, and to update the `planning_hints` test in a
            // straightforward and transparent manner.

            let svals: &[ConstExpr] = properties.equivalence_properties().constants();
            if svals.len() > 0 {
                let sv_columns: Option<Vec<usize>> = svals
                    .iter()
                    .map(|const_expr| match const_expr.across_partitions() {
                        AcrossPartitions::Uniform(_) => {
                            if let Some(column_expr) =
                                const_expr.expr().as_any().downcast_ref::<Column>()
                            {
                                Some(column_expr.index())
                            } else {
                                None
                            }
                        }
                        AcrossPartitions::Heterogeneous => None,
                    })
                    .collect();

                if let Some(column_indices) = sv_columns {
                    *out += &format!(", single_vals: {:?}", column_indices);
                } else {
                    *out += &format!(", single_vals: [..., len = {}]", svals.len());
                }
            }

            let ordering = properties.output_ordering();
            if let Some(so) = ordering {
                let so_columns: Option<Vec<usize>> = so
                    .iter()
                    .map(|sort_expr| {
                        if let Some(column_expr) = sort_expr.expr.as_any().downcast_ref::<Column>()
                        {
                            Some(column_expr.index())
                        } else {
                            None
                        }
                    })
                    .collect();

                if let Some(column_indices) = so_columns {
                    *out += &format!(", sort_order: {:?}", column_indices);
                } else {
                    *out += &format!(", sort_order: [..., len = {}]", so.len());
                }
            }
        }

        if o.show_schema {
            *out += &format!(", schema: {}", pp_schema(p.schema().as_ref()));
        }

        if o.show_partitions && !skip_show_partitions {
            *out += &format!(
                ", partitions: {}",
                p.properties().output_partitioning().partition_count()
            );
        }
    }
}

fn pp_row_range(r: &RowRange) -> String {
    if r.matches_all_rows() {
        return String::new();
    }
    let s = match &r.start {
        None => "-∞".to_string(),
        Some(s) => format!("{:?}", s.values()),
    };
    let e = match &r.end {
        None => "∞".to_string(),
        Some(e) => format!("{:?}", e.values()),
    };
    format!("[{},{})", s, e)
}

fn pp_exprs(v: &Vec<Expr>) -> String {
    "[".to_owned() + &v.iter().map(|e: &Expr| format!("{}", e)).join(", ") + "]"
}

fn pp_df_schema(schema: &DFSchema) -> String {
    // Like pp_schema but with qualifiers.
    format!("{}", schema)
}

fn pp_schema(schema: &Schema) -> String {
    // Mimicking DFSchema's Display
    format!(
        "fields:[{}], metadata:{:?}",
        schema.fields.iter().map(|f| f.name()).join(", "),
        schema.metadata
    )
}
