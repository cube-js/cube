//! Presentation of query plans for use in tests.

use bigdecimal::ToPrimitive;

use datafusion::cube_ext::alias::LogicalAlias;
use datafusion::datasource::TableProvider;
use datafusion::logical_plan::{LogicalPlan, PlanVisitor};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::hash_aggregate::{
    AggregateMode, AggregateStrategy, HashAggregateExec,
};
use datafusion::physical_plan::hash_join::HashJoinExec;
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::merge_join::MergeJoinExec;
use datafusion::physical_plan::merge_sort::{
    LastRowByUniqueKeyExec, MergeReSortExec, MergeSortExec,
};
use datafusion::physical_plan::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use itertools::{repeat_n, Itertools};

use crate::queryplanner::filter_by_key_range::FilterByKeyRangeExec;
use crate::queryplanner::panic::{PanicWorkerExec, PanicWorkerNode};
use crate::queryplanner::planning::{ClusterSendNode, Snapshot, WorkerExec};
use crate::queryplanner::query_executor::{
    ClusterSendExec, CubeTable, CubeTableExec, InlineTableProvider,
};
use crate::queryplanner::serialized_plan::{IndexSnapshot, RowRange};
use crate::queryplanner::tail_limit::TailLimitExec;
use crate::queryplanner::topk::ClusterAggregateTopK;
use crate::queryplanner::topk::{AggregateTopKExec, SortColumn};
use crate::queryplanner::CubeTableLogical;
use datafusion::cube_ext::join::CrossJoinExec;
use datafusion::cube_ext::joinagg::CrossJoinAggExec;
use datafusion::cube_ext::rolling::RollingWindowAggExec;
use datafusion::cube_ext::rolling::RollingWindowAggregate;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::merge::MergeExec;
use datafusion::physical_plan::parquet::ParquetExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::skip::SkipExec;
use datafusion::physical_plan::union::UnionExec;

#[derive(Default, Clone, Copy)]
pub struct PPOptions {
    pub show_filters: bool,
    pub show_sort_by: bool,
    pub show_aggregations: bool,
    // Applies only to physical plan.
    pub show_output_hints: bool,
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
    pp_plan_ext(p, &PPOptions::default())
}

pub fn pp_plan_ext(p: &LogicalPlan, opts: &PPOptions) -> String {
    let mut v = Printer {
        level: 0,
        output: String::new(),
        opts,
    };
    p.accept(&mut v).unwrap();
    return v.output;

    pub struct Printer<'a> {
        level: usize,
        output: String,
        opts: &'a PPOptions,
    }

    impl PlanVisitor for Printer<'_> {
        type Error = ();

        fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
            if self.level != 0 {
                self.output += "\n";
            }
            self.output.extend(repeat_n(' ', 2 * self.level));
            match plan {
                LogicalPlan::Projection {
                    expr,
                    schema,
                    input,
                } => {
                    self.output += &format!(
                        "Projection, [{}]",
                        expr.iter()
                            .enumerate()
                            .map(|(i, e)| {
                                let in_name = e.name(input.schema()).unwrap();
                                let out_name = schema.field(i).qualified_name();
                                if in_name != out_name {
                                    format!("{}:{}", in_name, out_name)
                                } else {
                                    in_name
                                }
                            })
                            .join(", ")
                    );
                }
                LogicalPlan::Filter { predicate, .. } => {
                    self.output += "Filter";
                    if self.opts.show_filters {
                        self.output += &format!(", predicate: {:?}", predicate)
                    }
                }
                LogicalPlan::Aggregate { aggr_expr, .. } => {
                    self.output += "Aggregate";
                    if self.opts.show_aggregations {
                        self.output += &format!(", aggs: {:?}", aggr_expr)
                    }
                }
                LogicalPlan::Sort { expr, .. } => {
                    self.output += "Sort";
                    if self.opts.show_sort_by {
                        self.output += &format!(", by: {:?}", expr)
                    }
                }
                LogicalPlan::Union { .. } => self.output += "Union",
                LogicalPlan::Join { on, .. } => {
                    self.output += &format!(
                        "Join on: [{}]",
                        on.iter().map(|(l, r)| format!("{} = {}", l, r)).join(", ")
                    )
                }
                LogicalPlan::Repartition { .. } => self.output += "Repartition",
                LogicalPlan::TableScan {
                    table_name,
                    source,
                    projected_schema,
                    filters,
                    ..
                } => {
                    self.output += &format!(
                        "Scan {}, source: {}",
                        table_name,
                        pp_source(source.as_ref())
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
                }
                LogicalPlan::EmptyRelation { .. } => self.output += "Empty",
                LogicalPlan::Limit { .. } => self.output += "Limit",
                LogicalPlan::Skip { .. } => self.output += "Skip",
                LogicalPlan::CreateExternalTable { .. } => self.output += "CreateExternalTable",
                LogicalPlan::Explain { .. } => self.output += "Explain",
                LogicalPlan::Extension { node } => {
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
                    } else if let Some(topk) = node.as_any().downcast_ref::<ClusterAggregateTopK>()
                    {
                        self.output += &format!("ClusterAggregateTopK, limit: {}", topk.limit);
                        if self.opts.show_aggregations {
                            self.output += &format!(", aggs: {:?}", topk.aggregate_expr)
                        }
                        if self.opts.show_sort_by {
                            self.output += &format!(
                                ", sortBy: {}",
                                pp_sort_columns(topk.group_expr.len(), &topk.order_by)
                            );
                        }
                        if self.opts.show_filters {
                            if let Some(having) = &topk.having_expr {
                                self.output += &format!(", having: {:?}", having)
                            }
                        }
                    } else if let Some(_) = node.as_any().downcast_ref::<PanicWorkerNode>() {
                        self.output += &format!("PanicWorker")
                    } else if let Some(_) = node.as_any().downcast_ref::<RollingWindowAggregate>() {
                        self.output += &format!("RollingWindowAggreagate");
                    } else if let Some(alias) = node.as_any().downcast_ref::<LogicalAlias>() {
                        self.output += &format!("LogicalAlias, alias: {}", alias.alias);
                    } else {
                        log::error!("unknown extension node")
                    }
                }
                LogicalPlan::Window { .. } | LogicalPlan::CrossJoin { .. } => {
                    panic!("unsupported logical plan node")
                }
            }

            self.level += 1;
            Ok(true)
        }

        fn post_visit(&mut self, _plan: &LogicalPlan) -> Result<bool, Self::Error> {
            self.level -= 1;
            Ok(true)
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

fn pp_source(t: &dyn TableProvider) -> String {
    if t.as_any().is::<CubeTableLogical>() {
        "CubeTableLogical".to_string()
    } else if let Some(t) = t.as_any().downcast_ref::<CubeTable>() {
        format!("CubeTable(index: {})", pp_index(t.index_snapshot()))
    } else if let Some(t) = t.as_any().downcast_ref::<InlineTableProvider>() {
        format!("InlineTableProvider(data: {} rows)", t.get_data().len())
    } else {
        panic!("unknown table provider");
    }
}

fn pp_sort_columns(first_agg: usize, cs: &[SortColumn]) -> String {
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
        } else if let Some(agg) = a.downcast_ref::<HashAggregateExec>() {
            let strat = match agg.strategy() {
                AggregateStrategy::Hash => "Hash",
                AggregateStrategy::InplaceSorted => "Inplace",
            };
            let mode = match agg.mode() {
                AggregateMode::Partial => "Partial",
                AggregateMode::Final => "Final",
                AggregateMode::FinalPartitioned => "FinalPartitioned",
                AggregateMode::Full => "Full",
            };
            *out += &format!("{}{}Aggregate", mode, strat);
            if o.show_aggregations {
                *out += &format!(", aggs: {:?}", agg.aggr_expr())
            }
        } else if let Some(l) = a.downcast_ref::<LocalLimitExec>() {
            *out += &format!("LocalLimit, n: {}", l.limit());
        } else if let Some(l) = a.downcast_ref::<GlobalLimitExec>() {
            *out += &format!("GlobalLimit, n: {}", l.limit());
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
        } else if let Some(_) = a.downcast_ref::<MergeExec>() {
            *out += "Merge";
        } else if let Some(_) = a.downcast_ref::<MergeSortExec>() {
            *out += "MergeSort";
        } else if let Some(_) = a.downcast_ref::<MergeReSortExec>() {
            *out += "MergeResort";
        } else if let Some(j) = a.downcast_ref::<MergeJoinExec>() {
            *out += &format!(
                "MergeJoin, on: [{}]",
                j.join_on()
                    .iter()
                    .map(|(l, r)| format!("{} = {}", l, r))
                    .join(", ")
            );
        } else if let Some(j) = a.downcast_ref::<CrossJoinExec>() {
            *out += &format!("CrossJoin, on: {}", j.on)
        } else if let Some(j) = a.downcast_ref::<CrossJoinAggExec>() {
            *out += &format!("CrossJoinAgg, on: {}", j.join.on);
            if o.show_aggregations {
                *out += &format!(", aggs: {:?}", j.agg_expr)
            }
        } else if let Some(_) = a.downcast_ref::<UnionExec>() {
            *out += "Union";
        } else if let Some(_) = a.downcast_ref::<FilterByKeyRangeExec>() {
            *out += "FilterByKeyRange";
        } else if let Some(p) = a.downcast_ref::<ParquetExec>() {
            *out += &format!(
                "ParquetScan, files: {}",
                p.partitions()
                    .iter()
                    .map(|p| p.filenames.iter())
                    .flatten()
                    .join(",")
            );
        } else if let Some(_) = a.downcast_ref::<SkipExec>() {
            *out += "SkipRows";
        } else if let Some(_) = a.downcast_ref::<RollingWindowAggExec>() {
            *out += "RollingWindowAgg";
        } else if let Some(_) = a.downcast_ref::<LastRowByUniqueKeyExec>() {
            *out += "LastRowByUniqueKey";
        } else if let Some(_) = a.downcast_ref::<MemoryExec>() {
            *out += "MemoryScan";
        } else {
            let to_string = format!("{:?}", p);
            *out += &to_string.split(" ").next().unwrap_or(&to_string);
        }

        if o.show_output_hints {
            let hints = p.output_hints();
            if !hints.single_value_columns.is_empty() {
                *out += &format!(", single_vals: {:?}", hints.single_value_columns);
            }
            if let Some(so) = hints.sort_order {
                *out += &format!(", sort_order: {:?}", so);
            }
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
