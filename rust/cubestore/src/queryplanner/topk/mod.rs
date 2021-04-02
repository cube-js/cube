mod execute;
mod plan;

pub use execute::AggregateTopKExec;
pub use plan::materialize_topk;
pub use plan::plan_topk;

use crate::queryplanner::serialized_plan::IndexSnapshot;
use arrow::compute::SortOptions;
use datafusion::logical_plan::{DFSchemaRef, Expr, LogicalPlan, UserDefinedLogicalNode};
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Aggregates input by [group_expr], sorts with [order_by] and returns [limit] first elements.
/// The output schema must have exactly columns for results of [group_expr] followed by results
/// of [aggregate_expr].
#[derive(Debug)]
pub struct ClusterAggregateTopK {
    pub limit: usize,
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<Expr>,
    pub aggregate_expr: Vec<Expr>,
    pub order_by: Vec<SortColumn>,
    pub schema: DFSchemaRef,
    pub snapshots: Vec<Vec<IndexSnapshot>>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SortColumn {
    /// Index of the column in the output schema.
    pub agg_index: usize,
    pub asc: bool,
    pub nulls_first: bool,
}

impl SortColumn {
    fn sort_options(&self) -> SortOptions {
        SortOptions {
            descending: !self.asc,
            nulls_first: self.nulls_first,
        }
    }
}

impl Display for SortColumn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.agg_index.fmt(f)?;
        if !self.asc {
            f.write_str(" desc")?;
        }
        if !self.nulls_first {
            f.write_str(" nulls last")?;
        }
        Ok(())
    }
}

impl ClusterAggregateTopK {
    pub fn into_plan(self) -> LogicalPlan {
        LogicalPlan::Extension {
            node: Arc::new(self),
        }
    }
}

impl UserDefinedLogicalNode for ClusterAggregateTopK {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(&self.aggregate_expr)
            .cloned()
            .collect_vec()
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(
            f,
            "ClusterAggregateTopK, limit = {}, groupBy = {:?}, aggr = {:?}, sortBy = {:?}",
            self.limit, self.group_expr, self.aggregate_expr, self.order_by
        )
    }

    fn from_template(
        &self,
        exprs: &Vec<Expr>,
        inputs: &Vec<LogicalPlan>,
    ) -> Arc<dyn UserDefinedLogicalNode + Send + Sync> {
        let num_groups = self.group_expr.len();
        let num_aggs = self.aggregate_expr.len();
        assert_eq!(inputs.len(), 1);
        assert_eq!(exprs.len(), num_groups + num_aggs);
        Arc::new(ClusterAggregateTopK {
            limit: self.limit,
            input: Arc::new(inputs[0].clone()),
            group_expr: Vec::from(&exprs[0..num_groups]),
            aggregate_expr: Vec::from(&exprs[num_groups..num_groups + num_aggs]),
            order_by: self.order_by.clone(),
            schema: self.schema.clone(),
            snapshots: self.snapshots.clone(),
        })
    }
}
