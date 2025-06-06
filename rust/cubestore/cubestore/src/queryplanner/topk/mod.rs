mod execute;
mod plan;
mod util;

use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion_proto::bytes::Serializeable;
pub use execute::AggregateTopKExec;
pub use plan::materialize_topk;
pub use plan::plan_topk;

use crate::queryplanner::planning::Snapshots;
use crate::CubeError;
use datafusion::arrow::compute::SortOptions;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use itertools::Itertools;
use serde_derive::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

/// Workers will split their local results into batches of at least this size.
pub const MIN_TOPK_STREAM_ROWS: usize = 1024;

/// Aggregates input by [group_expr], sorts with [order_by] and returns [limit] first elements.
/// The output schema must have exactly columns for results of [group_expr] followed by results
/// of [aggregate_expr].
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ClusterAggregateTopK {
    pub limit: usize,
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<Expr>,
    pub aggregate_expr: Vec<Expr>,
    pub order_by: Vec<SortColumn>,
    pub having_expr: Option<Expr>,
    pub schema: DFSchemaRef,
    pub snapshots: Vec<Snapshots>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterAggregateTopKSerialized {
    limit: usize,
    // Vec<Expr>
    group_expr: Vec<Vec<u8>>,
    // Vec<Expr>
    aggregate_expr: Vec<Vec<u8>>,
    order_by: Vec<SortColumn>,
    // Option<Expr>
    having_expr: Option<Vec<u8>>,
    snapshots: Vec<Snapshots>,
}

impl ClusterAggregateTopK {
    pub fn from_serialized(
        serialized: ClusterAggregateTopKSerialized,
        inputs: &[LogicalPlan],
        registry: &dyn FunctionRegistry,
    ) -> Result<ClusterAggregateTopK, CubeError> {
        assert_eq!(inputs.len(), 1);
        let input = Arc::new(inputs[0].clone());
        let group_expr = serialized
            .group_expr
            .into_iter()
            .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
            .collect::<Result<Vec<_>, _>>()?;
        let aggregate_expr = serialized
            .aggregate_expr
            .into_iter()
            .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
            .collect::<Result<Vec<_>, _>>()?;
        let having_expr: Option<Expr> = serialized
            .having_expr
            .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
            .transpose()?;
        let schema = datafusion::logical_expr::Aggregate::try_new(
            input.clone(),
            group_expr.clone(),
            aggregate_expr.clone(),
        )?
        .schema;
        Ok(ClusterAggregateTopK {
            input,
            limit: serialized.limit,
            group_expr,
            aggregate_expr,
            order_by: serialized.order_by,
            having_expr,
            schema,
            snapshots: serialized.snapshots,
        })
    }

    pub fn to_serialized(&self) -> Result<ClusterAggregateTopKSerialized, CubeError> {
        Ok(ClusterAggregateTopKSerialized {
            limit: self.limit,
            group_expr: self
                .group_expr
                .iter()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .collect::<Result<Vec<_>, _>>()?,
            aggregate_expr: self
                .aggregate_expr
                .iter()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .collect::<Result<Vec<_>, _>>()?,
            order_by: self.order_by.clone(),
            having_expr: self
                .having_expr
                .as_ref()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .transpose()?,
            snapshots: self.snapshots.clone(),
        })
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Hash)]
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

impl UserDefinedLogicalNode for ClusterAggregateTopK {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ClusterAggregateTopK"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut res = self
            .group_expr
            .iter()
            .chain(&self.aggregate_expr)
            .cloned()
            .collect_vec();
        // TODO upgrade DF: DF's type_coercion analysis pass doesn't like these exprs (which are
        // defined on the aggregate's output schema instead of the input schema).  Maybe we should
        // split ClusterAggregateTopK into separate logical nodes.  Instead we (hackishly) use
        // upper_expressions.
        if false && self.having_expr.is_some() {
            res.push(self.having_expr.clone().unwrap());
        }
        res
    }

    // Cube extension.
    fn upper_expressions(&self) -> Vec<Expr> {
        if let Some(e) = &self.having_expr {
            vec![e.clone()]
        } else {
            vec![]
        }
    }

    // Cube extension.
    fn with_upper_expressions(
        &self,
        upper_exprs: Vec<Expr>,
    ) -> Result<Option<Arc<dyn UserDefinedLogicalNode>>, DataFusionError> {
        assert_eq!(usize::from(self.having_expr.is_some()), upper_exprs.len());
        if self.having_expr.is_some() {
            let having_expr = Some(upper_exprs.into_iter().next().unwrap());
            Ok(Some(Arc::new(ClusterAggregateTopK {
                limit: self.limit,
                input: self.input.clone(),
                group_expr: self.group_expr.clone(),
                aggregate_expr: self.aggregate_expr.clone(),
                order_by: self.order_by.clone(),
                having_expr,
                schema: self.schema.clone(),
                snapshots: self.snapshots.clone(),
            })))
        } else {
            Ok(None)
        }
    }

    fn fmt_for_explain<'a>(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(
            f,
            "ClusterAggregateTopK, limit = {}, groupBy = {:?}, aggr = {:?}, sortBy = {:?}",
            self.limit, self.group_expr, self.aggregate_expr, self.order_by
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>, DataFusionError> {
        let num_groups = self.group_expr.len();
        let num_aggs = self.aggregate_expr.len();

        // TODO upgrade DF: See expressions() comment; having_expr is part of the
        // upper_expressions() -- we make the having expressions be "invisible" because they're
        // defined on the output schema.

        // let num_having = if self.having_expr.is_some() { 1 } else { 0 };
        assert_eq!(inputs.len(), 1);
        assert_eq!(exprs.len(), num_groups + num_aggs /* + num_having */); /* TODO upgrade DF */

        // let having_expr = if self.having_expr.is_some() {
        //     exprs.last().map(|p| p.clone())
        // } else {
        //     None
        // };
        let having_expr = self.having_expr.clone();
        Ok(Arc::new(ClusterAggregateTopK {
            limit: self.limit,
            input: Arc::new(inputs[0].clone()),
            group_expr: Vec::from(&exprs[0..num_groups]),
            aggregate_expr: Vec::from(&exprs[num_groups..num_groups + num_aggs]),
            order_by: self.order_by.clone(),
            having_expr,
            schema: self.schema.clone(),
            snapshots: self.snapshots.clone(),
        }))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut state = state;
        self.hash(&mut state);
    }

    fn dyn_eq(&self, other: &dyn UserDefinedLogicalNode) -> bool {
        other
            .as_any()
            .downcast_ref()
            .map(|s| self.eq(s))
            .unwrap_or(false)
    }
}
