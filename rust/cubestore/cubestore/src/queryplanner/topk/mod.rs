mod execute;
mod plan;
mod util;

use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion_proto::bytes::Serializeable;
pub use execute::AggregateTopKExec;
pub use plan::materialize_topk;
pub use plan::plan_topk;
pub use plan::DummyTopKLowerExec;

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

/// Aggregates input by [group_expr], sorts with [order_by] and returns [limit] first elements. The
/// output schema must have exactly columns for results of [group_expr] followed by results of
/// [aggregate_expr].  This is split in two nodes, so that DF's type_coercion analysis pass can
/// handle `having_expr` with the proper schema (the output schema of the Lower node).  This also
/// includes `order_by` and `limit` just because that seems better-organized, but what it really
/// needs is `having_expr`.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ClusterAggregateTopKUpper {
    // input is always a ClusterAggregateTopKLower node
    pub input: Arc<LogicalPlan>,
    pub limit: usize,
    pub order_by: Vec<SortColumn>,
    pub having_expr: Option<Expr>,
}

/// `ClusterAggregateTopKUpper`'s lower half.  This can't be used on its own -- it needs to be
/// planned together with its upper half, `ClusterAggregateTopKUpper`.
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct ClusterAggregateTopKLower {
    pub input: Arc<LogicalPlan>,
    pub group_expr: Vec<Expr>,
    pub aggregate_expr: Vec<Expr>,
    pub schema: DFSchemaRef,
    pub snapshots: Vec<Snapshots>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterAggregateTopKUpperSerialized {
    limit: usize,
    order_by: Vec<SortColumn>,
    // Option<Expr>
    having_expr: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterAggregateTopKLowerSerialized {
    // Vec<Expr>
    group_expr: Vec<Vec<u8>>,
    // Vec<Expr>
    aggregate_expr: Vec<Vec<u8>>,
    snapshots: Vec<Snapshots>,
}

impl ClusterAggregateTopKUpper {
    pub fn from_serialized(
        serialized: ClusterAggregateTopKUpperSerialized,
        inputs: &[LogicalPlan],
        registry: &dyn FunctionRegistry,
    ) -> Result<ClusterAggregateTopKUpper, CubeError> {
        assert_eq!(inputs.len(), 1);
        let input = Arc::new(inputs[0].clone());
        let having_expr: Option<Expr> = serialized
            .having_expr
            .map(|e| Expr::from_bytes_with_registry(e.as_slice(), registry))
            .transpose()?;
        Ok(ClusterAggregateTopKUpper {
            input,
            limit: serialized.limit,
            order_by: serialized.order_by,
            having_expr,
        })
    }

    pub fn to_serialized(&self) -> Result<ClusterAggregateTopKUpperSerialized, CubeError> {
        Ok(ClusterAggregateTopKUpperSerialized {
            limit: self.limit,
            order_by: self.order_by.clone(),
            having_expr: self
                .having_expr
                .as_ref()
                .map(|e| e.to_bytes().map(|b| b.to_vec()))
                .transpose()?,
        })
    }
}

impl ClusterAggregateTopKLower {
    pub fn from_serialized(
        serialized: ClusterAggregateTopKLowerSerialized,
        inputs: &[LogicalPlan],
        registry: &dyn FunctionRegistry,
    ) -> Result<ClusterAggregateTopKLower, CubeError> {
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
        let schema = datafusion::logical_expr::Aggregate::try_new(
            input.clone(),
            group_expr.clone(),
            aggregate_expr.clone(),
        )?
        .schema;
        Ok(ClusterAggregateTopKLower {
            input,
            group_expr,
            aggregate_expr,
            schema,
            snapshots: serialized.snapshots,
        })
    }

    pub fn to_serialized(&self) -> Result<ClusterAggregateTopKLowerSerialized, CubeError> {
        Ok(ClusterAggregateTopKLowerSerialized {
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

impl UserDefinedLogicalNode for ClusterAggregateTopKUpper {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ClusterAggregateTopKUpper"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut res = Vec::new();
        if self.having_expr.is_some() {
            res.push(self.having_expr.clone().unwrap());
        }
        res
    }

    fn fmt_for_explain<'a>(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(
            f,
            "ClusterAggregateTopKUpper, limit = {}, sortBy = {:?}",
            self.limit, self.order_by,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>, DataFusionError> {
        assert_eq!(inputs.len(), 1);
        assert_eq!(usize::from(self.having_expr.is_some()), exprs.len());

        let input: LogicalPlan = inputs.into_iter().next().unwrap();

        let having_expr = if self.having_expr.is_some() {
            Some(exprs.into_iter().next().unwrap())
        } else {
            None
        };
        Ok(Arc::new(ClusterAggregateTopKUpper {
            input: Arc::new(input),
            limit: self.limit,
            order_by: self.order_by.clone(),
            having_expr,
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

impl UserDefinedLogicalNode for ClusterAggregateTopKLower {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ClusterAggregateTopKLower"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        let res = self
            .group_expr
            .iter()
            .chain(&self.aggregate_expr)
            .cloned()
            .collect_vec();
        res
    }

    fn fmt_for_explain<'a>(&self, f: &mut Formatter<'a>) -> std::fmt::Result {
        write!(
            f,
            "ClusterAggregateTopKLower, groupBy = {:?}, aggr = {:?}",
            self.group_expr, self.aggregate_expr
        )
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Arc<dyn UserDefinedLogicalNode>, DataFusionError> {
        let num_groups = self.group_expr.len();
        let num_aggs = self.aggregate_expr.len();

        assert_eq!(inputs.len(), 1);
        assert_eq!(exprs.len(), num_groups + num_aggs);

        let input = inputs.into_iter().next().unwrap();

        Ok(Arc::new(ClusterAggregateTopKLower {
            input: Arc::new(input),
            group_expr: Vec::from(&exprs[0..num_groups]),
            aggregate_expr: Vec::from(&exprs[num_groups..num_groups + num_aggs]),
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
