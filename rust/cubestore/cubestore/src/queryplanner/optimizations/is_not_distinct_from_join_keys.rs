//! Fold `a IS NOT DISTINCT FROM b` join predicates into equi-keys with
//! `null_equals_null = true` so the planner can pick a HashJoin instead of
//! a NestedLoopJoin.
//!
//! Conservative scope: only fires when the `Join` has no existing equi-keys
//! in `on`, so flipping `null_equals_null` cannot change semantics of any
//! `=` predicates already extracted by DataFusion.

use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::expr_schema::ExprSchemable;
use datafusion::logical_expr::utils::{
    can_hash, find_valid_equijoin_key_pair, split_conjunction_owned,
};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};

#[derive(Debug)]
pub struct IsNotDistinctFromJoinKeysRule {}

impl OptimizerRule for IsNotDistinctFromJoinKeysRule {
    fn name(&self) -> &str {
        "is_not_distinct_from_join_keys"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(mut join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !join.on.is_empty() || join.filter.is_none() {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        // Safe: checked above. Keep the original around so we can put it back
        // verbatim if nothing gets lifted — split_conjunction_owned + reduce
        // would otherwise produce a structurally-different (but semantically
        // equivalent) tree.
        let filter_orig = join.filter.take().unwrap();

        let left_schema = join.left.schema().as_ref();
        let right_schema = join.right.schema().as_ref();

        let mut new_on: Vec<(Expr, Expr)> = Vec::new();
        let mut remaining: Vec<Expr> = Vec::new();

        for expr in split_conjunction_owned(filter_orig.clone()) {
            let lifted = match &expr {
                Expr::BinaryExpr(BinaryExpr {
                    left,
                    op: Operator::IsNotDistinctFrom,
                    right,
                }) => find_valid_equijoin_key_pair(left, right, left_schema, right_schema)?
                    .and_then(|(l, r)| {
                        let l_ty = l.get_type(left_schema).ok()?;
                        let r_ty = r.get_type(right_schema).ok()?;
                        if can_hash(&l_ty) && can_hash(&r_ty) {
                            Some((l, r))
                        } else {
                            None
                        }
                    }),
                _ => None,
            };
            match lifted {
                Some(pair) => new_on.push(pair),
                None => remaining.push(expr),
            }
        }

        if new_on.is_empty() {
            join.filter = Some(filter_orig);
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }

        join.on = new_on;
        join.filter = remaining.into_iter().reduce(Expr::and);
        join.null_equals_null = true;
        Ok(Transformed::yes(LogicalPlan::Join(join)))
    }
}
