use crate::queryplanner::optimizations::rewrite_plan::{rewrite_plan, PlanRewriter};
use datafusion::error::DataFusionError;
use datafusion::logical_plan::{Expr, ExprRewriter, LogicalPlan};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::utils::from_plan;
use datafusion::scalar::ScalarValue;
use itertools::Itertools;
use std::convert::TryFrom;
use std::time::SystemTime;

pub struct MaterializeNow;
impl OptimizerRule for MaterializeNow {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let t = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to get current timestamp: {}",
                    e
                )))
            }
        };
        let seconds = match i64::try_from(t.as_secs()) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to convert timestamp to i64: {}",
                    e
                )))
            }
        };
        let nanos = match i64::try_from(t.as_nanos()) {
            Ok(t) => t,
            Err(e) => {
                return Err(DataFusionError::Internal(format!(
                    "Failed to convert timestamp to i64: {}",
                    e
                )))
            }
        };
        return rewrite_plan(plan, &(), &mut Rewriter { seconds, nanos });

        #[derive(Clone)]
        struct Rewriter {
            seconds: i64,
            nanos: i64,
        }
        impl ExprRewriter for Rewriter {
            fn mutate(&mut self, expr: Expr) -> Result<Expr, DataFusionError> {
                match expr {
                    Expr::ScalarUDF { fun, args }
                        if fun.name.eq_ignore_ascii_case("now")
                            || fun.name.eq_ignore_ascii_case("unix_timestamp") =>
                    {
                        if args.len() != 0 {
                            return Err(DataFusionError::Plan(format!(
                                "NOW() must have 0 arguments, got {}",
                                args.len()
                            )));
                        }
                        let v = if fun.name.eq_ignore_ascii_case("now") {
                            ScalarValue::TimestampNanosecond(Some(self.nanos))
                        } else {
                            // unix_timestamp
                            ScalarValue::Int64(Some(self.seconds))
                        };
                        Ok(Expr::Literal(v))
                    }
                    _ => Ok(expr),
                }
            }
        }

        impl PlanRewriter for Rewriter {
            type Context = ();

            fn rewrite(&mut self, n: LogicalPlan, _: &()) -> Result<LogicalPlan, DataFusionError> {
                let mut exprs = n.expressions();
                for e in &mut exprs {
                    *e = std::mem::replace(e, Expr::Wildcard).rewrite(self)?
                }
                from_plan(&n, &exprs, &n.inputs().into_iter().cloned().collect_vec())
            }
        }
    }

    fn name(&self) -> &str {
        todo!()
    }
}
