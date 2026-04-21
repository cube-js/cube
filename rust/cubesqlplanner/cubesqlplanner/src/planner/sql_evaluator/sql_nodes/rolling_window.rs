use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::symbols::{AggregationType, MeasureKind};
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct RollingWindowNode {
    input: Rc<dyn SqlNode>,
    default_processor: Rc<dyn SqlNode>,
}

impl RollingWindowNode {
    pub fn new(input: Rc<dyn SqlNode>, default_processor: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self {
            input,
            default_processor,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for RollingWindowNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(m) => {
                let kind = m.kind();
                let wraps_child = m.is_cumulative()
                    && match kind {
                        MeasureKind::Aggregated(a) => matches!(
                            a.agg_type(),
                            AggregationType::CountDistinctApprox
                                | AggregationType::Sum
                                | AggregationType::RunningTotal
                                | AggregationType::Min
                                | AggregationType::Max
                        ),
                        MeasureKind::Count(_) => true,
                        _ => false,
                    };
                if wraps_child {
                    let inner_visitor = visitor.with_arg_needs_paren_safe(false);
                    let input = self.input.to_sql(
                        &inner_visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?;
                    match kind {
                        MeasureKind::Aggregated(a)
                            if a.agg_type() == AggregationType::CountDistinctApprox =>
                        {
                            templates.hll_cardinality_merge(input)?
                        }
                        MeasureKind::Count(_) => format!("sum({})", input),
                        MeasureKind::Aggregated(a) => match a.agg_type() {
                            AggregationType::Sum | AggregationType::RunningTotal => {
                                format!("sum({})", input)
                            }
                            AggregationType::Min | AggregationType::Max => {
                                format!("{}({})", a.agg_type().as_str(), input)
                            }
                            _ => unreachable!(),
                        },
                        _ => unreachable!(),
                    }
                } else {
                    // Delegates to the default processor without adding a wrap —
                    // visitor propagates unchanged.
                    self.default_processor.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor,
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Unexpected evaluation node type for RollingWindowNode"
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
