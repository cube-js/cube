use super::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::AggregateWrap;
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Applies the final aggregation wrap to a measure (sum / avg /
/// count_distinct / pass-through, etc.) using `MeasureKind::aggregate_wrap`.
/// Routes `count_distinct_approx` through an HLL state when requested.
pub struct FinalMeasureSqlNode {
    input: Rc<dyn SqlNode>,
    count_approx_as_state: bool,
}

impl FinalMeasureSqlNode {
    pub fn new(input: Rc<dyn SqlNode>, count_approx_as_state: bool) -> Rc<Self> {
        Rc::new(Self {
            input,
            count_approx_as_state,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    fn apply_wrap(
        &self,
        wrap: AggregateWrap,
        input: String,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        match wrap {
            AggregateWrap::PassThrough => Ok(input),
            AggregateWrap::Function(name) => Ok(format!("{}({})", name, input)),
            AggregateWrap::CountDistinct => templates.count_distinct(&input),
            AggregateWrap::CountDistinctApprox => {
                if self.count_approx_as_state {
                    templates.hll_init(input)
                } else {
                    templates.count_distinct_approx(input)
                }
            }
        }
    }
}

impl SqlNode for FinalMeasureSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::Measure(ev) => {
                let wrap = ev.kind().aggregate_wrap();
                let child_visitor = match wrap {
                    AggregateWrap::PassThrough => visitor.clone(),
                    _ => visitor.with_arg_needs_paren_safe(false),
                };
                let input = self.input.to_sql(
                    &child_visitor,
                    node,
                    query_tools.clone(),
                    node_processor.clone(),
                    templates,
                )?;
                self.apply_wrap(wrap, input, templates)?
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Measure filter node processor called for wrong node",
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
