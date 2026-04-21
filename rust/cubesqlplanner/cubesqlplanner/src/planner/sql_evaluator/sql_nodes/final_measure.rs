use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::symbols::AggregateWrap;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashSet;
use std::rc::Rc;

pub struct FinalMeasureSqlNode {
    input: Rc<dyn SqlNode>,
    rendered_as_multiplied_measures: HashSet<String>,
    count_approx_as_state: bool,
}

impl FinalMeasureSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        rendered_as_multiplied_measures: HashSet<String>,
        count_approx_as_state: bool,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            rendered_as_multiplied_measures,
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
                let is_multiplied = self
                    .rendered_as_multiplied_measures
                    .contains(&ev.full_name());
                let wrap = ev.kind().aggregate_wrap(is_multiplied);
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
