use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct EvaluateSqlNode {}

impl EvaluateSqlNode {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {})
    }
}

impl SqlNode for EvaluateSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        // Data masking: if a dimension or measure is masked and has a compiled
        // mask SQL template, evaluate it instead of the member's regular SQL.
        if let Some(mask_call) = node.mask_sql() {
            let full_name = node.full_name();
            if query_tools.is_member_masked(&full_name) {
                return mask_call.eval(
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                );
            }
        }

        let res = match node.as_ref() {
            MemberSymbol::Dimension(ev) => {
                let res = ev.evaluate_sql(
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                )?;
                Ok(res)
            }
            MemberSymbol::TimeDimension(ev) => {
                let res = visitor.apply(&ev.base_symbol(), node_processor.clone(), templates)?;
                Ok(res)
            }
            MemberSymbol::Measure(ev) => ev.evaluate_sql(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
            MemberSymbol::MemberExpression(e) => {
                let res = e.evaluate_sql(
                    visitor,
                    node_processor.clone(),
                    query_tools.clone(),
                    templates,
                )?;
                Ok(res)
            }
        }?;
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
