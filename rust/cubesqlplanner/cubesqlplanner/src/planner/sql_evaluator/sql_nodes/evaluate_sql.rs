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
        // Data masking: if a dimension or measure is masked, return the mask SQL
        // instead of evaluating the member's regular SQL.
        match node.as_ref() {
            MemberSymbol::Dimension(_) | MemberSymbol::Measure(_) => {
                let full_name = node.full_name();
                if query_tools.is_member_masked(&full_name) {
                    return query_tools.resolve_mask_sql(&full_name);
                }
            }
            _ => {}
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
