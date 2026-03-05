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
            MemberSymbol::CubeTable(ev) => ev.evaluate_sql(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
            MemberSymbol::CubeName(ev) => ev.evaluate_sql(),
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
