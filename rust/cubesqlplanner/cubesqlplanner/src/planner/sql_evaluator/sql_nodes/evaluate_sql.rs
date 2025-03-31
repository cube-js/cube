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
        match node.as_ref() {
            MemberSymbol::Dimension(ev) => ev.evaluate_sql(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
            MemberSymbol::TimeDimension(ev) => {
                let res = visitor.apply(&ev.base_symbol(), node_processor.clone(), templates)?;
                Ok(res)
            }
            MemberSymbol::Measure(ev) => {
                let res = if ev.has_sql() {
                    ev.evaluate_sql(
                        visitor,
                        node_processor.clone(),
                        query_tools.clone(),
                        templates,
                    )?
                } else if ev.pk_sqls().len() > 1 {
                    let pk_strings = ev
                        .pk_sqls()
                        .iter()
                        .map(|pk| -> Result<_, CubeError> {
                            let res = pk.eval(
                                &visitor,
                                node_processor.clone(),
                                query_tools.clone(),
                                templates,
                            )?;
                            templates.cast_to_string(&res)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    templates.concat_strings(&pk_strings)?
                } else if ev.pk_sqls().len() == 1 {
                    let pk_sql = ev.pk_sqls().first().unwrap();
                    pk_sql.eval(
                        &visitor,
                        node_processor.clone(),
                        query_tools.clone(),
                        templates,
                    )?
                } else {
                    format!("*")
                };
                Ok(res)
            }
            MemberSymbol::CubeTable(ev) => ev.evaluate_sql(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
            MemberSymbol::CubeName(ev) => ev.evaluate_sql(),
            MemberSymbol::MemberExpression(e) => e.evaluate_sql(
                visitor,
                node_processor.clone(),
                query_tools.clone(),
                templates,
            ),
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![]
    }
}
