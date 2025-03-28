use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MeasureFilterSqlNode {
    input: Rc<dyn SqlNode>,
}

impl MeasureFilterSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for MeasureFilterSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let input = self.input.to_sql(
            visitor,
            node,
            query_tools.clone(),
            node_processor.clone(),
            templates,
        )?;
        let res = match node.as_ref() {
            MemberSymbol::Measure(ev) => {
                let measure_filters = ev.measure_filters();
                if !measure_filters.is_empty() {
                    let filters = measure_filters
                        .iter()
                        .map(|filter| -> Result<String, CubeError> {
                            Ok(format!(
                                "({})",
                                filter.eval(
                                    &visitor,
                                    node_processor.clone(),
                                    query_tools.clone(),
                                    templates
                                )?
                            ))
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" AND ");
                    let result = if input.as_str() == "*" {
                        "1".to_string()
                    } else {
                        input
                    };
                    format!("CASE WHEN {} THEN {} END", filters, result)
                } else {
                    input
                }
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
