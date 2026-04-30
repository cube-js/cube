use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;

pub struct OriginalSqlPreAggregationSqlNode {
    input: Rc<dyn SqlNode>,
    original_sql_pre_aggregations: HashMap<String, String>,
}

impl OriginalSqlPreAggregationSqlNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        original_pre_aggregations: HashMap<String, String>,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            original_sql_pre_aggregations: original_pre_aggregations,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for OriginalSqlPreAggregationSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let res = match node.as_ref() {
            MemberSymbol::CubeTable(ev) => {
                if let Some(original_sql_table_name) =
                    self.original_sql_pre_aggregations.get(ev.cube_name())
                {
                    format!("{}", original_sql_table_name)
                } else {
                    self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "OriginalSqlPreAggregationSqlNode node processor called for wrong node",
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
