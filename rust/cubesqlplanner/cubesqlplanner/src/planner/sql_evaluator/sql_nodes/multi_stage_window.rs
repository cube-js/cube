use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{MemberSymbol, SqlEvaluatorVisitor};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MultiStageWindowNode {
    input: Rc<dyn SqlNode>,
    else_processor: Rc<dyn SqlNode>,
    partition: Vec<String>,
}

impl MultiStageWindowNode {
    pub fn new(
        input: Rc<dyn SqlNode>,
        else_processor: Rc<dyn SqlNode>,
        partition: Vec<String>,
    ) -> Rc<Self> {
        Rc::new(Self {
            input,
            else_processor,
            partition,
        })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }

    pub fn else_processor(&self) -> &Rc<dyn SqlNode> {
        &self.else_processor
    }

    pub fn partition(&self) -> &Vec<String> {
        &self.partition
    }
}

impl SqlNode for MultiStageWindowNode {
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
                if m.is_multi_stage() && !m.is_calculated() {
                    let input_sql = self.input.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                        templates,
                    )?;

                    let partition_by = if self.partition.is_empty() {
                        "".to_string()
                    } else {
                        format!("PARTITION BY {} ", self.partition.join(", "))
                    };
                    let measure_type = m.measure_type();
                    format!("{measure_type}({measure_type}({input_sql})) OVER ({partition_by})")
                } else {
                    self.else_processor.to_sql(
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
                    "Unexpected evaluation node type for MultStageWindowNode"
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone(), self.else_processor.clone()]
    }
}
