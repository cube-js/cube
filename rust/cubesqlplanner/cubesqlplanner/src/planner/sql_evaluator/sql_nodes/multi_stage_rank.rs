use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

pub struct MultiStageRankNode {
    else_processor: Rc<dyn SqlNode>,
    partition: Vec<String>,
}

impl MultiStageRankNode {
    pub fn new(else_processor: Rc<dyn SqlNode>, partition: Vec<String>) -> Rc<Self> {
        Rc::new(Self {
            else_processor,
            partition,
        })
    }

    pub fn else_processor(&self) -> &Rc<dyn SqlNode> {
        &self.else_processor
    }

    pub fn partition(&self) -> &Vec<String> {
        &self.partition
    }
}

impl SqlNode for MultiStageRankNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
    ) -> Result<String, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(m) => {
                if m.is_multi_stage() && m.measure_type() == "rank" {
                    let order_by = if !m.measure_order_by().is_empty() {
                        let sql = m
                            .measure_order_by()
                            .iter()
                            .map(|item| -> Result<String, CubeError> {
                                let sql = visitor
                                    .apply(item.evaluation_node(), node_processor.clone())?;
                                Ok(format!("{} {}", sql, item.direction()))
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join(", ");
                        format!("ORDER BY {sql}")
                    } else {
                        "".to_string()
                    };
                    let partition_by = if self.partition.is_empty() {
                        "".to_string()
                    } else {
                        format!("PARTITION BY {} ", self.partition.join(", "))
                    };
                    format!("rank() OVER ({partition_by}{order_by})")
                } else {
                    self.else_processor.to_sql(
                        visitor,
                        node,
                        query_tools.clone(),
                        node_processor.clone(),
                    )?
                }
            }
            _ => {
                return Err(CubeError::internal(format!(
                    "Unexpected evaluation node type for MultStageRankNode"
                )));
            }
        };
        Ok(res)
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.else_processor.clone()]
    }
}
