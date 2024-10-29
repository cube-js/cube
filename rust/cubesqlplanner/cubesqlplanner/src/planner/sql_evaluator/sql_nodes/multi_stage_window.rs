use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType, SqlEvaluatorVisitor};
use cubenativeutils::CubeError;
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
}

impl SqlNode for MultiStageWindowNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let res = match node.symbol() {
            MemberSymbolType::Measure(m) => {
                if m.is_multi_stage() && !m.is_calculated() {
                    let input_sql = self.input.to_sql(visitor, node, query_tools.clone())?;

                    let partition_by = if self.partition.is_empty() {
                        "".to_string()
                    } else {
                        format!("PARTITION BY {} ", self.partition.join(", "))
                    };
                    let measure_type = m.measure_type();
                    format!("{measure_type}({measure_type}({input_sql})) OVER ({partition_by})")
                } else {
                    self.else_processor
                        .to_sql(visitor, node, query_tools.clone())?
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
}
