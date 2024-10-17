use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbolType};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

pub struct TimeShiftSqlNode {
    shifts: HashMap<String, String>,
    input: Rc<dyn SqlNode>,
}

impl TimeShiftSqlNode {
    pub fn new(shifts: HashMap<String, String>, input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { shifts, input })
    }
}

impl SqlNode for TimeShiftSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let input = self.input.to_sql(visitor, node, query_tools.clone())?;
        let res = match node.symbol() {
            MemberSymbolType::Dimension(ev) => {
                if let Some(shift) = self.shifts.get(&ev.full_name()) {
                    format!("({input} + interval '{shift}')")
                } else {
                    input
                }
            }
            _ => input,
        };
        Ok(res)
    }
}
