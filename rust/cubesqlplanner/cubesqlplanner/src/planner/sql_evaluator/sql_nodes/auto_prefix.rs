use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_evaluator::{EvaluationNode, MemberSymbol, MemberSymbolType};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct AutoPrefixSqlNode {
    input: Rc<dyn SqlNode>,
}

impl AutoPrefixSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }
}

impl SqlNode for AutoPrefixSqlNode {
    fn to_sql(
        &self,
        visitor: &mut SqlEvaluatorVisitor,
        node: &Rc<EvaluationNode>,
        query_tools: Rc<QueryTools>,
    ) -> Result<String, CubeError> {
        let prefix = visitor.cube_alias_prefix().clone();
        let input = self.input.to_sql(visitor, node, query_tools.clone())?;
        let res = match node.symbol() {
            MemberSymbolType::Dimension(ev) => {
                query_tools.auto_prefix_with_cube_name(&ev.cube_name(), &input, &prefix)
            }
            MemberSymbolType::Measure(ev) => {
                query_tools.auto_prefix_with_cube_name(&ev.cube_name(), &input, &prefix)
            }
            MemberSymbolType::CubeName(_) => {
                query_tools.escape_column_name(&query_tools.cube_alias_name(&input, &prefix))
            }
            _ => input,
        };
        Ok(res)
    }
}
