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
        let source_schema = visitor.source_schema().clone();
        let input = self.input.to_sql(visitor, node, query_tools.clone())?;
        let res = match node.symbol() {
            MemberSymbolType::Dimension(ev) => {
                let cube_alias = source_schema.resolve_cube_alias(&ev.cube_name());
                query_tools.auto_prefix_with_cube_name(&cube_alias, &input)
            }
            MemberSymbolType::Measure(ev) => {
                let cube_alias = source_schema.resolve_cube_alias(&ev.cube_name());
                query_tools.auto_prefix_with_cube_name(&cube_alias, &input)
            }
            MemberSymbolType::CubeName(_) => {
                let cube_alias = source_schema.resolve_cube_alias(&input);
                query_tools.escape_column_name(&cube_alias)
            }
            _ => input,
        };
        Ok(res)
    }
}
