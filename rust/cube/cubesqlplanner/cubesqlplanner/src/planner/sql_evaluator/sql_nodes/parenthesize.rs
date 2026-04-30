use super::SqlNode;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_evaluator::MemberSymbol;
use crate::planner::sql_evaluator::SqlEvaluatorVisitor;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::utils::sql_expression_scanner::is_top_level_compound;
use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// Wraps the child's rendered SQL in parentheses when the visitor signals that
/// the surrounding context expects a parentheses-safe argument (for example, a
/// `SqlCall` substitution into an arithmetic or logical position) and the
/// rendered expression is compound at the top level.
///
/// Sits immediately above [`AutoPrefixSqlNode`] in the processor chain — the
/// lowest point where renaming is complete. Higher-layer nodes that wrap the
/// child's output in a syntactically safe construct (aggregate, window
/// function, CASE/DATE_TRUNC/CONVERT_TZ, etc.) should reset
/// `arg_needs_paren_safe` on the visitor before recursing, so this node avoids
/// scanning output that will be discarded.
pub struct ParenthesizeSqlNode {
    input: Rc<dyn SqlNode>,
}

impl ParenthesizeSqlNode {
    pub fn new(input: Rc<dyn SqlNode>) -> Rc<Self> {
        Rc::new(Self { input })
    }

    pub fn input(&self) -> &Rc<dyn SqlNode> {
        &self.input
    }
}

impl SqlNode for ParenthesizeSqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let input_sql = self
            .input
            .to_sql(visitor, node, query_tools, node_processor, templates)?;
        if visitor.arg_needs_paren_safe() && is_top_level_compound(&input_sql) {
            Ok(format!("({})", input_sql))
        } else {
            Ok(input_sql)
        }
    }

    fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
        self.clone()
    }

    fn childs(&self) -> Vec<Rc<dyn SqlNode>> {
        vec![self.input.clone()]
    }
}
