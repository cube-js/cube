use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::MemberSymbol;

use cubenativeutils::CubeError;
use std::any::Any;
use std::rc::Rc;

/// One link in the SQL-rendering chain. Given a `MemberSymbol` and
/// the surrounding context, returns the rendered SQL — either by
/// computing it directly or by delegating up the chain through
/// `node_processor`.
pub trait SqlNode {
    fn to_sql(
        &self,
        visitor: &SqlEvaluatorVisitor,
        node: &Rc<MemberSymbol>,
        query_tools: Rc<QueryTools>,
        node_processor: Rc<dyn SqlNode>,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError>;

    fn as_any(self: Rc<Self>) -> Rc<dyn Any>;

    fn childs(&self) -> Vec<Rc<dyn SqlNode>>;
}

/// Specialised renderer for `{CUBE}` / `{TABLE}` placeholders that
/// only need the cube's name.
pub trait CubeNameNode {
    fn to_sql(&self, cube_name: &String) -> Result<String, CubeError>;
}
