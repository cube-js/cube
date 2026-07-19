use crate::physical_plan::sql_nodes::SqlNode;
use crate::physical_plan::SqlEvaluatorVisitor;
use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::SqlCall;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct MemberSqlContext<'a> {
    pub visitor: &'a SqlEvaluatorVisitor,
    pub node_processor: &'a Rc<dyn SqlNode>,
    pub query_tools: &'a Rc<QueryTools>,
    pub templates: &'a PlanSqlTemplates,
    pub name: &'a str,
    pub full_name: &'a str,
}

impl<'a> MemberSqlContext<'a> {
    pub fn eval_sql_call(&self, sql_call: &Rc<SqlCall>) -> Result<String, CubeError> {
        sql_call.eval(
            self.visitor,
            self.node_processor.clone(),
            self.query_tools.clone(),
            self.templates,
        )
    }
}

pub trait ToSql {
    fn to_sql(&self, ctx: &MemberSqlContext) -> Result<String, CubeError>;
}
