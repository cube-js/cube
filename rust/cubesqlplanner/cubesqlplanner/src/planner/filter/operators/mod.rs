pub mod equality;
pub mod nullability;

use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct FilterSqlContext<'a> {
    pub member_sql: &'a str,
    pub query_tools: &'a Rc<QueryTools>,
    pub plan_templates: &'a PlanSqlTemplates,
}

pub trait FilterOperationSql {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError>;
}
