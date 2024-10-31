use super::Select;
use super::Union;
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum QueryPlan {
    Select(Rc<Select>),
    Union(Rc<Union>),
}

impl QueryPlan {
    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        match self {
            QueryPlan::Select(s) => s.to_sql(templates),
            QueryPlan::Union(u) => u.to_sql(templates),
        }
    }
}
