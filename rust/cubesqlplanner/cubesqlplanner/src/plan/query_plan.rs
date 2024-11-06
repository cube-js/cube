use super::{Schema, Select, Union};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub enum QueryPlan {
    Select(Rc<Select>),
    Union(Rc<Union>),
}

impl QueryPlan {
    pub fn make_schema(&self, self_alias: Option<String>) -> Schema {
        match self {
            QueryPlan::Select(select) => select.make_schema(self_alias),
            QueryPlan::Union(union) => union.make_schema(self_alias),
        }
    }
    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        match self {
            QueryPlan::Select(s) => s.to_sql(templates),
            QueryPlan::Union(u) => u.to_sql(templates),
        }
    }
}
