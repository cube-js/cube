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

impl<'a> FilterSqlContext<'a> {
    pub fn allocate_param(&self, value: &str) -> String {
        self.query_tools.allocate_param(value)
    }

    pub fn cast_param(
        &self,
        value: &str,
        member_type: &Option<String>,
    ) -> Result<String, CubeError> {
        match member_type.as_deref() {
            Some("boolean") => self.plan_templates.bool_param_cast(value),
            Some("number") => self.plan_templates.number_param_cast(value),
            _ => Ok(value.to_string()),
        }
    }

    pub fn allocate_and_cast(
        &self,
        value: &str,
        member_type: &Option<String>,
    ) -> Result<String, CubeError> {
        let allocated = self.allocate_param(value);
        self.cast_param(&allocated, member_type)
    }

    pub fn allocate_and_cast_values(
        &self,
        values: &[Option<String>],
        member_type: &Option<String>,
    ) -> Result<Vec<String>, CubeError> {
        values
            .iter()
            .filter_map(|v| v.as_ref())
            .map(|v| self.allocate_and_cast(v, member_type))
            .collect()
    }
}

pub trait FilterOperationSql {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError>;
}
