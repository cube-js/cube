use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::QueryDateTimeHelper;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct FilterSqlContext<'a> {
    pub member_sql: &'a str,
    pub query_tools: &'a Rc<QueryTools>,
    pub plan_templates: &'a PlanSqlTemplates,
    pub use_db_time_zone: bool,
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

    pub fn allocate_timestamp_param(&self, value: &str) -> Result<String, CubeError> {
        let placeholder = self.query_tools.allocate_param(value);
        self.plan_templates.time_stamp_cast(placeholder)
    }

    pub fn format_and_allocate_from_date(&self, value: &str) -> Result<String, CubeError> {
        let precision = self.plan_templates.timestamp_precision()?;
        let formatted = QueryDateTimeHelper::format_from_date(value, precision)?;
        let with_tz = self.apply_db_time_zone(formatted)?;
        self.allocate_timestamp_param(&with_tz)
    }

    pub fn format_and_allocate_to_date(&self, value: &str) -> Result<String, CubeError> {
        let precision = self.plan_templates.timestamp_precision()?;
        let formatted = QueryDateTimeHelper::format_to_date(value, precision)?;
        let with_tz = self.apply_db_time_zone(formatted)?;
        self.allocate_timestamp_param(&with_tz)
    }

    fn apply_db_time_zone(&self, value: String) -> Result<String, CubeError> {
        if self.use_db_time_zone {
            self.plan_templates.in_db_time_zone(value)
        } else {
            Ok(value)
        }
    }
}

pub trait FilterOperationSql {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError>;
}
