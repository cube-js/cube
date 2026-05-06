use crate::planner::query_tools::QueryTools;
use crate::planner::sql_templates::{PlanSqlTemplates, TemplateProjectionColumn};
use crate::planner::QueryDateTimeHelper;
use cubenativeutils::CubeError;
use std::rc::Rc;

const FROM_PARTITION_RANGE: &str = "__FROM_PARTITION_RANGE";
const TO_PARTITION_RANGE: &str = "__TO_PARTITION_RANGE";

pub struct FilterSqlContext<'a> {
    pub member_sql: &'a str,
    pub query_tools: &'a Rc<QueryTools>,
    pub plan_templates: &'a PlanSqlTemplates,
    pub use_db_time_zone: bool,
    pub use_raw_values: bool,
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
        if self.use_raw_values {
            return Ok(value.to_string());
        }
        let placeholder = self.query_tools.allocate_param(value);
        self.plan_templates.time_stamp_cast(placeholder)
    }

    pub fn format_and_allocate_from_date(&self, value: &str) -> Result<String, CubeError> {
        if self.use_raw_values {
            return Ok(value.to_string());
        }
        if self.is_partition_range(value) {
            return self.allocate_timestamp_param(value);
        }
        let precision = self.plan_templates.timestamp_precision()?;
        let formatted = QueryDateTimeHelper::format_from_date(value, precision)?;
        let with_tz = self.apply_db_time_zone(formatted)?;
        self.allocate_timestamp_param(&with_tz)
    }

    pub fn format_and_allocate_to_date(&self, value: &str) -> Result<String, CubeError> {
        if self.use_raw_values {
            return Ok(value.to_string());
        }
        if self.is_partition_range(value) {
            return self.allocate_timestamp_param(value);
        }
        let precision = self.plan_templates.timestamp_precision()?;
        let formatted = QueryDateTimeHelper::format_to_date(value, precision)?;
        let with_tz = self.apply_db_time_zone(formatted)?;
        self.allocate_timestamp_param(&with_tz)
    }

    fn is_partition_range(&self, value: &str) -> bool {
        value == FROM_PARTITION_RANGE || value == TO_PARTITION_RANGE
    }

    fn apply_db_time_zone(&self, value: String) -> Result<String, CubeError> {
        if self.use_db_time_zone {
            self.plan_templates.in_db_time_zone(value)
        } else {
            Ok(value)
        }
    }

    pub fn convert_tz(&self, field: &str) -> Result<String, CubeError> {
        self.plan_templates.convert_tz(field.to_string())
    }

    pub fn date_range_from_time_series(&self) -> Result<(String, String), CubeError> {
        let from_expr = format!(
            "min({})",
            self.plan_templates.quote_identifier("date_from")?
        );
        let to_expr = format!("max({})", self.plan_templates.quote_identifier("date_to")?);
        let from_expr = self.plan_templates.series_bounds_cast(&from_expr)?;
        let to_expr = self.plan_templates.series_bounds_cast(&to_expr)?;
        let alias = "value".to_string();
        let time_series_cte_name = "time_series".to_string();

        let from_column = TemplateProjectionColumn {
            expr: from_expr.clone(),
            alias: alias.clone(),
            aliased: self.plan_templates.column_aliased(&from_expr, &alias)?,
        };
        let to_column = TemplateProjectionColumn {
            expr: to_expr.clone(),
            alias: alias.clone(),
            aliased: self.plan_templates.column_aliased(&to_expr, &alias)?,
        };

        let from = self.plan_templates.select(
            vec![],
            &time_series_cte_name,
            vec![from_column],
            None,
            vec![],
            None,
            vec![],
            None,
            None,
            false,
        )?;
        let to = self.plan_templates.select(
            vec![],
            &time_series_cte_name,
            vec![to_column],
            None,
            vec![],
            None,
            vec![],
            None,
            None,
            false,
        )?;
        Ok((format!("({})", from), format!("({})", to)))
    }

    pub fn extend_date_range_bound(
        &self,
        date: String,
        interval: &Option<String>,
        is_sub: bool,
    ) -> Result<Option<String>, CubeError> {
        match interval {
            Some(interval) if interval != "unbounded" => {
                if is_sub {
                    Ok(Some(
                        self.plan_templates
                            .subtract_interval(date, interval.clone())?,
                    ))
                } else {
                    Ok(Some(
                        self.plan_templates.add_interval(date, interval.clone())?,
                    ))
                }
            }
            Some(_) => Ok(None), // unbounded
            None => Ok(Some(date)),
        }
    }
}

pub trait FilterOperationSql {
    fn to_sql(&self, ctx: &FilterSqlContext) -> Result<String, CubeError>;
}
