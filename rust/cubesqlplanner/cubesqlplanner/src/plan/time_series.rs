use super::{Schema, SchemaColumn, Select, Union};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct TimeSeries {
    pub time_dimension_name: String,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    pub seria: Vec<Vec<String>>,
}

impl TimeSeries {
    pub fn make_schema(&self, self_alias: Option<String>) -> Schema {
        let column = SchemaColumn::new(
            self_alias,
            format!("date_from"),
            self.time_dimension_name.clone(),
        );
        Schema::new(vec![column], vec![])
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        templates.time_series_select(
            self.from_date.clone(),
            self.to_date.clone(),
            self.seria.clone(),
        )
    }
}
