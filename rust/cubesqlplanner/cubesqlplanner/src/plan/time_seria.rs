use super::{Schema, SchemaColumn, Select, Union};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct TimeSeria {
    pub time_dimension_name: String,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    pub seria: Vec<Vec<String>>,
}

impl TimeSeria {
    pub fn make_schema(&self, self_alias: Option<String>) -> Schema {
        let column = SchemaColumn::new(
            self_alias,
            format!("from_date"),
            self.time_dimension_name.clone(),
        );
        Schema::new(vec![column], vec![])
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        templates.time_seria_select(
            self.from_date.clone(),
            self.to_date.clone(),
            self.seria.clone(),
        )
    }
}
