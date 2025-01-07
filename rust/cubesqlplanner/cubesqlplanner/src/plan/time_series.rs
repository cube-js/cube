use super::{Schema, SchemaColumn};
use crate::planner::sql_templates::PlanSqlTemplates;
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct TimeSeries {
    pub time_dimension_name: String,
    pub from_date: Option<String>,
    pub to_date: Option<String>,
    pub seria: Vec<Vec<String>>,
    pub schema: Rc<Schema>,
}

impl TimeSeries {
    pub fn new(
        time_dimension_name: String,
        from_date: Option<String>,
        to_date: Option<String>,
        seria: Vec<Vec<String>>,
    ) -> Self {
        let column = SchemaColumn::new(format!("date_from"), Some(time_dimension_name.clone()));
        let schema = Rc::new(Schema::new(vec![column]));
        Self {
            time_dimension_name,
            from_date,
            to_date,
            seria,
            schema,
        }
    }

    pub fn schema(&self) -> Rc<Schema> {
        self.schema.clone()
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        templates.time_series_select(
            self.from_date.clone(),
            self.to_date.clone(),
            self.seria.clone(),
        )
    }
}
