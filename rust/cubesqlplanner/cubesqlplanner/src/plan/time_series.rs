use super::{Schema, SchemaColumn};
use crate::planner::{
    query_tools::QueryTools,
    sql_templates::{PlanSqlTemplates, TemplateProjectionColumn},
    Granularity,
};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct TimeSeries {
    query_tools: Rc<QueryTools>,
    #[allow(dead_code)]
    time_dimension_name: String,
    date_range: TimeSeriesDateRange,
    granularity: Granularity,
    schema: Rc<Schema>,
}

pub enum TimeSeriesDateRange {
    Filter(String, String),
    Generated(String), // Name of cte with min/max dates
}

impl TimeSeries {
    pub fn new(
        query_tools: Rc<QueryTools>,
        time_dimension_name: String,
        date_range: TimeSeriesDateRange,
        granularity: Granularity,
    ) -> Self {
        let column = SchemaColumn::new(format!("date_from"), Some(time_dimension_name.clone()));
        let schema = Rc::new(Schema::new(vec![column]));
        Self {
            query_tools,
            time_dimension_name,
            granularity,
            date_range,
            schema,
        }
    }

    pub fn schema(&self) -> Rc<Schema> {
        self.schema.clone()
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        if templates.supports_generated_time_series() {
            let (from_date, to_date) = match &self.date_range {
                TimeSeriesDateRange::Filter(from_date, to_date) => {
                    (format!("'{}'", from_date), format!("'{}'", to_date))
                }
                TimeSeriesDateRange::Generated(cte_name) => {
                    let date_from_name = format!("date_from");
                    let date_to_name = format!("date_to");
                    let from_column = TemplateProjectionColumn {
                        expr: date_from_name.clone(),
                        alias: date_from_name.clone(),
                        aliased: templates.column_aliased(&date_from_name, &date_from_name)?,
                    };
                    let to_column = TemplateProjectionColumn {
                        expr: date_to_name.clone(),
                        alias: date_to_name.clone(),
                        aliased: templates.column_aliased(&date_to_name, &date_to_name)?,
                    };
                    let from = templates.select(
                        vec![],
                        &cte_name,
                        vec![from_column],
                        None,
                        vec![],
                        None,
                        vec![],
                        None,
                        None,
                        false,
                    )?;
                    let to = templates.select(
                        vec![],
                        &cte_name,
                        vec![to_column],
                        None,
                        vec![],
                        None,
                        vec![],
                        None,
                        None,
                        false,
                    )?;
                    (format!("({})", from), format!("({})", to))
                }
            };
            templates.generated_time_series_select(
                &from_date,
                &to_date,
                &self.granularity.granularity_interval(),
            )
        } else {
            let (from_date, to_date) = match &self.date_range {
                TimeSeriesDateRange::Filter(from_date, to_date) => {
                    (format!("'{}'", from_date), format!("'{}'", to_date))
                }
                TimeSeriesDateRange::Generated(_) => {
                    return Err(CubeError::user(
                        "Date range is required for time series in drivers where generated time series is not supported".to_string(),
                    ));
                }
            };
            let series = if self.granularity.is_predefined_granularity() {
                self.query_tools.base_tools().generate_time_series(
                    self.granularity.granularity().clone(),
                    vec![from_date.clone(), to_date.clone()],
                )?
            } else {
                self.query_tools.base_tools().generate_custom_time_series(
                    self.granularity.granularity_interval().clone(),
                    vec![from_date.clone(), to_date.clone()],
                    self.granularity.origin_local_formatted(),
                )?
            };
            templates.time_series_select(from_date.clone(), to_date.clone(), series)
        }
    }
}
