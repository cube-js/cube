use super::{Schema, SchemaColumn};
use crate::planner::{query_tools::QueryTools, sql_templates::PlanSqlTemplates, Granularity};
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
        if templates.supports_generated_time_series(self.granularity.is_predefined_granularity())? {
            let interval_description = templates
                .interval_and_minimal_time_unit(self.granularity.granularity_interval().to_sql())?;
            if interval_description.len() != 2 {
                return Err(CubeError::internal(
                    "Interval description must have 2 elements".to_string(),
                ));
            }
            let interval = interval_description[0].clone();
            let minimal_time_unit = interval_description[1].clone();
            match &self.date_range {
                TimeSeriesDateRange::Filter(from_date, to_date) => {
                    let start = templates.quote_string(from_date)?;
                    let date_field = templates.quote_identifier("d")?;
                    let date_from = templates.time_stamp_cast(date_field.clone())?;
                    let end = templates.quote_string(to_date)?;
                    let date_to = format!(
                        "({})",
                        templates.add_interval(date_from.clone(), interval.clone())?
                    );
                    let date_to =
                        templates.subtract_interval(date_to, "1 millisecond".to_string())?;

                    templates.generated_time_series_select(
                        &date_from,
                        &date_to,
                        &start,
                        &end,
                        &templates.interval_string(interval)?,
                        &self.granularity.granularity_offset(),
                        &minimal_time_unit,
                    )
                }
                TimeSeriesDateRange::Generated(cte_name) => {
                    let min_date_name = format!("min_date");
                    let max_date_name = format!("max_date");
                    templates.generated_time_series_with_cte_range_source(
                        &cte_name,
                        &min_date_name,
                        &max_date_name,
                        &templates.interval_string(interval)?,
                        &minimal_time_unit,
                    )
                }
            }
        } else {
            let (from_date, to_date, raw_from_date, raw_to_date) = match &self.date_range {
                TimeSeriesDateRange::Filter(from_date, to_date) => (
                    format!("'{}'", from_date),
                    format!("'{}'", to_date),
                    from_date.clone(),
                    to_date.clone(),
                ),
                TimeSeriesDateRange::Generated(_) => {
                    return Err(CubeError::user(
                        "Date range is required for time series in drivers where generated time series is not supported".to_string(),
                    ));
                }
            };
            let series = if self.granularity.is_predefined_granularity() {
                self.query_tools.base_tools().generate_time_series(
                    self.granularity.granularity().clone(),
                    vec![raw_from_date.clone(), raw_to_date.clone()],
                )?
            } else {
                self.query_tools.base_tools().generate_custom_time_series(
                    self.granularity.granularity_interval().to_sql(),
                    vec![raw_from_date.clone(), raw_to_date.clone()],
                    self.granularity.origin_local_formatted(),
                )?
            };
            templates.time_series_select(from_date.clone(), to_date.clone(), series)
        }
    }
}
