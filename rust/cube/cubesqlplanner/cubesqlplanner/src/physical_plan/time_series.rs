use super::{QueryPlan, Schema, SchemaColumn};
use crate::planner::{sql_templates::PlanSqlTemplates, Granularity, MemberSymbol, QueryTimeSeries};
use cubenativeutils::CubeError;
use std::rc::Rc;

pub struct TimeSeries {
    #[allow(dead_code)]
    time_dimension_name: String,
    source: TimeSeriesSource,
    granularity: Granularity,
    schema: Rc<Schema>,
}

pub enum TimeSeriesDateRange {
    Filter(String, String),
    Generated(String), // Name of cte with min/max dates
}

/// Series points read off a calendar cube, each paired with the period it falls
/// into. Bounds a `to_date` window whose granularity defines its own SQL, which
/// no interval math can reproduce.
pub struct CalendarPeriodSource {
    source: Rc<QueryPlan>,
    date_from_alias: String,
    /// Granularity name paired with the column `source` projects it as.
    period_aliases: Vec<(String, String)>,
    /// Name of the cte resolving the range at query time. `None` when the
    /// query states a date range, which bounds `source` on its own.
    range_cte: Option<String>,
}

impl CalendarPeriodSource {
    pub fn new(
        source: Rc<QueryPlan>,
        date_from_alias: String,
        period_aliases: Vec<(String, String)>,
        range_cte: Option<String>,
    ) -> Self {
        Self {
            source,
            date_from_alias,
            period_aliases,
            range_cte,
        }
    }
}

pub enum TimeSeriesSource {
    Range(TimeSeriesDateRange),
    Calendar(CalendarPeriodSource),
}

impl TimeSeries {
    pub fn new(
        time_dimension: &Rc<MemberSymbol>,
        source: TimeSeriesSource,
        granularity: Granularity,
    ) -> Self {
        let column = SchemaColumn::new(format!("date_from"), Some(time_dimension.clone()));
        let schema = Rc::new(Schema::new(vec![column]));
        Self {
            time_dimension_name: time_dimension.full_name(),
            granularity,
            source,
            schema,
        }
    }

    pub fn schema(&self) -> Rc<Schema> {
        self.schema.clone()
    }

    /// Column the series exposes the start of `granularity`'s period as.
    pub fn period_start_column(granularity: &str) -> String {
        format!("date_period_start_{}", granularity)
    }

    pub fn to_sql(&self, templates: &PlanSqlTemplates) -> Result<String, CubeError> {
        let date_range = match &self.source {
            TimeSeriesSource::Calendar(calendar) => {
                return self.calendar_to_sql(calendar, templates)
            }
            TimeSeriesSource::Range(date_range) => date_range,
        };
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
            match date_range {
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
            let (from_date, to_date, raw_from_date, raw_to_date) = match date_range {
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
            let precision = templates.timestamp_precision()?;
            let range = [raw_from_date.clone(), raw_to_date.clone()];
            let series = if self.granularity.is_predefined_granularity() {
                QueryTimeSeries::generate_predefined(
                    self.granularity.granularity(),
                    &range,
                    precision,
                )?
            } else {
                QueryTimeSeries::generate_custom(
                    &self.granularity.granularity_interval().to_sql(),
                    &range,
                    &self.granularity.origin_local_formatted(),
                    precision,
                )?
            };
            templates.time_series_select(from_date.clone(), to_date.clone(), series)
        }
    }

    fn calendar_to_sql(
        &self,
        calendar: &CalendarPeriodSource,
        templates: &PlanSqlTemplates,
    ) -> Result<String, CubeError> {
        let interval_description = templates
            .interval_and_minimal_time_unit(self.granularity.granularity_interval().to_sql())?;
        if interval_description.len() != 2 {
            return Err(CubeError::internal(
                "Interval description must have 2 elements".to_string(),
            ));
        }
        let interval = interval_description[0].clone();

        let source_alias = "calendar_series".to_string();
        let date_from =
            templates.column_reference(&Some(source_alias.clone()), &calendar.date_from_alias)?;
        let date_from = templates.time_stamp_cast(date_from)?;
        let next_start = format!("({})", templates.add_interval(date_from.clone(), interval)?);
        let next_start = if self.granularity.calendar_sql().is_some() {
            // A calendar period ends where the next one starts, and how far off
            // that is only the calendar knows — a 4-5-4 month runs 28 or 35 days
            // against a nominal `1 month`. Only the last point of the series has
            // no next row to read it from.
            let lead = templates.window_function(
                &format!("LEAD({})", date_from),
                "",
                &format!("{} ASC", date_from),
                "",
            )?;
            format!("COALESCE({}, {})", lead, next_start)
        } else {
            next_start
        };
        let date_to = templates.subtract_interval(next_start, "1 millisecond".to_string())?;

        let mut columns = vec![
            templates.column_aliased(&date_from, "date_from")?,
            templates.column_aliased(&date_to, "date_to")?,
        ];
        for (granularity, alias) in calendar.period_aliases.iter() {
            let column = templates.column_reference(&Some(source_alias.clone()), alias)?;
            columns
                .push(templates.column_aliased(&column, &Self::period_start_column(granularity))?);
        }

        let where_clause = match &calendar.range_cte {
            Some(range_cte) => {
                let min = Self::range_cte_bound(templates, range_cte, "min_date")?;
                let max = Self::range_cte_bound(templates, range_cte, "max_date")?;
                format!(" \nWHERE {date_from} >= {min} AND {date_from} <= {max}")
            }
            None => String::new(),
        };

        let source = templates.query_aliased(
            &format!("({})", calendar.source.to_sql(templates)?),
            &source_alias,
        )?;

        Ok(format!(
            "SELECT {}\nFROM {}{}",
            columns.join(", "),
            source,
            where_clause
        ))
    }

    fn range_cte_bound(
        templates: &PlanSqlTemplates,
        range_cte: &str,
        column: &str,
    ) -> Result<String, CubeError> {
        Ok(format!(
            "(SELECT {} FROM {})",
            templates.quote_identifier(column)?,
            range_cte
        ))
    }
}
