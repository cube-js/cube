use super::{GranularityHelper, QueryDateTime, SqlInterval};
use crate::planner::sql_evaluator::SqlCall;
use crate::planner::sql_templates::PlanSqlTemplates;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use itertools::Itertools;
use std::rc::Rc;
use std::str::FromStr;

#[derive(Clone)]
pub struct Granularity {
    granularity: String,
    granularity_interval: String,
    granularity_offset: Option<String>,
    origin: QueryDateTime,
    is_predefined_granularity: bool,
    is_natural_aligned: bool,
    calendar_sql: Option<Rc<SqlCall>>,
}

impl Granularity {
    pub fn try_new_predefined(timezone: Tz, granularity: String) -> Result<Self, CubeError> {
        let granularity_interval = format!("1 {}", granularity);
        let origin = Self::default_origin(timezone)?;

        Ok(Self {
            granularity,
            granularity_interval,
            granularity_offset: None,
            origin,
            is_predefined_granularity: true,
            is_natural_aligned: true,
            calendar_sql: None,
        })
    }
    pub fn try_new_custom(
        timezone: Tz,
        granularity: String,
        origin: Option<String>,
        granularity_interval: String,
        granularity_offset: Option<String>,
        calendar_sql: Option<Rc<SqlCall>>,
    ) -> Result<Self, CubeError> {
        // sql() is mutual exclusive with interval and offset/origin
        if calendar_sql.is_some() {
            return Ok(Self {
                granularity,
                granularity_interval,
                granularity_offset: None,
                origin: Self::default_origin(timezone)?,
                is_predefined_granularity: false,
                is_natural_aligned: false,
                calendar_sql,
            });
        }

        let origin = if let Some(origin) = origin {
            QueryDateTime::from_date_str(timezone, &origin)?
        } else if let Some(offset) = &granularity_offset {
            let origin = Self::default_origin(timezone)?;
            let interval = SqlInterval::from_str(offset)?;
            origin.add_interval(&interval)?
        } else {
            Self::default_origin(timezone)?
        };

        let mut interval_parts = granularity_interval.split_whitespace().tuples::<(_, _)>();
        let first_part = interval_parts.next();
        let second_part = interval_parts.next();
        let is_natural_aligned = if second_part.is_none() {
            if let Some((value, _)) = first_part {
                let value = value
                    .parse::<i32>()
                    .map_err(|_| CubeError::user(format!("Invalid interval value: {}", value)))?;
                value == 1
            } else {
                false
            }
        } else {
            false
        };

        Ok(Self {
            granularity,
            granularity_interval,
            granularity_offset,
            origin,
            is_predefined_granularity: false,
            is_natural_aligned,
            calendar_sql,
        })
    }

    pub fn is_natural_aligned(&self) -> bool {
        self.is_natural_aligned
    }

    pub fn granularity_offset(&self) -> &Option<String> {
        &self.granularity_offset
    }

    pub fn calendar_sql(&self) -> &Option<Rc<SqlCall>> {
        &self.calendar_sql
    }

    pub fn granularity(&self) -> &String {
        &self.granularity
    }

    pub fn granularity_interval(&self) -> &String {
        &self.granularity_interval
    }

    pub fn origin_local_formatted(&self) -> String {
        self.origin.format("%Y-%m-%dT%H:%M:%S%.3f")
    }

    pub fn granularity_from_interval(&self) -> Result<String, CubeError> {
        self.granularity_interval
            .parse::<SqlInterval>()?
            .min_granularity()
    }

    pub fn granularity_from_offset(&self) -> Result<String, CubeError> {
        if let Some(offset) = &self.granularity_offset {
            offset.parse::<SqlInterval>()?.min_granularity()
        } else {
            Ok("".to_string())
        }
    }

    pub fn is_predefined_granularity(&self) -> bool {
        self.is_predefined_granularity
    }

    pub fn min_granularity(&self) -> Result<Option<String>, CubeError> {
        if self.is_predefined_granularity {
            return Ok(Some(self.granularity.clone()));
        }

        if self.granularity_offset.is_some() {
            return GranularityHelper::min_granularity(
                &Some(self.granularity_from_interval()?),
                &Some(self.granularity_from_offset()?),
            );
        }

        GranularityHelper::min_granularity(
            &Some(self.granularity_from_interval()?),
            &Some(self.origin.granularity()),
        )
    }

    pub fn resolved_granularity(&self) -> Result<String, CubeError> {
        if self.is_predefined_granularity {
            Ok(self.granularity.clone())
        } else {
            self.granularity_from_interval()
        }
    }

    pub fn align_date_to_origin(&self, date: QueryDateTime) -> Result<QueryDateTime, CubeError> {
        let interval = self.granularity_interval.parse::<SqlInterval>()?;
        date.align_to_origin(&self.origin, &interval)
    }

    fn default_origin(timezone: Tz) -> Result<QueryDateTime, CubeError> {
        Ok(QueryDateTime::now(timezone)?.start_of_year())
    }

    pub fn apply_to_input_sql(
        &self,
        templates: &PlanSqlTemplates,
        input: String,
    ) -> Result<String, CubeError> {
        let res = if self.is_natural_aligned {
            if let Some(offset) = &self.granularity_offset {
                let mut res = templates.subtract_interval(input.clone(), offset.clone())?;
                res = templates.time_grouped_column(self.granularity_from_interval()?, res)?;
                res = templates.add_interval(res, offset.clone())?;
                res
            } else {
                templates.time_grouped_column(self.granularity_from_interval()?, input)?
            }
        } else {
            templates.date_bin(
                self.granularity_interval.clone(),
                input,
                self.origin_local_formatted(),
            )?
        };

        Ok(res)
    }
}
