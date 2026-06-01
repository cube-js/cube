use super::{GranularityHelper, QueryDateTime, SqlInterval};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::{MemberSymbol, SqlCall};
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::rc::Rc;
use std::str::FromStr;

#[derive(Clone)]
pub struct Granularity {
    granularity: String,
    granularity_interval: SqlInterval,
    granularity_offset: Option<String>,
    origin: QueryDateTime,
    is_predefined_granularity: bool,
    is_natural_aligned: bool,
    calendar_sql: Option<Rc<SqlCall>>,
}

impl Granularity {
    pub fn try_new_predefined(timezone: Tz, granularity: String) -> Result<Self, CubeError> {
        let granularity_interval = format!("1 {}", granularity).parse()?;
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
        let granularity_interval = granularity_interval.parse::<SqlInterval>()?;
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
            // Week-based intervals expect the offset relative to the start of a week.
            let origin = Self::fix_origin_for_weeks_if_needed(
                Self::default_origin(timezone)?,
                &granularity_interval,
            );
            let interval = SqlInterval::from_str(offset)?;
            origin.add_interval(&interval)?
        } else {
            Self::fix_origin_for_weeks_if_needed(
                Self::default_origin(timezone)?,
                &granularity_interval,
            )
        };

        let is_natural_aligned = granularity_interval.is_trivial();
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

    pub fn apply_to_deps<F: Fn(&Rc<MemberSymbol>) -> Result<Rc<MemberSymbol>, CubeError>>(
        &self,
        f: &F,
    ) -> Result<Self, CubeError> {
        let mut result = self.clone();
        if let Some(calendar_sql) = &self.calendar_sql {
            result.calendar_sql = Some(calendar_sql.apply_recursive(f)?);
        }
        Ok(result)
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

    pub fn granularity_interval(&self) -> &SqlInterval {
        &self.granularity_interval
    }

    pub fn origin_local_formatted(&self) -> String {
        self.origin.format("%Y-%m-%dT%H:%M:%S%.3f")
    }

    pub fn granularity_from_interval(&self) -> Result<String, CubeError> {
        self.granularity_interval.min_granularity()
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
        date.align_to_origin(&self.origin, &self.granularity_interval)
    }

    fn default_origin(timezone: Tz) -> Result<QueryDateTime, CubeError> {
        Ok(QueryDateTime::now(timezone)?.start_of_year())
    }

    fn fix_origin_for_weeks_if_needed(
        origin: QueryDateTime,
        interval: &SqlInterval,
    ) -> QueryDateTime {
        if interval.is_week_only() {
            origin.start_of_iso_week()
        } else {
            origin
        }
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
                self.granularity_interval.to_sql(),
                input,
                self.origin_local_formatted(),
            )?
        };

        Ok(res)
    }

    /// Check if the granularity is aligned with the given date range.
    /// For custom granularities, this checks if:
    /// 1. The date range duration is an exact multiple of the granularity interval
    /// 2. The start date is aligned with the granularity origin
    pub fn is_aligned_with_date_range(
        &self,
        start_str: &str,
        end_str: &str,
        timezone: Tz,
    ) -> Result<bool, CubeError> {
        let start = QueryDateTime::from_date_str(timezone, start_str)?;
        let end = QueryDateTime::from_date_str(timezone, end_str)?;
        let end = end.add_duration(chrono::Duration::milliseconds(1))?;

        // Check if the start is aligned with the origin first
        let aligned_start = self.align_date_to_origin(start.clone())?;

        if start != aligned_start {
            return Ok(false);
        }

        // Check if the interval fits exactly into the date range
        let mut test_date = start;
        while test_date < end {
            test_date = test_date.add_interval(&self.granularity_interval)?;
        }

        if test_date != end {
            return Ok(false);
        }

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, NaiveDate, Weekday};

    fn origin_date(g: &Granularity) -> NaiveDate {
        NaiveDate::parse_from_str(&g.origin_local_formatted()[..10], "%Y-%m-%d").unwrap()
    }

    fn custom(interval: &str, origin: Option<&str>, offset: Option<&str>) -> Granularity {
        Granularity::try_new_custom(
            "UTC".parse::<Tz>().unwrap(),
            "test_granularity".to_string(),
            origin.map(str::to_string),
            interval.to_string(),
            offset.map(str::to_string),
            None,
        )
        .unwrap()
    }

    #[test]
    fn week_only_default_origin_snaps_to_iso_monday() {
        assert_eq!(
            origin_date(&custom("2 weeks", None, None)).weekday(),
            Weekday::Mon
        );
    }

    #[test]
    fn non_week_default_origin_stays_at_year_start() {
        let d = origin_date(&custom("2 days", None, None));
        assert_eq!((d.month(), d.day()), (1, 1));
    }

    #[test]
    fn week_with_offset_aligns_to_monday_then_offsets() {
        // Monday-of-year-start + 2 days => Wednesday.
        assert_eq!(
            origin_date(&custom("2 weeks", None, Some("2 days"))).weekday(),
            Weekday::Wed
        );
    }

    #[test]
    fn explicit_origin_is_not_snapped_for_week_interval() {
        // 2024-01-03 is a Wednesday; an explicit origin must be preserved verbatim.
        assert_eq!(
            origin_date(&custom("2 weeks", Some("2024-01-03"), None)),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap()
        );
    }
}
