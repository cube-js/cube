use super::{GranularityHelper, QueryDateTime, SqlInterval};
use crate::planner::sql_templates::PlanSqlTemplates;
use crate::planner::symbols::deps::symbol_deps;
use crate::planner::SqlCall;
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

symbol_deps! {
    Granularity {
        calendar_sql: dep,
        granularity: skip,
        granularity_interval: skip,
        granularity_offset: skip,
        origin: skip,
        is_predefined_granularity: skip,
        is_natural_aligned: skip,
    }
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

        // `origin` takes precedence over `offset`, and drops it: keeping the offset around would
        // let the rendering shift buckets that `origin` — which every alignment check reads —
        // knows nothing about.
        let (origin, granularity_offset) = if let Some(origin) = origin {
            (QueryDateTime::from_date_str(timezone, &origin)?, None)
        } else if let Some(offset) = &granularity_offset {
            // Week-based intervals expect the offset relative to the start of a week.
            let origin = Self::fix_origin_for_weeks_if_needed(
                Self::default_origin(timezone)?,
                &granularity_interval,
            );
            let interval = SqlInterval::from_str(offset)?;
            (origin.add_interval(&interval)?, granularity_offset)
        } else {
            (
                Self::fix_origin_for_weeks_if_needed(
                    Self::default_origin(timezone)?,
                    &granularity_interval,
                ),
                granularity_offset,
            )
        };

        // A trivial interval can only ride the DATE_TRUNC path when its origin actually sits on
        // that unit's natural boundary — `1 year` from 2024-04-01 is a fiscal year, and truncating
        // to the calendar year would silently discard the origin. An `offset` origin is exempt:
        // it is off-boundary by construction, and the aligned branch renders it by subtracting the
        // offset before truncating and adding it back.
        let is_natural_aligned = granularity_interval.is_trivial()
            && (granularity_offset.is_some()
                || origin.is_start_of(&granularity_interval.min_granularity()?)?);

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

    #[test]
    fn trivial_interval_with_off_boundary_origin_is_not_natural_aligned() {
        // A fiscal year starting on April 1 cannot be rendered as DATE_TRUNC('year', ...).
        assert!(!custom("1 year", Some("2024-04-01"), None).is_natural_aligned());
    }

    #[test]
    fn trivial_interval_with_on_boundary_origin_stays_natural_aligned() {
        // January 1 is the natural year boundary, so DATE_TRUNC is still correct.
        assert!(custom("1 year", Some("2024-01-01"), None).is_natural_aligned());
    }

    #[test]
    fn off_boundary_origin_is_judged_against_the_intervals_own_unit() {
        // April 1 is off the year boundary but exactly on the month and quarter boundaries.
        assert!(custom("1 month", Some("2024-04-01"), None).is_natural_aligned());
        assert!(custom("1 quarter", Some("2024-04-01"), None).is_natural_aligned());
        // ...and February 1 is on the month boundary but not the quarter one.
        assert!(custom("1 month", Some("2024-02-01"), None).is_natural_aligned());
        assert!(!custom("1 quarter", Some("2024-02-01"), None).is_natural_aligned());
    }

    #[test]
    fn origin_with_a_time_component_defeats_alignment_of_date_intervals() {
        assert!(!custom("1 day", Some("2024-04-01T06:00:00"), None).is_natural_aligned());
        assert!(custom("1 hour", Some("2024-04-01T06:00:00"), None).is_natural_aligned());
    }

    #[test]
    fn week_origin_is_judged_against_monday() {
        // 2024-01-01 is a Monday; 2024-01-03 is a Wednesday.
        assert!(custom("1 week", Some("2024-01-01"), None).is_natural_aligned());
        assert!(!custom("1 week", Some("2024-01-03"), None).is_natural_aligned());
    }

    #[test]
    fn default_and_offset_origins_keep_their_existing_alignment() {
        // No explicit origin: the default origin is the start of the year, and the week-only
        // interval snaps to Monday — both natural boundaries, so DATE_TRUNC still applies.
        assert!(custom("1 year", None, None).is_natural_aligned());
        assert!(custom("1 week", None, None).is_natural_aligned());
        // `offset` renders through the subtract/truncate/add branch, which stays aligned.
        assert!(custom("1 week", None, Some("-1 day")).is_natural_aligned());
    }

    #[test]
    fn explicit_origin_discards_the_offset() {
        // `origin` takes precedence when both are set. The offset must be dropped outright —
        // left in place it would shift the rendered buckets away from the origin that
        // `align_date_to_origin` and the materialized time series both bin on.
        let g = custom("1 year", Some("2024-04-01"), Some("3 months"));
        assert_eq!(g.granularity_offset(), &None);
        assert!(!g.is_natural_aligned());

        // Same for an on-boundary origin, which would otherwise stay on the offset sub-branch.
        let g = custom("1 year", Some("2024-01-01"), Some("3 months"));
        assert_eq!(g.granularity_offset(), &None);
        assert_eq!(
            origin_date(&g),
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        );
        assert!(g.is_natural_aligned());
    }

    #[test]
    fn non_trivial_intervals_are_never_natural_aligned() {
        assert!(!custom("15 minutes", None, None).is_natural_aligned());
        assert!(!custom("6 months", Some("2024-01-01"), None).is_natural_aligned());
    }
}
