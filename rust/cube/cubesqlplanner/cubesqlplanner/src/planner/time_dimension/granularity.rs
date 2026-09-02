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

        // A granularity carries either an `origin` or an `offset`, never both: `CubeValidator`
        // describes the two as separate alternatives, neither of which allows the other's key.
        let (origin, granularity_offset) = if let Some(origin) = origin {
            let origin = QueryDateTime::from_date_str(timezone, &origin)?;
            let derived_offset = Self::origin_as_offset(&origin, &granularity_interval)?;
            (origin, derived_offset)
        } else if let Some(offset) = &granularity_offset {
            // Week-based intervals expect the offset relative to the start of a week.
            let origin = Self::fix_origin_for_weeks_if_needed(
                Self::default_origin(timezone)?,
                &granularity_interval,
            )?;
            let interval = SqlInterval::from_str(offset)?;
            (origin.add_interval(&interval)?, granularity_offset)
        } else {
            (
                Self::fix_origin_for_weeks_if_needed(
                    Self::default_origin(timezone)?,
                    &granularity_interval,
                )?,
                granularity_offset,
            )
        };

        // DATE_TRUNC can only carry an origin that sits on its unit's natural boundary, or one
        // expressed as an offset around the truncation. Anything else — a `1 month` grain
        // starting on the 30th, say — has to go through DATE_BIN.
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
        QueryDateTime::now(timezone)?.start_of("year")
    }

    /// An explicit origin on a single-unit interval is a natural boundary plus a constant shift,
    /// which is exactly what `offset` expresses — and the offset rendering
    /// (`DATE_TRUNC(unit, x - offset) + offset`) asks nothing of the dialect beyond truncation and
    /// interval arithmetic. Returns that shift, or `None` when the origin needs none or cannot be
    /// expressed as one.
    fn origin_as_offset(
        origin: &QueryDateTime,
        interval: &SqlInterval,
    ) -> Result<Option<String>, CubeError> {
        if !interval.is_trivial() {
            return Ok(None);
        }

        let unit = interval.min_granularity()?;
        if origin.is_start_of(&unit)? {
            return Ok(None);
        }

        // An interval has no sub-second component to shift by.
        if origin.nanosecond() != 0 {
            return Ok(None);
        }

        // The shift counts days from the first of the origin's month, so it only reproduces the
        // grid where that month is always long enough — February 29 is the one day that is not.
        // Month and quarter grains additionally step through `add_interval`, which clamps to the
        // shortest month and then keeps the shortened day (a quarter grain from the 31st walks
        // 31 -> 30 -> 30), and no fixed shift follows that drift.
        let day_is_out_of_reach = match unit.as_str() {
            "year" => origin.month() == 2 && origin.day() == 29,
            "quarter" | "month" => origin.day() > 28,
            _ => false,
        };
        if day_is_out_of_reach {
            return Ok(None);
        }

        Ok(Some(origin.offset_from_start_of(&unit)?.to_sql()))
    }

    fn fix_origin_for_weeks_if_needed(
        origin: QueryDateTime,
        interval: &SqlInterval,
    ) -> Result<QueryDateTime, CubeError> {
        if interval.is_week_only() {
            origin.start_of("week")
        } else {
            Ok(origin)
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

    fn offset_of(interval: &str, origin: &str) -> Option<String> {
        custom(interval, Some(origin), None)
            .granularity_offset()
            .clone()
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
    fn off_boundary_origin_becomes_an_offset_in_the_intervals_own_unit() {
        assert_eq!(
            offset_of("1 year", "2024-04-01").as_deref(),
            Some("3 month")
        );
        assert_eq!(
            offset_of("1 quarter", "2024-02-01").as_deref(),
            Some("1 month")
        );
        assert_eq!(
            offset_of("1 month", "2024-01-15").as_deref(),
            Some("14 day")
        );
        // 2024-01-03 is a Wednesday.
        assert_eq!(offset_of("1 week", "2024-01-03").as_deref(), Some("2 day"));
        assert_eq!(
            offset_of("1 day", "2024-04-01T06:00:00").as_deref(),
            Some("6 hour")
        );
        assert_eq!(
            offset_of("1 hour", "2024-04-01T06:30:15").as_deref(),
            Some("30 minute 15 second")
        );
        assert_eq!(
            offset_of("1 minute", "2024-04-01T06:30:15").as_deref(),
            Some("15 second")
        );
    }

    #[test]
    fn a_time_of_day_carries_into_the_offset_of_date_intervals() {
        assert_eq!(
            offset_of("1 year", "2024-04-15T06:30:15").as_deref(),
            Some("3 month 14 day 6 hour 30 minute 15 second")
        );
        assert_eq!(
            offset_of("1 week", "2024-01-03T18:45:00").as_deref(),
            Some("2 day 18 hour 45 minute")
        );
    }

    #[test]
    fn an_origin_on_the_boundary_needs_no_offset() {
        assert_eq!(offset_of("1 year", "2024-01-01"), None);
        assert_eq!(offset_of("1 quarter", "2024-04-01"), None);
        assert_eq!(offset_of("1 month", "2024-04-01"), None);
        // 2024-01-01 is a Monday.
        assert_eq!(offset_of("1 week", "2024-01-01"), None);
        assert_eq!(offset_of("1 day", "2024-04-01"), None);
        assert_eq!(offset_of("1 hour", "2024-04-01T06:00:00"), None);
        assert_eq!(offset_of("1 minute", "2024-04-01T06:30:00"), None);
    }

    #[test]
    fn a_day_of_month_out_of_reach_has_no_offset_form() {
        // February 29 is the only day a year grain cannot reach: the shift lands on March 1 in
        // every other year.
        assert_eq!(offset_of("1 year", "2024-02-29"), None);
        // Every other day past the 28th is exact for a year grain, since only February varies.
        assert_eq!(
            offset_of("1 year", "2024-04-30").as_deref(),
            Some("3 month 29 day")
        );
        assert_eq!(offset_of("1 year", "2024-01-31").as_deref(), Some("30 day"));
        assert_eq!(
            offset_of("1 year", "2024-05-31").as_deref(),
            Some("4 month 30 day")
        );
        // Month and quarter grains drift once `add_interval` clamps, so the whole tail is out.
        assert_eq!(offset_of("1 month", "2024-01-29"), None);
        assert_eq!(offset_of("1 month", "2024-01-30"), None);
        assert_eq!(offset_of("1 quarter", "2024-01-31"), None);
        assert_eq!(offset_of("1 quarter", "2024-05-31"), None);
        // Day counts stay exact for week- and time-based intervals.
        assert_eq!(offset_of("1 week", "2024-01-31").as_deref(), Some("2 day"));
        assert_eq!(
            offset_of("1 day", "2024-01-31T06:00:00").as_deref(),
            Some("6 hour")
        );
    }

    #[test]
    fn a_derived_offset_keeps_the_grain_on_the_date_trunc_rendering() {
        assert!(custom("1 year", Some("2024-04-01"), None).is_natural_aligned());
        assert!(custom("1 year", Some("2024-01-01"), None).is_natural_aligned());
        assert!(custom("1 month", Some("2024-01-15"), None).is_natural_aligned());
        // The residue that has no offset form falls through to DATE_BIN.
        assert!(!custom("1 month", Some("2024-01-30"), None).is_natural_aligned());
        assert!(!custom("1 year", Some("2024-02-29"), None).is_natural_aligned());
        assert!(custom("1 year", Some("2024-04-30"), None).is_natural_aligned());
    }

    #[test]
    fn a_derived_offset_does_not_move_the_origin() {
        let g = custom("1 year", Some("2024-04-01"), None);
        assert_eq!(g.origin_local_formatted(), "2024-04-01T00:00:00.000");
    }

    #[test]
    fn a_derived_offset_reports_the_same_min_granularity_as_the_origin() {
        // Pre-aggregation matching reads `min_granularity`, which switches to the offset branch
        // once an offset exists; both branches must agree on the grain.
        assert_eq!(
            custom("1 year", Some("2024-04-01"), None)
                .min_granularity()
                .unwrap(),
            Some("month".to_string())
        );
        assert_eq!(
            custom("1 year", Some("2024-04-15"), None)
                .min_granularity()
                .unwrap(),
            Some("day".to_string())
        );
        assert_eq!(
            custom("1 day", Some("2024-04-01T06:00:00"), None)
                .min_granularity()
                .unwrap(),
            Some("hour".to_string())
        );
    }

    #[test]
    fn default_and_offset_origins_keep_their_existing_alignment() {
        // No explicit origin: the default origin is the start of the year, and a week-only
        // interval snaps to Monday — both natural boundaries.
        assert!(custom("1 year", None, None).is_natural_aligned());
        assert!(custom("1 week", None, None).is_natural_aligned());
        assert_eq!(custom("1 year", None, None).granularity_offset(), &None);
        // A model-supplied offset is passed through untouched.
        assert_eq!(
            custom("1 week", None, Some("-1 day"))
                .granularity_offset()
                .as_deref(),
            Some("-1 day")
        );
        assert!(custom("1 week", None, Some("-1 day")).is_natural_aligned());
    }

    #[test]
    fn non_trivial_intervals_are_never_natural_aligned() {
        assert!(!custom("15 minutes", None, None).is_natural_aligned());
        assert!(!custom("6 months", Some("2024-01-01"), None).is_natural_aligned());
        assert_eq!(offset_of("6 months", "2024-04-01"), None);
    }
}
