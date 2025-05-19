use std::rc::Rc;

use crate::planner::query_tools::QueryTools;
use chrono::{DateTime, Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use regex::Regex;
pub struct QueryDateTimeHelper {}

use lazy_static::lazy_static;
lazy_static! {
    static ref DATE_TIME_LOCAL_MS_RE: Regex =
        Regex::new(r"^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d$").unwrap();
    static ref DATE_TIME_LOCAL_U_RE: Regex =
        Regex::new(r"^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d\d\d\d$").unwrap();
    static ref DATE_RE: Regex = Regex::new(r"^\d\d\d\d-\d\d-\d\d$").unwrap();
}
impl QueryDateTimeHelper {
    pub fn parse_native_date_time(date: &str) -> Result<NaiveDateTime, CubeError> {
        let formats = &[
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S%.f",
        ];

        for format in formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(date, format) {
                return Ok(dt);
            }
        }

        if let Ok(d) = NaiveDate::parse_from_str(date, "%Y-%m-%d") {
            return Ok(d.and_hms_opt(0, 0, 0).unwrap());
        }

        Err(CubeError::user(format!("Can't parse date: '{}'", date)))
    }

    pub fn resolve_local_result(
        tz: &Tz,
        origin_date: &NaiveDateTime,
        local_result: LocalResult<DateTime<Tz>>,
    ) -> Result<DateTime<Tz>, CubeError> {
        match local_result {
            LocalResult::Single(dt) => Ok(dt),
            LocalResult::Ambiguous(dt, _) => Ok(dt),
            LocalResult::None => Self::resolve_nonexistent(tz, origin_date),
        }
    }

    /// Resolves a nonexistent local time (DST gap) using binary search,
    /// mimicking `moment.tz` behavior — finds the nearest valid local time **after or equal to** the input.
    ///
    /// Assumes that any DST gap is less than 2 hours (true for almost all real-world time zones).
    fn resolve_nonexistent(tz: &Tz, date: &NaiveDateTime) -> Result<DateTime<Tz>, CubeError> {
        // Max time delta to look ahead — generous enough for most DST gaps
        let max_offset = Duration::hours(2);

        let mut low = date.clone();
        let mut high = low + max_offset;

        // Ensure `high` is a valid local time (expand if needed)
        while let LocalResult::None = tz.from_local_datetime(&high) {
            high = high + max_offset;
        }

        // Binary search for the first valid local time >= `naive`
        while high - low > Duration::seconds(1) {
            let mid = low + (high - low) / 2;
            match tz.from_local_datetime(&mid) {
                LocalResult::None => {
                    // Still in the invalid range — move lower bound up
                    low = mid;
                }
                _ => {
                    // Found a valid or ambiguous time — narrow upper bound
                    high = mid;
                }
            }
        }

        // Return the first valid interpretation
        match tz.from_local_datetime(&high) {
            LocalResult::Single(dt) => Ok(dt),
            LocalResult::Ambiguous(dt, _) => Ok(dt),
            LocalResult::None => Err(CubeError::user(format!(
                "Could not resolve nonexistent time {date} in timezone {}",
                tz
            ))),
        }
    }

    pub fn format_from_date(date: &str, query_tools: Rc<QueryTools>) -> Result<String, CubeError> {
        let precision = query_tools.base_tools().timestamp_precision()?;
        if precision == 3 {
            if DATE_TIME_LOCAL_MS_RE.is_match(date) {
                return Ok(date.to_string());
            }
        } else if precision == 6 {
            if date.len() == 23 && DATE_TIME_LOCAL_MS_RE.is_match(date) {
                return Ok(format!("{}000", date));
            } else if date.len() == 26 && DATE_TIME_LOCAL_U_RE.is_match(date) {
                return Ok(date.to_string());
            }
        } else {
            return Err(CubeError::user(format!(
                "Unsupported timestamp precision: {}",
                precision
            )));
        }

        if DATE_RE.is_match(date) {
            return Ok(format!(
                "{}T00:00:00.{}",
                date,
                "0".repeat(precision as usize)
            ));
        }
        Ok(date.to_string())
    }

    pub fn format_to_date(date: &str, query_tools: Rc<QueryTools>) -> Result<String, CubeError> {
        let precision = query_tools.base_tools().timestamp_precision()?;
        if precision == 3 {
            if DATE_TIME_LOCAL_MS_RE.is_match(date) {
                return Ok(date.to_string());
            }
        } else if precision == 6 {
            if date.len() == 23 && DATE_TIME_LOCAL_MS_RE.is_match(date) {
                if date.ends_with(".999") {
                    return Ok(format!("{}999", date));
                }
                return Ok(format!("{}000", date));
            } else if date.len() == 26 && DATE_TIME_LOCAL_U_RE.is_match(date) {
                return Ok(date.to_string());
            }
        } else {
            return Err(CubeError::user(format!(
                "Unsupported timestamp precision: {}",
                precision
            )));
        }

        if DATE_RE.is_match(date) {
            return Ok(format!(
                "{}T23:59:59.{}",
                date,
                "9".repeat(precision as usize)
            ));
        }

        Ok(date.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_native_date_time() {
        assert_eq!(
            QueryDateTimeHelper::parse_native_date_time("2021-01-01").unwrap(),
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
        assert_eq!(
            QueryDateTimeHelper::parse_native_date_time("2021-01-01T12:10:15").unwrap(),
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_opt(12, 10, 15)
                .unwrap()
        );
        assert_eq!(
            QueryDateTimeHelper::parse_native_date_time("2021-01-01 12:10:15").unwrap(),
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_opt(12, 10, 15)
                .unwrap()
        );
        assert_eq!(
            QueryDateTimeHelper::parse_native_date_time("2021-01-01 12:10:15.345").unwrap(),
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_milli_opt(12, 10, 15, 345)
                .unwrap()
        );
        assert_eq!(
            QueryDateTimeHelper::parse_native_date_time("2021-01-01T12:10:15.345").unwrap(),
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_milli_opt(12, 10, 15, 345)
                .unwrap()
        );
    }
}
