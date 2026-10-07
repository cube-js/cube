use super::{QueryDateTime, SqlInterval};
use chrono::Utc;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use lazy_static::lazy_static;
use regex::Regex;
use std::str::FromStr;

lazy_static! {
    static ref RELATIVE: Regex =
        Regex::new(r"^(this|last|next)\s+(day|week|month|year|quarter|hour|minute|second)s?$")
            .unwrap();
    static ref RELATIVE_N: Regex =
        Regex::new(r"^(last|next)\s+(\d+)\s+(day|week|month|year|quarter|hour|minute|second)s?$")
            .unwrap();
}

/// Resolves a relative date range such as `this month`, `last 7 days` or
/// `yesterday` to its `[start, end]` in `tz`, as the REST API's date parser
/// does for a query's `dateRange`. Returns `None` for anything else, so
/// absolute dates pass through untouched.
pub fn resolve_relative_date_range(
    value: &str,
    tz: Tz,
) -> Result<Option<(String, String)>, CubeError> {
    let now = QueryDateTime::new(Utc::now().with_timezone(&tz));
    resolve_relative_date_range_at(value, &now)
}

pub fn resolve_relative_date_range_at(
    value: &str,
    now: &QueryDateTime,
) -> Result<Option<(String, String)>, CubeError> {
    let value = value.trim().to_lowercase();
    let (unit, start_shift, end_shift) = if let Some(c) = RELATIVE.captures(&value) {
        let shift = match &c[1] {
            "last" => -1,
            "next" => 1,
            _ => 0,
        };
        (c[2].to_string(), shift, shift)
    } else if let Some(c) = RELATIVE_N.captures(&value) {
        let n: i64 = c[2]
            .parse()
            .map_err(|_| CubeError::user(format!("Can't parse date range: '{}'", value)))?;
        let (start, end) = if &c[1] == "last" { (-n, -1) } else { (1, n) };
        (c[3].to_string(), start, end)
    } else {
        match value.as_str() {
            "today" => ("day".to_string(), 0, 0),
            "yesterday" => ("day".to_string(), -1, -1),
            "tomorrow" => ("day".to_string(), 1, 1),
            _ => return Ok(None),
        }
    };
    let start = shift(now, &unit, start_shift)?.start_of(&unit)?;
    let end = end_of(&shift(now, &unit, end_shift)?, &unit)?;
    Ok(Some((start.default_format(), end.default_format())))
}

fn shift(now: &QueryDateTime, unit: &str, n: i64) -> Result<QueryDateTime, CubeError> {
    if n == 0 {
        return Ok(now.clone());
    }
    now.add_interval(&SqlInterval::from_str(&format!("{} {}", n, unit))?)
}

fn end_of(date: &QueryDateTime, unit: &str) -> Result<QueryDateTime, CubeError> {
    date.start_of(unit)?
        .add_interval(&SqlInterval::from_str(&format!("1 {}", unit))?)?
        .add_duration(chrono::Duration::milliseconds(-1))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(value: &str) -> Option<(String, String)> {
        let now = QueryDateTime::from_date_str(Tz::UTC, "2026-10-07T13:45:10.000").unwrap();
        resolve_relative_date_range_at(value, &now).unwrap()
    }

    fn range(from: &str, to: &str) -> Option<(String, String)> {
        Some((from.to_string(), to.to_string()))
    }

    #[test]
    fn this_last_next() {
        assert_eq!(
            at("this month"),
            range("2026-10-01T00:00:00.000", "2026-10-31T23:59:59.999")
        );
        assert_eq!(
            at("Last month"),
            range("2026-09-01T00:00:00.000", "2026-09-30T23:59:59.999")
        );
        assert_eq!(
            at("next quarter"),
            range("2027-01-01T00:00:00.000", "2027-03-31T23:59:59.999")
        );
        // Weeks start on Monday, as ISO weeks do in the REST API.
        assert_eq!(
            at("this week"),
            range("2026-10-05T00:00:00.000", "2026-10-11T23:59:59.999")
        );
    }

    #[test]
    fn last_and_next_n() {
        assert_eq!(
            at("last 7 days"),
            range("2026-09-30T00:00:00.000", "2026-10-06T23:59:59.999")
        );
        assert_eq!(
            at("last 3 months"),
            range("2026-07-01T00:00:00.000", "2026-09-30T23:59:59.999")
        );
        assert_eq!(
            at("next 2 years"),
            range("2027-01-01T00:00:00.000", "2028-12-31T23:59:59.999")
        );
    }

    #[test]
    fn named_days() {
        assert_eq!(
            at("today"),
            range("2026-10-07T00:00:00.000", "2026-10-07T23:59:59.999")
        );
        assert_eq!(
            at("yesterday"),
            range("2026-10-06T00:00:00.000", "2026-10-06T23:59:59.999")
        );
    }

    #[test]
    fn absolute_dates_are_not_relative() {
        assert_eq!(at("2026-06-01"), None);
        assert_eq!(at("from 2026-01-01 to 2026-02-01"), None);
    }

    #[test]
    fn resolves_in_the_query_time_zone() {
        // 23:30 UTC on Sep 30 is already Oct 1 in Tokyo.
        let now = QueryDateTime::from_date_str(Tz::UTC, "2026-09-30T23:30:00.000")
            .unwrap()
            .date_time()
            .with_timezone(&Tz::Asia__Tokyo);
        assert_eq!(
            resolve_relative_date_range_at("this month", &QueryDateTime::new(now)).unwrap(),
            range("2026-10-01T00:00:00.000", "2026-10-31T23:59:59.999")
        );
    }
}
