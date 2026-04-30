use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use cubenativeutils::CubeError;

const TIMESTAMP_PRECISION: usize = 3;

/// Generates a time series for a given granularity and date range.
/// Matches `timeSeries()` from `@cubejs-backend/shared/src/time.ts`.
pub fn generate_time_series(
    granularity: &str,
    date_range: &[String],
) -> Result<Vec<Vec<String>>, CubeError> {
    if date_range.len() != 2 {
        return Err(CubeError::internal(
            "date_range must have exactly 2 elements".to_string(),
        ));
    }

    let start = parse_date(&date_range[0])?;
    let end = parse_date(&date_range[1])?;

    let snap = snap_fn(granularity)
        .ok_or_else(|| CubeError::user(format!("Unsupported time granularity: {granularity}")))?;
    let advance = advance_fn(granularity).unwrap();
    let period_end = period_end_fn(granularity).unwrap();

    let mut current = snap(start);
    let mut result = Vec::new();
    while current <= end {
        let to = period_end(current);
        result.push(vec![format_from(current), format_to(to)]);
        current = advance(current);
    }

    Ok(result)
}

type DateFn = fn(NaiveDateTime) -> NaiveDateTime;

/// Snap datetime to the start of its granularity period
fn snap_fn(g: &str) -> Option<DateFn> {
    Some(match g {
        "second" => |dt| dt.with_nanosecond(0).unwrap(),
        "minute" => |dt| make(dt.date(), dt.hour(), dt.minute(), 0),
        "hour" => |dt| make(dt.date(), dt.hour(), 0, 0),
        "day" => |dt| day_start(dt.date()),
        "week" => |dt| {
            let days_from_mon = dt.date().weekday().num_days_from_monday();
            day_start(dt.date() - Duration::days(days_from_mon as i64))
        },
        "month" => |dt| day_start(NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).unwrap()),
        "quarter" => |dt| {
            let q_month = (dt.month() - 1) / 3 * 3 + 1;
            day_start(NaiveDate::from_ymd_opt(dt.year(), q_month, 1).unwrap())
        },
        "year" => |dt| day_start(NaiveDate::from_ymd_opt(dt.year(), 1, 1).unwrap()),
        _ => return None,
    })
}

/// Advance to the next period
fn advance_fn(g: &str) -> Option<DateFn> {
    Some(match g {
        "second" => |dt| dt + Duration::seconds(1),
        "minute" => |dt| dt + Duration::minutes(1),
        "hour" => |dt| dt + Duration::hours(1),
        "day" => |dt| dt + Duration::days(1),
        "week" => |dt| dt + Duration::weeks(1),
        "month" => |dt| add_months(dt, 1),
        "quarter" => |dt| add_months(dt, 3),
        "year" => |dt| add_months(dt, 12),
        _ => return None,
    })
}

/// Get the end of the current period
fn period_end_fn(g: &str) -> Option<DateFn> {
    Some(match g {
        "second" => |dt| dt, // same second
        "minute" => |dt| make(dt.date(), dt.hour(), dt.minute(), 59),
        "hour" => |dt| make(dt.date(), dt.hour(), 59, 59),
        "day" => |dt| day_end(dt.date()),
        "week" => |dt| day_end(dt.date() + Duration::days(6)),
        "month" => |dt| day_end(last_day_of_month(dt.year(), dt.month())),
        "quarter" => |dt| {
            let last_month = (dt.month() - 1) / 3 * 3 + 3;
            day_end(last_day_of_month(dt.year(), last_month))
        },
        "year" => |dt| day_end(NaiveDate::from_ymd_opt(dt.year(), 12, 31).unwrap()),
        _ => return None,
    })
}

fn make(date: NaiveDate, h: u32, m: u32, s: u32) -> NaiveDateTime {
    date.and_time(NaiveTime::from_hms_opt(h, m, s).unwrap())
}

fn day_start(d: NaiveDate) -> NaiveDateTime {
    make(d, 0, 0, 0)
}

fn day_end(d: NaiveDate) -> NaiveDateTime {
    make(d, 23, 59, 59)
}

fn add_months(dt: NaiveDateTime, months: u32) -> NaiveDateTime {
    let total = dt.month0() + months;
    let new_year = dt.year() + (total / 12) as i32;
    let new_month = total % 12 + 1;
    day_start(NaiveDate::from_ymd_opt(new_year, new_month, 1).unwrap())
}

fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    let (y, m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(y, m, 1).unwrap() - Duration::days(1)
}

fn parse_date(s: &str) -> Result<NaiveDateTime, CubeError> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| d.and_hms_opt(0, 0, 0).unwrap())
        })
        .map_err(|_| CubeError::internal(format!("Cannot parse date: '{s}'")))
}

fn format_from(dt: NaiveDateTime) -> String {
    format!(
        "{}.{}",
        dt.format("%Y-%m-%dT%H:%M:%S"),
        "0".repeat(TIMESTAMP_PRECISION)
    )
}

fn format_to(dt: NaiveDateTime) -> String {
    format!(
        "{}.{}",
        dt.format("%Y-%m-%dT%H:%M:%S"),
        "9".repeat(TIMESTAMP_PRECISION)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_day() {
        let r = generate_time_series("day", &["2025-10-07".into(), "2025-10-09".into()]).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(
            r[0],
            vec!["2025-10-07T00:00:00.000", "2025-10-07T23:59:59.999"]
        );
        assert_eq!(
            r[2],
            vec!["2025-10-09T00:00:00.000", "2025-10-09T23:59:59.999"]
        );
    }

    #[test]
    fn test_month() {
        let r = generate_time_series("month", &["2025-01-15".into(), "2025-03-10".into()]).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], "2025-01-01T00:00:00.000");
        assert_eq!(r[0][1], "2025-01-31T23:59:59.999");
        assert_eq!(r[1][1], "2025-02-28T23:59:59.999");
    }

    #[test]
    fn test_week() {
        // 2025-10-07 is Tuesday, snaps to Monday 2025-10-06
        let r = generate_time_series("week", &["2025-10-07".into(), "2025-10-14".into()]).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0][0], "2025-10-06T00:00:00.000");
        assert_eq!(r[0][1], "2025-10-12T23:59:59.999");
    }

    #[test]
    fn test_quarter() {
        let r =
            generate_time_series("quarter", &["2025-01-15".into(), "2025-07-10".into()]).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], "2025-01-01T00:00:00.000");
        assert_eq!(r[0][1], "2025-03-31T23:59:59.999");
        assert_eq!(r[2][0], "2025-07-01T00:00:00.000");
    }

    #[test]
    fn test_year() {
        let r = generate_time_series("year", &["2024-06-15".into(), "2025-03-10".into()]).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(
            r[0],
            vec!["2024-01-01T00:00:00.000", "2024-12-31T23:59:59.999"]
        );
        assert_eq!(
            r[1],
            vec!["2025-01-01T00:00:00.000", "2025-12-31T23:59:59.999"]
        );
    }

    #[test]
    fn test_hour() {
        let r = generate_time_series(
            "hour",
            &[
                "2025-10-07T10:30:00.000".into(),
                "2025-10-07T12:15:00.000".into(),
            ],
        )
        .unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0][0], "2025-10-07T10:00:00.000");
        assert_eq!(r[0][1], "2025-10-07T10:59:59.999");
    }

    #[test]
    fn test_unsupported() {
        assert!(
            generate_time_series("millennium", &["2025-01-01".into(), "2025-01-02".into()])
                .is_err()
        );
    }
}
