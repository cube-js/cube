use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use cubenativeutils::CubeError;

const TIMESTAMP_PRECISION: usize = 3;

/// Generates a time series for a given granularity and date range.
///
/// Returns a list of `[date_from, date_to]` pairs covering the range,
/// snapped and iterated by the given granularity.
///
/// Matches the behavior of `timeSeries()` from `@cubejs-backend/shared/src/time.ts`.
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

    let series = match granularity {
        "second" => generate_by_second(start, end),
        "minute" => generate_by_minute(start, end),
        "hour" => generate_by_hour(start, end),
        "day" => generate_by_day(start, end),
        "week" => generate_by_week(start, end),
        "month" => generate_by_month(start, end),
        "quarter" => generate_by_quarter(start, end),
        "year" => generate_by_year(start, end),
        _ => {
            return Err(CubeError::user(format!(
                "Unsupported time granularity: {}",
                granularity
            )));
        }
    };

    Ok(series)
}

fn parse_date(s: &str) -> Result<NaiveDateTime, CubeError> {
    // Try full ISO format first: "2025-10-07T00:00:00.000"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Ok(dt);
    }
    // Try without fractional: "2025-10-07T00:00:00"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt);
    }
    // Try date only: "2025-10-07"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap());
    }
    Err(CubeError::internal(format!(
        "Cannot parse date: '{}'",
        s
    )))
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

fn generate_by_second(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut current = start
        .with_nanosecond(0)
        .unwrap();
    let mut result = Vec::new();
    while current <= end {
        let from = current;
        let to = current;
        result.push(vec![format_from(from), format_to(to)]);
        current += Duration::seconds(1);
    }
    result
}

fn generate_by_minute(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut current = start
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let mut result = Vec::new();
    while current <= end {
        let from = current;
        let to = current.with_second(59).unwrap();
        result.push(vec![format_from(from), format_to(to)]);
        current += Duration::minutes(1);
    }
    result
}

fn generate_by_hour(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut current = start
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let mut result = Vec::new();
    while current <= end {
        let from = current;
        let to = current.with_minute(59).unwrap().with_second(59).unwrap();
        result.push(vec![format_from(from), format_to(to)]);
        current += Duration::hours(1);
    }
    result
}

fn generate_by_day(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut current = start.date();
    let end_date = end.date();
    let mut result = Vec::new();
    while current <= end_date {
        let from = current.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let to = current.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        result.push(vec![format_from(from), format_to(to)]);
        current += Duration::days(1);
    }
    result
}

fn generate_by_week(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    // Snap to ISO week start (Monday)
    let start_date = start.date();
    let weekday = start_date.weekday().num_days_from_monday();
    let mut current = start_date - Duration::days(weekday as i64);
    let end_date = end.date();
    let mut result = Vec::new();
    while current <= end_date {
        let from = current.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let week_end = current + Duration::days(6);
        let to = week_end.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        result.push(vec![format_from(from), format_to(to)]);
        current += Duration::weeks(1);
    }
    result
}

fn generate_by_month(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut year = start.year();
    let mut month = start.month();
    let end_date = end.date();
    let mut result = Vec::new();
    loop {
        let first_day = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        if first_day > end_date {
            break;
        }
        let last_day = last_day_of_month(year, month);
        let from = first_day.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let to = last_day.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        result.push(vec![format_from(from), format_to(to)]);
        month += 1;
        if month > 12 {
            month = 1;
            year += 1;
        }
    }
    result
}

fn generate_by_quarter(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut year = start.year();
    let mut quarter = (start.month() - 1) / 3; // 0-based
    let end_date = end.date();
    let mut result = Vec::new();
    loop {
        let first_month = quarter * 3 + 1;
        let first_day = NaiveDate::from_ymd_opt(year, first_month, 1).unwrap();
        if first_day > end_date {
            break;
        }
        let last_month = first_month + 2;
        let last_day = last_day_of_month(year, last_month);
        let from = first_day.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let to = last_day.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        result.push(vec![format_from(from), format_to(to)]);
        quarter += 1;
        if quarter > 3 {
            quarter = 0;
            year += 1;
        }
    }
    result
}

fn generate_by_year(start: NaiveDateTime, end: NaiveDateTime) -> Vec<Vec<String>> {
    let mut year = start.year();
    let end_year = end.year();
    let mut result = Vec::new();
    while year <= end_year {
        let first_day = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
        let last_day = NaiveDate::from_ymd_opt(year, 12, 31).unwrap();
        let from = first_day.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let to = last_day.and_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap());
        result.push(vec![format_from(from), format_to(to)]);
        year += 1;
    }
    result
}

fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1).unwrap() - Duration::days(1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap() - Duration::days(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_day_granularity() {
        let result =
            generate_time_series("day", &["2025-10-07".into(), "2025-10-09".into()]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2025-10-07T00:00:00.000");
        assert_eq!(result[0][1], "2025-10-07T23:59:59.999");
        assert_eq!(result[1][0], "2025-10-08T00:00:00.000");
        assert_eq!(result[1][1], "2025-10-08T23:59:59.999");
        assert_eq!(result[2][0], "2025-10-09T00:00:00.000");
        assert_eq!(result[2][1], "2025-10-09T23:59:59.999");
    }

    #[test]
    fn test_day_granularity_iso_input() {
        let result = generate_time_series(
            "day",
            &[
                "2025-10-07T00:00:00.000".into(),
                "2025-10-08T23:59:59.999".into(),
            ],
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], "2025-10-07T00:00:00.000");
        assert_eq!(result[1][0], "2025-10-08T00:00:00.000");
    }

    #[test]
    fn test_month_granularity() {
        let result =
            generate_time_series("month", &["2025-01-15".into(), "2025-03-10".into()]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2025-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2025-01-31T23:59:59.999");
        assert_eq!(result[1][0], "2025-02-01T00:00:00.000");
        assert_eq!(result[1][1], "2025-02-28T23:59:59.999");
        assert_eq!(result[2][0], "2025-03-01T00:00:00.000");
        assert_eq!(result[2][1], "2025-03-31T23:59:59.999");
    }

    #[test]
    fn test_week_granularity() {
        // 2025-10-06 is Monday
        let result =
            generate_time_series("week", &["2025-10-07".into(), "2025-10-14".into()]).unwrap();
        assert_eq!(result.len(), 2);
        // Snaps to Monday
        assert_eq!(result[0][0], "2025-10-06T00:00:00.000");
        assert_eq!(result[0][1], "2025-10-12T23:59:59.999");
        assert_eq!(result[1][0], "2025-10-13T00:00:00.000");
        assert_eq!(result[1][1], "2025-10-19T23:59:59.999");
    }

    #[test]
    fn test_year_granularity() {
        let result =
            generate_time_series("year", &["2024-06-15".into(), "2025-03-10".into()]).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2024-12-31T23:59:59.999");
        assert_eq!(result[1][0], "2025-01-01T00:00:00.000");
        assert_eq!(result[1][1], "2025-12-31T23:59:59.999");
    }

    #[test]
    fn test_quarter_granularity() {
        let result =
            generate_time_series("quarter", &["2025-01-15".into(), "2025-07-10".into()]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2025-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2025-03-31T23:59:59.999");
        assert_eq!(result[1][0], "2025-04-01T00:00:00.000");
        assert_eq!(result[1][1], "2025-06-30T23:59:59.999");
        assert_eq!(result[2][0], "2025-07-01T00:00:00.000");
        assert_eq!(result[2][1], "2025-09-30T23:59:59.999");
    }

    #[test]
    fn test_hour_granularity() {
        let result = generate_time_series(
            "hour",
            &[
                "2025-10-07T10:30:00.000".into(),
                "2025-10-07T12:15:00.000".into(),
            ],
        )
        .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2025-10-07T10:00:00.000");
        assert_eq!(result[0][1], "2025-10-07T10:59:59.999");
        assert_eq!(result[1][0], "2025-10-07T11:00:00.000");
        assert_eq!(result[1][1], "2025-10-07T11:59:59.999");
        assert_eq!(result[2][0], "2025-10-07T12:00:00.000");
        assert_eq!(result[2][1], "2025-10-07T12:59:59.999");
    }

    #[test]
    fn test_unsupported_granularity() {
        let result = generate_time_series("millennium", &["2025-01-01".into(), "2025-01-02".into()]);
        assert!(result.is_err());
    }
}
