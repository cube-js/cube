use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, Timelike};
use cubenativeutils::CubeError;
use std::str::FromStr;

use super::date_time_helper::QueryDateTimeHelper;
use super::sql_interval::SqlInterval;

const PREDEFINED_GRANULARITIES: &[&str] = &[
    "second", "minute", "hour", "day", "week", "month", "quarter", "year",
];

// Cap on emitted buckets. Guards against pathologically wide ranges or tiny
// intervals running effectively unbounded; expressed as an iteration limit
// since calendar units have no fixed length in seconds.
const MAX_BUCKETS: usize = 50_000;

pub fn is_predefined_granularity(name: &str) -> bool {
    PREDEFINED_GRANULARITIES.contains(&name)
}

pub struct QueryTimeSeries;

impl QueryTimeSeries {
    /// Snaps the range start to the bucket boundary for the given granularity,
    /// then emits one `[start, end]` pair per bucket until the bucket boundary
    /// passes the range end. The sub-second part of each timestamp is padded
    /// with literal `'0'`/`'9'` characters to the requested precision.
    pub fn generate_predefined(
        granularity: &str,
        date_range: &[String; 2],
        timestamp_precision: u32,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        check_precision(timestamp_precision)?;
        if !is_predefined_granularity(granularity) {
            return Err(CubeError::user(format!(
                "Unsupported time granularity: {granularity}"
            )));
        }
        let range_start = QueryDateTimeHelper::parse_native_date_time(&date_range[0])?;
        let range_end = QueryDateTimeHelper::parse_native_date_time(&date_range[1])?;
        let mut current = range_start;
        let mut buckets = Vec::new();
        loop {
            if buckets.len() >= MAX_BUCKETS {
                return Err(CubeError::user(format!(
                    "Time series exceeded {MAX_BUCKETS} buckets; \
                     reduce date range or use a larger granularity"
                )));
            }
            let window = predefined_bucket(granularity, current, timestamp_precision)?;
            // Inclusive upper bound: a bucket whose start lands exactly on
            // range_end is kept. The custom path is exclusive (`aligned <
            // range_end`); the asymmetry is intentional and matches the
            // established predefined/custom semantics.
            if window.bucket_start > range_end {
                break;
            }
            buckets.push(vec![window.start_str, window.end_str]);
            current = window.next;
        }
        Ok(buckets)
    }

    /// Walks buckets by repeatedly adding the parsed interval starting from the
    /// position aligned to `origin`. Each bucket's end is `next_start - 1s`,
    /// formatted with the sub-second `'9'` padding.
    pub fn generate_custom(
        interval_str: &str,
        date_range: &[String; 2],
        origin_str: &str,
        timestamp_precision: u32,
    ) -> Result<Vec<Vec<String>>, CubeError> {
        check_precision(timestamp_precision)?;
        let interval = SqlInterval::from_str(interval_str)?;
        if is_zero_interval(&interval) {
            return Err(CubeError::user("Custom interval can't be zero".to_string()));
        }
        let range_start = QueryDateTimeHelper::parse_native_date_time(&date_range[0])?;
        let range_end = QueryDateTimeHelper::parse_native_date_time(&date_range[1])?;
        let origin = QueryDateTimeHelper::parse_native_date_time(origin_str)?;
        let zeros = "0".repeat(timestamp_precision as usize);
        let nines = "9".repeat(timestamp_precision as usize);
        let mut aligned = align_to_origin(range_start, &interval, origin)?;
        let mut buckets = Vec::new();
        while aligned < range_end {
            if buckets.len() >= MAX_BUCKETS {
                return Err(CubeError::user(format!(
                    "Custom time series exceeded {MAX_BUCKETS} buckets; \
                     reduce date range or use a larger interval"
                )));
            }
            let next = add_interval_to_dt(aligned, &interval)?;
            if next <= aligned {
                return Err(CubeError::user(
                    "Custom interval did not advance the cursor".to_string(),
                ));
            }
            let bucket_end = next - Duration::seconds(1);
            buckets.push(vec![
                format_with_padding(aligned, &zeros),
                format_with_padding(bucket_end, &nines),
            ]);
            aligned = next;
        }
        Ok(buckets)
    }
}

struct BucketWindow {
    bucket_start: NaiveDateTime,
    start_str: String,
    end_str: String,
    next: NaiveDateTime,
}

fn check_precision(precision: u32) -> Result<(), CubeError> {
    match precision {
        3 | 6 => Ok(()),
        other => Err(CubeError::user(format!(
            "Unsupported timestamp precision: {other}"
        ))),
    }
}

fn predefined_bucket(
    granularity: &str,
    pos: NaiveDateTime,
    precision: u32,
) -> Result<BucketWindow, CubeError> {
    let zeros = "0".repeat(precision as usize);
    let nines = "9".repeat(precision as usize);
    let res = match granularity {
        "second" => {
            let start = pos
                .date()
                .and_hms_opt(pos.hour(), pos.minute(), pos.second())
                .unwrap();
            let next = start + Duration::seconds(1);
            BucketWindow {
                bucket_start: start,
                start_str: format!(
                    "{}T{:02}:{:02}:{:02}.{zeros}",
                    start.date(),
                    start.hour(),
                    start.minute(),
                    start.second()
                ),
                end_str: format!(
                    "{}T{:02}:{:02}:{:02}.{nines}",
                    start.date(),
                    start.hour(),
                    start.minute(),
                    start.second()
                ),
                next,
            }
        }
        "minute" => {
            let start = pos.date().and_hms_opt(pos.hour(), pos.minute(), 0).unwrap();
            let next = start + Duration::minutes(1);
            BucketWindow {
                bucket_start: start,
                start_str: format!(
                    "{}T{:02}:{:02}:00.{zeros}",
                    start.date(),
                    start.hour(),
                    start.minute()
                ),
                end_str: format!(
                    "{}T{:02}:{:02}:59.{nines}",
                    start.date(),
                    start.hour(),
                    start.minute()
                ),
                next,
            }
        }
        "hour" => {
            let start = pos.date().and_hms_opt(pos.hour(), 0, 0).unwrap();
            let next = start + Duration::hours(1);
            BucketWindow {
                bucket_start: start,
                start_str: format!("{}T{:02}:00:00.{zeros}", start.date(), start.hour()),
                end_str: format!("{}T{:02}:59:59.{nines}", start.date(), start.hour()),
                next,
            }
        }
        "day" => {
            let start = pos.date().and_hms_opt(0, 0, 0).unwrap();
            let next = start + Duration::days(1);
            BucketWindow {
                bucket_start: start,
                start_str: format!("{}T00:00:00.{zeros}", start.date()),
                end_str: format!("{}T23:59:59.{nines}", start.date()),
                next,
            }
        }
        "week" => {
            // ISO week: Monday-anchored.
            let weekday = pos.date().weekday();
            // Monday=0 .. Sunday=6 (chrono uses num_days_from_monday()).
            let from_monday = weekday.num_days_from_monday() as i64;
            let monday = pos.date() - Duration::days(from_monday);
            let sunday = monday + Duration::days(6);
            let start = monday.and_hms_opt(0, 0, 0).unwrap();
            let next = start + Duration::weeks(1);
            BucketWindow {
                bucket_start: start,
                start_str: format!("{}T00:00:00.{zeros}", monday),
                end_str: format!("{}T23:59:59.{nines}", sunday),
                next,
            }
        }
        "month" => {
            let first = NaiveDate::from_ymd_opt(pos.year(), pos.month(), 1)
                .ok_or_else(|| CubeError::user(format!("Invalid date in bucket for {pos}")))?;
            let next_first = add_months(first, 1)?;
            let last = next_first - Duration::days(1);
            let start = first.and_hms_opt(0, 0, 0).unwrap();
            BucketWindow {
                bucket_start: start,
                start_str: format!("{}T00:00:00.{zeros}", first),
                end_str: format!("{}T23:59:59.{nines}", last),
                next: next_first.and_hms_opt(0, 0, 0).unwrap(),
            }
        }
        "quarter" => {
            // Quarter starts: Jan, Apr, Jul, Oct.
            let qmonth = ((pos.month() - 1) / 3) * 3 + 1;
            let first = NaiveDate::from_ymd_opt(pos.year(), qmonth, 1)
                .ok_or_else(|| CubeError::user(format!("Invalid quarter start for {pos}")))?;
            let next_first = add_months(first, 3)?;
            let last = next_first - Duration::days(1);
            let start = first.and_hms_opt(0, 0, 0).unwrap();
            BucketWindow {
                bucket_start: start,
                start_str: format!("{}T00:00:00.{zeros}", first),
                end_str: format!("{}T23:59:59.{nines}", last),
                next: next_first.and_hms_opt(0, 0, 0).unwrap(),
            }
        }
        "year" => {
            let first = NaiveDate::from_ymd_opt(pos.year(), 1, 1).unwrap();
            let next_first = NaiveDate::from_ymd_opt(pos.year() + 1, 1, 1).unwrap();
            let last = next_first - Duration::days(1);
            let start = first.and_hms_opt(0, 0, 0).unwrap();
            BucketWindow {
                bucket_start: start,
                start_str: format!("{}T00:00:00.{zeros}", first),
                end_str: format!("{}T23:59:59.{nines}", last),
                next: next_first.and_hms_opt(0, 0, 0).unwrap(),
            }
        }
        other => {
            return Err(CubeError::user(format!(
                "Unsupported time granularity: {other}"
            )))
        }
    };
    Ok(res)
}

fn is_zero_interval(interval: &SqlInterval) -> bool {
    interval.year == 0
        && interval.quarter == 0
        && interval.month == 0
        && interval.week == 0
        && interval.day == 0
        && interval.hour == 0
        && interval.minute == 0
        && interval.second == 0
}

fn add_months(date: NaiveDate, months: u32) -> Result<NaiveDate, CubeError> {
    date.checked_add_months(Months::new(months)).ok_or_else(|| {
        CubeError::user(format!(
            "Date overflow when adding {months} months to {date}"
        ))
    })
}

fn sub_months(date: NaiveDate, months: u32) -> Result<NaiveDate, CubeError> {
    date.checked_sub_months(Months::new(months)).ok_or_else(|| {
        CubeError::user(format!(
            "Date overflow when subtracting {months} months from {date}"
        ))
    })
}

fn add_interval_to_dt(
    dt: NaiveDateTime,
    interval: &SqlInterval,
) -> Result<NaiveDateTime, CubeError> {
    // `SqlInterval` is field-based, so the written order of the source string
    // is already lost. Units are applied months → days → sub-day; month-end
    // clamping (e.g. Mar 31 + 1 month → Apr 30) therefore happens before days
    // are added.
    let mut date = dt.date();
    let time = dt.time();
    // Calendar parts first (year/quarter/month) — they're not fixed in seconds.
    let total_months = interval.year * 12 + interval.quarter * 3 + interval.month;
    date = apply_months(date, total_months)?;
    let extra_days = interval.week * 7 + interval.day;
    if extra_days != 0 {
        date = date
            .checked_add_signed(Duration::days(extra_days as i64))
            .ok_or_else(|| CubeError::user(format!("Date overflow adding days to {dt}")))?;
    }
    // Sub-day part: lower into a Duration and add to the combined dt.
    let sub_day = Duration::hours(interval.hour as i64)
        + Duration::minutes(interval.minute as i64)
        + Duration::seconds(interval.second as i64);
    let combined = date.and_time(time);
    combined
        .checked_add_signed(sub_day)
        .ok_or_else(|| CubeError::user(format!("Date overflow adding sub-day to {dt}")))
}

fn sub_interval_from_dt(
    dt: NaiveDateTime,
    interval: &SqlInterval,
) -> Result<NaiveDateTime, CubeError> {
    let neg = SqlInterval::new(
        -interval.year,
        -interval.quarter,
        -interval.month,
        -interval.week,
        -interval.day,
        -interval.hour,
        -interval.minute,
        -interval.second,
    );
    add_interval_to_dt(dt, &neg)
}

fn apply_months(date: NaiveDate, months_signed: i32) -> Result<NaiveDate, CubeError> {
    if months_signed >= 0 {
        add_months(date, months_signed as u32)
    } else {
        sub_months(date, (-months_signed) as u32)
    }
}

fn align_to_origin(
    start: NaiveDateTime,
    interval: &SqlInterval,
    origin: NaiveDateTime,
) -> Result<NaiveDateTime, CubeError> {
    let mut aligned = start;
    let mut offset = origin;
    // Cap iterations: a net-negative (or net-zero) interval would otherwise
    // step away from `start` forever instead of converging on it.
    let mut steps = 0;
    if start < origin {
        // Pull origin back until it sits at or below start.
        while offset > start {
            converge_guard(&mut steps)?;
            offset = sub_interval_from_dt(offset, interval)?;
        }
        aligned = offset;
    } else {
        // Push origin forward; remember the last step that didn't overshoot.
        while offset < start {
            converge_guard(&mut steps)?;
            aligned = offset;
            offset = add_interval_to_dt(offset, interval)?;
        }
        if offset == start {
            aligned = offset;
        }
    }
    Ok(aligned)
}

fn converge_guard(steps: &mut usize) -> Result<(), CubeError> {
    *steps += 1;
    if *steps > MAX_BUCKETS {
        return Err(CubeError::user(
            "Origin alignment did not converge; check the granularity interval".to_string(),
        ));
    }
    Ok(())
}

fn format_with_padding(dt: NaiveDateTime, sub_second: &str) -> String {
    format!(
        "{}T{:02}:{:02}:{:02}.{sub_second}",
        dt.date(),
        dt.hour(),
        dt.minute(),
        dt.second()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dr(a: &str, b: &str) -> [String; 2] {
        [a.to_string(), b.to_string()]
    }

    // ---- predefined ----

    #[test]
    fn day_buckets_basic() {
        let result =
            QueryTimeSeries::generate_predefined("day", &dr("2024-01-10", "2024-01-12"), 3)
                .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            vec!["2024-01-10T00:00:00.000", "2024-01-10T23:59:59.999"]
        );
        assert_eq!(
            result[2],
            vec!["2024-01-12T00:00:00.000", "2024-01-12T23:59:59.999"]
        );
    }

    #[test]
    fn day_buckets_partial_start_snaps_down() {
        let result = QueryTimeSeries::generate_predefined(
            "day",
            &dr("2024-01-10T15:30:00", "2024-01-11T03:00:00"),
            3,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][0], "2024-01-10T00:00:00.000");
        assert_eq!(result[1][1], "2024-01-11T23:59:59.999");
    }

    #[test]
    fn month_buckets_cross_year_boundary() {
        let result =
            QueryTimeSeries::generate_predefined("month", &dr("2023-11-15", "2024-02-05"), 3)
                .unwrap();
        // Buckets: 2023-11, 2023-12, 2024-01, 2024-02
        assert_eq!(result.len(), 4);
        assert_eq!(result[0][0], "2023-11-01T00:00:00.000");
        assert_eq!(result[0][1], "2023-11-30T23:59:59.999");
        assert_eq!(result[1][1], "2023-12-31T23:59:59.999");
        assert_eq!(result[2][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[3][0], "2024-02-01T00:00:00.000");
        assert_eq!(result[3][1], "2024-02-29T23:59:59.999"); // 2024 is leap
    }

    #[test]
    fn quarter_buckets_align_to_quarter_start() {
        let result =
            QueryTimeSeries::generate_predefined("quarter", &dr("2024-02-15", "2024-08-01"), 3)
                .unwrap();
        // Buckets: Q1 (Jan-Mar), Q2 (Apr-Jun), Q3 (Jul-Sep)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2024-03-31T23:59:59.999");
        assert_eq!(result[1][0], "2024-04-01T00:00:00.000");
        assert_eq!(result[1][1], "2024-06-30T23:59:59.999");
        assert_eq!(result[2][0], "2024-07-01T00:00:00.000");
        assert_eq!(result[2][1], "2024-09-30T23:59:59.999");
    }

    #[test]
    fn year_buckets() {
        let result =
            QueryTimeSeries::generate_predefined("year", &dr("2022-06-15", "2024-01-01"), 3)
                .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2022-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2022-12-31T23:59:59.999");
        assert_eq!(result[2][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[2][1], "2024-12-31T23:59:59.999");
    }

    #[test]
    fn week_iso_monday_anchored() {
        // 2024-01-10 was a Wednesday. ISO week starts Mon 2024-01-08.
        let result =
            QueryTimeSeries::generate_predefined("week", &dr("2024-01-10", "2024-01-22"), 3)
                .unwrap();
        // Buckets: week of Jan 8 (Mon)–Jan 14 (Sun), week of Jan 15 (Mon)–Jan 21 (Sun), week of Jan 22 (Mon)–Jan 28 (Sun)
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2024-01-08T00:00:00.000");
        assert_eq!(result[0][1], "2024-01-14T23:59:59.999");
        assert_eq!(result[1][0], "2024-01-15T00:00:00.000");
        assert_eq!(result[1][1], "2024-01-21T23:59:59.999");
        assert_eq!(result[2][0], "2024-01-22T00:00:00.000");
    }

    #[test]
    fn hour_minute_second() {
        let h = QueryTimeSeries::generate_predefined(
            "hour",
            &dr("2024-01-10T10:30:00", "2024-01-10T12:00:00"),
            3,
        )
        .unwrap();
        assert_eq!(h.len(), 3);
        assert_eq!(
            h[0],
            vec!["2024-01-10T10:00:00.000", "2024-01-10T10:59:59.999"]
        );
        assert_eq!(
            h[2],
            vec!["2024-01-10T12:00:00.000", "2024-01-10T12:59:59.999"]
        );

        let m = QueryTimeSeries::generate_predefined(
            "minute",
            &dr("2024-01-10T10:30:15", "2024-01-10T10:32:00"),
            3,
        )
        .unwrap();
        assert_eq!(m.len(), 3);
        assert_eq!(
            m[0],
            vec!["2024-01-10T10:30:00.000", "2024-01-10T10:30:59.999"]
        );

        let s = QueryTimeSeries::generate_predefined(
            "second",
            &dr("2024-01-10T10:30:15", "2024-01-10T10:30:17"),
            3,
        )
        .unwrap();
        assert_eq!(s.len(), 3);
        assert_eq!(
            s[0],
            vec!["2024-01-10T10:30:15.000", "2024-01-10T10:30:15.999"]
        );
    }

    #[test]
    fn precision_six_pads_to_microseconds() {
        let result =
            QueryTimeSeries::generate_predefined("day", &dr("2024-01-10", "2024-01-10"), 6)
                .unwrap();
        assert_eq!(
            result[0],
            vec!["2024-01-10T00:00:00.000000", "2024-01-10T23:59:59.999999"]
        );
    }

    #[test]
    fn unsupported_granularity_errors() {
        let err =
            QueryTimeSeries::generate_predefined("fortnight", &dr("2024-01-10", "2024-01-11"), 3)
                .unwrap_err();
        assert!(err.message.contains("Unsupported time granularity"));
    }

    #[test]
    fn unsupported_precision_errors() {
        let err = QueryTimeSeries::generate_predefined("day", &dr("2024-01-10", "2024-01-11"), 4)
            .unwrap_err();
        assert!(err.message.contains("Unsupported timestamp precision"));
    }

    #[test]
    fn predefined_exceeding_buckets_errors() {
        // One day at second granularity is 86_400 buckets, over the 50k cap.
        let err = QueryTimeSeries::generate_predefined(
            "second",
            &dr("2024-01-10T00:00:00", "2024-01-11T00:00:00"),
            3,
        )
        .unwrap_err();
        assert!(err.message.contains("exceeded"));
    }

    // ---- custom ----

    #[test]
    fn custom_two_day_interval_aligned_at_origin() {
        // Origin 2024-01-01, interval = 2 days. Range starts mid-bucket.
        let result = QueryTimeSeries::generate_custom(
            "2 days",
            &dr("2024-01-04", "2024-01-10"),
            "2024-01-01",
            3,
        )
        .unwrap();
        // Buckets aligned to origin: Jan 1-2, Jan 3-4, Jan 5-6, Jan 7-8, Jan 9-10
        // range_start = Jan 4 → aligned start = Jan 3 (since Jan 1 + 2d = Jan 3 < Jan 4).
        // Iterate while aligned < range_end (Jan 10): Jan 3, Jan 5, Jan 7, Jan 9
        assert_eq!(result.len(), 4);
        assert_eq!(result[0][0], "2024-01-03T00:00:00.000");
        assert_eq!(result[0][1], "2024-01-04T23:59:59.999"); // next=Jan 5, -1s = Jan 4 23:59:59
        assert_eq!(result[1][0], "2024-01-05T00:00:00.000");
        assert_eq!(result[3][0], "2024-01-09T00:00:00.000");
        assert_eq!(result[3][1], "2024-01-10T23:59:59.999");
    }

    #[test]
    fn custom_origin_after_start_walks_backwards() {
        // Origin Jan 10, interval = 3 days, range starts Jan 5.
        // Pull origin back: Jan 10 - 3d = Jan 7, - 3d = Jan 4 (≤ Jan 5). Aligned = Jan 4.
        let result = QueryTimeSeries::generate_custom(
            "3 days",
            &dr("2024-01-05", "2024-01-12"),
            "2024-01-10",
            3,
        )
        .unwrap();
        // Buckets: Jan 4, Jan 7, Jan 10
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2024-01-04T00:00:00.000");
        assert_eq!(result[0][1], "2024-01-06T23:59:59.999");
        assert_eq!(result[1][0], "2024-01-07T00:00:00.000");
        assert_eq!(result[2][0], "2024-01-10T00:00:00.000");
    }

    #[test]
    fn custom_calendar_interval_month() {
        let result = QueryTimeSeries::generate_custom(
            "1 month",
            &dr("2024-01-15", "2024-04-01"),
            "2024-01-15",
            3,
        )
        .unwrap();
        // Aligned = 2024-01-15. Steps: Jan 15→Feb 15→Mar 15→Apr 15. While aligned < Apr 1 → 3 buckets.
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2024-01-15T00:00:00.000");
        assert_eq!(result[0][1], "2024-02-14T23:59:59.999");
        assert_eq!(result[1][0], "2024-02-15T00:00:00.000");
        assert_eq!(result[1][1], "2024-03-14T23:59:59.999");
        assert_eq!(result[2][0], "2024-03-15T00:00:00.000");
        // Apr 15 - 1 second = Apr 14 23:59:59
        assert_eq!(result[2][1], "2024-04-14T23:59:59.999");
    }

    #[test]
    fn custom_zero_interval_errors() {
        let err = QueryTimeSeries::generate_custom(
            "0 days",
            &dr("2024-01-01", "2024-01-10"),
            "2024-01-01",
            3,
        )
        .unwrap_err();
        assert!(err.message.contains("can't be zero"));
    }

    #[test]
    fn custom_invalid_interval_errors() {
        let err = QueryTimeSeries::generate_custom(
            "garbage",
            &dr("2024-01-01", "2024-01-10"),
            "2024-01-01",
            3,
        )
        .unwrap_err();
        assert!(!err.message.is_empty());
    }

    #[test]
    fn custom_exceeding_buckets_errors() {
        // One day at a 1-second interval is 86_400 buckets, over the 50k cap.
        let err = QueryTimeSeries::generate_custom(
            "1 second",
            &dr("2024-01-10T00:00:00", "2024-01-11T00:00:00"),
            "2024-01-10T00:00:00",
            3,
        )
        .unwrap_err();
        assert!(err.message.contains("exceeded"));
    }

    #[test]
    fn custom_non_advancing_interval_errors() {
        // Net-negative interval with origin == range start reaches the
        // "did not advance" guard without spinning in alignment.
        let err = QueryTimeSeries::generate_custom(
            "-1 day",
            &dr("2024-01-01", "2024-01-10"),
            "2024-01-01",
            3,
        )
        .unwrap_err();
        assert!(err.message.contains("did not advance"));
    }

    #[test]
    fn custom_alignment_non_convergent_errors() {
        // start < origin with a net-negative interval would step away from
        // start forever in align_to_origin; the cap turns it into an error.
        let err = QueryTimeSeries::generate_custom(
            "-1 day",
            &dr("2024-01-05", "2024-01-10"),
            "2024-01-10",
            3,
        )
        .unwrap_err();
        assert!(err.message.contains("did not converge"));
    }

    #[test]
    fn custom_month_plus_days_interval() {
        // "1 month 15 days" from Jan 1: Feb 16, Mar 31, May 15. Mar 31 + 1 month
        // clamps to Apr 30 before adding 15 days, exercising months+days.
        let result = QueryTimeSeries::generate_custom(
            "1 month 15 days",
            &dr("2024-01-01", "2024-05-01"),
            "2024-01-01",
            3,
        )
        .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2024-02-15T23:59:59.999");
        assert_eq!(result[1][0], "2024-02-16T00:00:00.000");
        assert_eq!(result[1][1], "2024-03-30T23:59:59.999");
        assert_eq!(result[2][0], "2024-03-31T00:00:00.000");
        assert_eq!(result[2][1], "2024-05-14T23:59:59.999");
    }

    #[test]
    fn custom_sub_day_interval() {
        // "2 days 6 hours" = 54h steps from Jan 1 00:00.
        let result = QueryTimeSeries::generate_custom(
            "2 days 6 hours",
            &dr("2024-01-01T00:00:00", "2024-01-06T00:00:00"),
            "2024-01-01T00:00:00",
            3,
        )
        .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(
            result[0],
            vec!["2024-01-01T00:00:00.000", "2024-01-03T05:59:59.999"]
        );
        assert_eq!(result[1][0], "2024-01-03T06:00:00.000");
        assert_eq!(result[2][0], "2024-01-05T12:00:00.000");
    }

    #[test]
    fn custom_quarter_interval() {
        let result = QueryTimeSeries::generate_custom(
            "1 quarter",
            &dr("2024-01-01", "2024-12-31"),
            "2024-01-01",
            3,
        )
        .unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result[0][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[0][1], "2024-03-31T23:59:59.999");
        assert_eq!(result[3][0], "2024-10-01T00:00:00.000");
        assert_eq!(result[3][1], "2024-12-31T23:59:59.999");
    }

    #[test]
    fn custom_year_interval() {
        let result = QueryTimeSeries::generate_custom(
            "1 year",
            &dr("2022-01-01", "2024-06-01"),
            "2022-01-01",
            3,
        )
        .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0][1], "2022-12-31T23:59:59.999");
        assert_eq!(result[2][0], "2024-01-01T00:00:00.000");
        assert_eq!(result[2][1], "2024-12-31T23:59:59.999");
    }

    #[test]
    fn custom_precision_six() {
        let result = QueryTimeSeries::generate_custom(
            "1 day",
            &dr("2024-01-01", "2024-01-02"),
            "2024-01-01",
            6,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            vec!["2024-01-01T00:00:00.000000", "2024-01-01T23:59:59.999999"]
        );
    }
}
