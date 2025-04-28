use chrono::{DateTime, Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use cubenativeutils::CubeError;
pub struct QueryDateTimeHelper {}

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
