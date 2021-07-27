use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;

pub fn date_add(t: DateTime<Utc>, i: ScalarValue) -> Result<DateTime<Utc>, DataFusionError> {
    match i {
        ScalarValue::IntervalYearMonth(Some(v)) => {
            if v < 0 {
                return Err(DataFusionError::Plan(
                    "Second argument of `DATE_ADD` must be a positive".to_string(),
                ));
            }

            let mut year = t.year();
            let mut month = t.month();

            year += v / 12;
            month += (v % 12) as u32;
            if 12 < month {
                year += 1;
                month -= 12;
            }
            assert!(month <= 12);

            match change_ym(t, year, month) {
                Some(t) => return Ok(t),
                None => {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to set date to ({}-{})",
                        year, month
                    )))
                }
            };
        }
        ScalarValue::IntervalDayTime(Some(v)) => {
            if v < 0 {
                return Err(DataFusionError::Plan(
                    "Second argument of `DATE_ADD` must be positive".to_string(),
                ));
            }
            let days: i64 = v >> 32;
            let millis: i64 = v & 0xFFFFFFFF;
            return Ok(t + Duration::days(days) + Duration::milliseconds(millis));
        }
        _ => {
            return Err(DataFusionError::Plan(
                "Second argument of `DATE_ADD` must be a non-null interval".to_string(),
            ));
        }
    }
}

fn change_ym(t: DateTime<Utc>, y: i32, m: u32) -> Option<DateTime<Utc>> {
    debug_assert!(1 <= m && m <= 12);
    let mut d = t.day();
    d = d.min(last_day_of_month(y, m));
    t.with_day(1)?.with_year(y)?.with_month(m)?.with_day(d)
}

fn last_day_of_month(y: i32, m: u32) -> u32 {
    debug_assert!(1 <= m && m <= 12);
    if m == 12 {
        return 31;
    }
    NaiveDate::from_ymd(y, m + 1, 1).pred().day()
}
