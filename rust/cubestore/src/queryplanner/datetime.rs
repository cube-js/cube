use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;

pub fn date_add(mut t: DateTime<Utc>, i: ScalarValue) -> Result<DateTime<Utc>, DataFusionError> {
    match i {
        ScalarValue::IntervalYearMonth(Some(v)) => {
            if v < 0 {
                return Err(DataFusionError::Plan(
                    "Second argument of `DATE_ADD` must be a positive".to_string(),
                ));
            }

            let years_to_add = v / 12;
            let months_to_add = (v % 12) as u32;

            let mut year = t.year() + years_to_add;
            let mut month = t.month();
            let mut day = t.day();

            if month + months_to_add > 12 {
                year += 1;
                month = (month + months_to_add) - 12;
            } else {
                month += months_to_add;
            }

            assert!(month <= 12);

            let days_in_month = last_day_of_month(year, month);

            if day > days_in_month {
                day = days_in_month;
            }

            t = datetime_safety_unwrap(t.with_day(1))?;

            // @todo Optimize? Chrono is using string -> parsing and applying it back to obj
            t = datetime_safety_unwrap(t.with_month(month))?;
            t = datetime_safety_unwrap(t.with_year(year))?;
            t = datetime_safety_unwrap(t.with_day(day))?;
            return Ok(t);
        }
        ScalarValue::IntervalDayTime(Some(v)) => {
            if v < 0 {
                return Err(DataFusionError::Plan(
                    "Second argument of `DATE_ADD` must be positive".to_string(),
                ));
            }

            let days_parts: i64 = (((v as u64) & 0xFFFFFFFF00000000) >> 32) as i64;
            let milliseconds_part: i64 = ((v as u64) & 0xFFFFFFFF) as i64;

            t = t + Duration::days(days_parts);
            t = t + Duration::milliseconds(milliseconds_part);
            return Ok(t);
        }
        _ => {
            return Err(DataFusionError::Plan(
                "Second argument of `DATE_ADD` must be a non-null interval".to_string(),
            ));
        }
    }
}

fn datetime_safety_unwrap(opt: Option<DateTime<Utc>>) -> Result<DateTime<Utc>, DataFusionError> {
    if opt.is_some() {
        return Ok(opt.unwrap());
    }

    return Err(DataFusionError::Internal(
        "Unable to calculate operation between timestamp and interval".to_string(),
    ));
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    NaiveDate::from_ymd_opt(year, month + 1, 1)
        .unwrap_or(NaiveDate::from_ymd(year + 1, 1, 1))
        .pred()
        .day()
}
