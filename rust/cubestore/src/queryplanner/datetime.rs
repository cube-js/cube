use arrow::array::{Array, TimestampNanosecondArray, TimestampNanosecondBuilder};
use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;

pub fn date_addsub_array(
    t: &TimestampNanosecondArray,
    i: ScalarValue,
    is_add: bool,
) -> Result<TimestampNanosecondArray, DataFusionError> {
    let mut result = TimestampNanosecondBuilder::new(t.len());
    match i {
        ScalarValue::IntervalYearMonth(Some(v)) => {
            for i in 0..t.len() {
                if t.is_null(i) {
                    result.append_null()?;
                } else {
                    let t = Utc.timestamp_nanos(t.value(i));
                    result.append_value(date_addsub_year_month(t, v, is_add)?.timestamp_nanos())?;
                }
            }
        }
        ScalarValue::IntervalDayTime(Some(v)) => {
            for i in 0..t.len() {
                if t.is_null(i) {
                    result.append_null()?;
                } else {
                    let t = Utc.timestamp_nanos(t.value(i));
                    result.append_value(date_addsub_day_time(t, v, is_add)?.timestamp_nanos())?;
                }
            }
        }
        _ => {
            let name = match is_add {
                true => "DATE_ADD",
                false => "DATE_SUB",
            };
            return Err(DataFusionError::Plan(format!(
                "Second argument of `{}` must be a non-null interval",
                name
            )));
        }
    }

    Ok(result.finish())
}

pub fn date_addsub_scalar(
    t: DateTime<Utc>,
    i: ScalarValue,
    is_add: bool,
) -> Result<DateTime<Utc>, DataFusionError> {
    match i {
        ScalarValue::IntervalYearMonth(Some(v)) => date_addsub_year_month(t, v, is_add),
        ScalarValue::IntervalDayTime(Some(v)) => date_addsub_day_time(t, v, is_add),
        _ => {
            let name = match is_add {
                true => "DATE_ADD",
                false => "DATE_SUB",
            };
            return Err(DataFusionError::Plan(format!(
                "Second argument of `{}` must be a non-null interval",
                name
            )));
        }
    }
}

fn date_addsub_year_month(
    t: DateTime<Utc>,
    i: i32,
    is_add: bool,
) -> Result<DateTime<Utc>, DataFusionError> {
    let i = match is_add {
        true => i,
        false => -i,
    };

    let mut year = t.year();
    // Note month is numbered 0..11 in this function.
    let mut month = t.month() as i32 - 1;

    year += i / 12;
    month += i % 12;

    if month < 0 {
        year -= 1;
        month += 12;
    }
    debug_assert!(0 <= month);
    year += month / 12;
    month = month % 12;

    match change_ym(t, year, 1 + month as u32) {
        Some(t) => return Ok(t),
        None => {
            return Err(DataFusionError::Execution(format!(
                "Failed to set date to ({}-{})",
                year,
                1 + month
            )))
        }
    };
}

fn date_addsub_day_time(
    t: DateTime<Utc>,
    interval: i64,
    is_add: bool,
) -> Result<DateTime<Utc>, DataFusionError> {
    let i = match is_add {
        true => interval,
        false => -interval,
    };

    let days: i64 = i.signum() * (i.abs() >> 32);
    let millis: i64 = i.signum() * ((i.abs() << 32) >> 32);
    return Ok(t + Duration::days(days) + Duration::milliseconds(millis));
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
