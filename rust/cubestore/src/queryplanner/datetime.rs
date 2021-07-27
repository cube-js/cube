use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;

pub fn date_addsub(
    t: DateTime<Utc>,
    i: ScalarValue,
    is_add: bool,
) -> Result<DateTime<Utc>, DataFusionError> {
    match i {
        ScalarValue::IntervalYearMonth(Some(v)) => {
            let v = match is_add {
                true => v,
                false => -v,
            };

            let mut year = t.year();
            // Note month is numbered 0..11 in this function.
            let mut month = t.month() as i32 - 1;

            year += v / 12;
            month += v % 12;

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
        ScalarValue::IntervalDayTime(Some(v)) => {
            let v = match is_add {
                true => v,
                false => -v,
            };

            let days: i64 = v.signum() * (v.abs() >> 32);
            let millis: i64 = v.signum() * ((v.abs() << 32) >> 32);
            return Ok(t + Duration::days(days) + Duration::milliseconds(millis));
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
