use super::{QueryDateTimeHelper, SqlInterval};
use chrono::prelude::*;
use chrono::Duration;
use chrono_tz::Tz;
use cubenativeutils::CubeError;
use std::cmp::Ord;

#[derive(Clone, PartialEq, PartialOrd, Eq, Ord, Debug)]
pub struct QueryDateTime {
    date_time: DateTime<Tz>,
}

impl ToString for QueryDateTime {
    fn to_string(&self) -> String {
        self.default_format()
    }
}

impl QueryDateTime {
    pub fn new(date_time: DateTime<Tz>) -> QueryDateTime {
        QueryDateTime { date_time }
    }
    pub fn now(tz: Tz) -> Result<QueryDateTime, CubeError> {
        let local = Local::now().naive_local();
        Self::from_local_date_time(tz, local)
    }
    pub fn from_date_str(tz: Tz, date: &str) -> Result<Self, CubeError> {
        let local_dt = QueryDateTimeHelper::parse_native_date_time(date)?;
        Self::from_local_date_time(tz, local_dt)
    }

    pub fn from_local_date_time(tz: Tz, date: NaiveDateTime) -> Result<Self, CubeError> {
        let date_time =
            QueryDateTimeHelper::resolve_local_result(&tz, &date, tz.from_local_datetime(&date))?;
        Ok(Self { date_time })
    }

    pub fn start_of_year(&self) -> Self {
        let tz = self.date_time.timezone();
        Self::new(
            tz.with_ymd_and_hms(self.date_time.year(), 1, 1, 0, 0, 0)
                .unwrap(),
        )
    }

    pub fn date_time(&self) -> DateTime<Tz> {
        self.date_time
    }

    pub fn naive_local(&self) -> NaiveDateTime {
        self.date_time.naive_local()
    }

    pub fn naive_utc(&self) -> NaiveDateTime {
        self.date_time.naive_utc()
    }

    pub fn format(&self, format: &str) -> String {
        self.date_time.format(format).to_string()
    }

    pub fn default_format(&self) -> String {
        self.date_time.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
    }

    pub fn add_interval(&self, interval: &SqlInterval) -> Result<Self, CubeError> {
        let date = self.naive_local().date();

        // Step 1: add years and months with fallback logic
        let mut year = date.year() + interval.year;
        let mut month = date.month() as i32 + interval.month;

        while month > 12 {
            year += 1;
            month -= 12;
        }
        while month < 1 {
            year -= 1;
            month += 12;
        }

        let day = date.day();
        // Adjust for overflowed day in shorter months (e.g. Feb 30 → Feb 28)
        let adjusted_date = NaiveDate::from_ymd_opt(year, month as u32, day)
            .or_else(|| {
                (1..=31)
                    .rev()
                    .find_map(|d| NaiveDate::from_ymd_opt(year, month as u32, d))
            })
            .ok_or_else(|| {
                CubeError::internal(format!(
                    "Failed to compute valid date while adding interval {:?} to date {}",
                    interval, self.date_time
                ))
            })?;

        // Step 2: Add weeks and days
        let adjusted_date =
            adjusted_date + Duration::days(interval.week as i64 * 7 + interval.day as i64);

        // Step 3: Recombine with original time
        let time = self.naive_local().time();
        let mut naive = NaiveDateTime::new(adjusted_date, time);

        // Step 4: Add time-based parts
        naive = naive
            + Duration::hours(interval.hour as i64)
            + Duration::minutes(interval.minute as i64)
            + Duration::seconds(interval.second as i64);

        Self::from_local_date_time(self.date_time.timezone(), naive)
    }

    pub fn sub_interval(&self, interval: &SqlInterval) -> Result<Self, CubeError> {
        self.add_interval(&interval.inverse())
    }

    pub fn add_duration(&self, duration: Duration) -> Result<Self, CubeError> {
        let mut native = self.naive_local();
        native = native + duration;
        Self::from_local_date_time(self.date_time.timezone(), native)
    }

    pub fn granularity(&self) -> String {
        let time = self.date_time.time();

        let weekday = self.date_time.weekday();
        let is_zero_time = |t: chrono::NaiveTime| {
            t.hour() == 0 && t.minute() == 0 && t.second() == 0 && t.nanosecond() == 0
        };

        if self.date_time.month() == 1 && self.date_time.day() == 1 && is_zero_time(time) {
            "year".to_string()
        } else if self.date_time.day() == 1 && is_zero_time(time) {
            "month".to_string()
        } else if weekday == Weekday::Mon && is_zero_time(time) {
            "week".to_string()
        } else if is_zero_time(time) {
            "day".to_string()
        } else if time.minute() == 0 && time.second() == 0 && time.nanosecond() == 0 {
            "hour".to_string()
        } else if time.second() == 0 && time.nanosecond() == 0 {
            "minute".to_string()
        } else if time.nanosecond() == 0 {
            "second".to_string()
        } else {
            "second".to_string()
        }
    }

    pub fn align_to_origin(
        &self,
        origin: &Self,
        interval: &SqlInterval,
    ) -> Result<Self, CubeError> {
        let mut aligned = self.clone();
        let mut offset = origin.clone();

        if self < origin {
            while &offset > self {
                offset = offset.sub_interval(interval)?;
            }
            aligned = offset;
        } else {
            while &offset < self {
                aligned = offset.clone();
                offset = offset.add_interval(interval)?;
            }

            if &offset == self {
                aligned = offset;
            }
        }

        Ok(aligned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_date_time() {
        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();
        let parsed = QueryDateTime::from_date_str(tz, "2021-01-01").unwrap();
        assert_eq!(
            parsed.naive_utc(),
            NaiveDate::from_ymd_opt(2020, 12, 31)
                .unwrap()
                .and_hms_opt(21, 0, 0)
                .unwrap()
        );

        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();
        let parsed = QueryDateTime::from_date_str(tz, "2021-01-01 03:15:20").unwrap();

        assert_eq!(
            parsed.naive_utc(),
            NaiveDate::from_ymd_opt(2021, 1, 1)
                .unwrap()
                .and_hms_opt(0, 15, 20)
                .unwrap()
        );

        //Ambiguous time
        let tz = "America/New_York".parse::<Tz>().unwrap();
        let parsed = QueryDateTime::from_date_str(tz, "2024-11-03 01:30:00").unwrap();
        assert_eq!(
            parsed.date_time().naive_utc(),
            NaiveDate::from_ymd_opt(2024, 11, 3)
                .unwrap()
                .and_hms_opt(5, 30, 0)
                .unwrap()
        );
        //Not exist time
        let tz = "America/New_York".parse::<Tz>().unwrap();
        let parsed = QueryDateTime::from_date_str(tz, "2024-03-10 02:30:00").unwrap();
        assert_eq!(
            parsed.date_time().naive_utc(),
            NaiveDate::from_ymd_opt(2024, 3, 10)
                .unwrap()
                .and_hms_opt(7, 0, 0)
                .unwrap()
        );
    }
    #[test]
    fn test_start_of_year() {
        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();
        let date = QueryDateTime::from_date_str(tz, "2024-11-03 01:30:00").unwrap();
        let start = date.start_of_year();
        assert_eq!(
            start.date_time().naive_utc(),
            NaiveDate::from_ymd_opt(2023, 12, 31)
                .unwrap()
                .and_hms_opt(21, 0, 0)
                .unwrap()
        );
    }
    #[test]
    fn test_add_interval() {
        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();

        let date = QueryDateTime::from_date_str(tz, "2024-11-03 01:30:00").unwrap();
        let interval = "4 hours 2 minutes 10 second"
            .parse::<SqlInterval>()
            .unwrap();
        let result = date.add_interval(&interval).unwrap().naive_utc();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2024, 11, 3)
                .unwrap()
                .and_hms_opt(2, 32, 10)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-11-03 01:30:00").unwrap();
        let interval = "2 hours -2 minutes 10 second"
            .parse::<SqlInterval>()
            .unwrap();
        let result = date.add_interval(&interval).unwrap().naive_utc();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2024, 11, 3)
                .unwrap()
                .and_hms_opt(0, 28, 10)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-11-03 4:30:00").unwrap();
        let interval = "-4 hours -31 minutes 10 second"
            .parse::<SqlInterval>()
            .unwrap();
        let result = date.add_interval(&interval).unwrap().naive_utc();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2024, 11, 2)
                .unwrap()
                .and_hms_opt(20, 59, 10)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-02-03 01:30:00").unwrap();
        let interval = "1 week".parse::<SqlInterval>().unwrap();
        let result = date.add_interval(&interval).unwrap().naive_local();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2024, 2, 10)
                .unwrap()
                .and_hms_opt(1, 30, 0)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-02-03 01:30:00").unwrap();
        let interval = "1 month 1 week".parse::<SqlInterval>().unwrap();
        let result = date.add_interval(&interval).unwrap().naive_local();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2024, 3, 10)
                .unwrap()
                .and_hms_opt(1, 30, 0)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-02-03 01:30:00").unwrap();
        let interval = "1 year 1 month 1 week 3 minute"
            .parse::<SqlInterval>()
            .unwrap();
        let result = date.add_interval(&interval).unwrap().naive_local();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2025, 3, 10)
                .unwrap()
                .and_hms_opt(1, 33, 0)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-02-03 01:30:00").unwrap();
        let interval = "11 month 1 week 3 minute".parse::<SqlInterval>().unwrap();
        let result = date.add_interval(&interval).unwrap().naive_local();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2025, 1, 10)
                .unwrap()
                .and_hms_opt(1, 33, 0)
                .unwrap()
        );
    }
    #[test]
    fn test_add_interval_age_cases() {
        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();

        let date = QueryDateTime::from_date_str(tz, "2024-01-31 01:30:00").unwrap();
        let interval = "1 month 3 minute".parse::<SqlInterval>().unwrap();
        let result = date.add_interval(&interval).unwrap().naive_local();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2024, 2, 29)
                .unwrap()
                .and_hms_opt(1, 33, 0)
                .unwrap()
        );

        let date = QueryDateTime::from_date_str(tz, "2024-02-29 01:30:00").unwrap();
        let interval = "1 year 3 minute".parse::<SqlInterval>().unwrap();
        let result = date.add_interval(&interval).unwrap().naive_local();
        assert_eq!(
            result,
            NaiveDate::from_ymd_opt(2025, 2, 28)
                .unwrap()
                .and_hms_opt(1, 33, 0)
                .unwrap()
        );
    }
    #[test]
    fn test_align_to_origin() {
        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();
        let date = QueryDateTime::from_date_str(tz, "2024-01-31").unwrap();
        let interval = "1 day".parse::<SqlInterval>().unwrap();
        let origin = date.start_of_year();
        let result = date.align_to_origin(&origin, &interval).unwrap();
        assert_eq!(
            result.naive_local(),
            NaiveDate::from_ymd_opt(2024, 1, 31)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );

        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();
        let date = QueryDateTime::from_date_str(tz, "2024-01-31").unwrap();
        let interval = "2 day".parse::<SqlInterval>().unwrap();
        let origin = QueryDateTime::from_date_str(tz, "2024-01-30").unwrap();
        let result = date.align_to_origin(&origin, &interval).unwrap();
        assert_eq!(
            result.naive_local(),
            NaiveDate::from_ymd_opt(2024, 1, 30)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
        let tz = "Etc/GMT-3".parse::<Tz>().unwrap();
        let date = QueryDateTime::from_date_str(tz, "2024-01-31").unwrap();
        let interval = "2 month".parse::<SqlInterval>().unwrap();
        let origin = QueryDateTime::from_date_str(tz, "2024-05-15").unwrap();
        let result = date.align_to_origin(&origin, &interval).unwrap();
        assert_eq!(
            result.naive_local(),
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
        );
    }
}
