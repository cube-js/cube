use cubenativeutils::CubeError;
use itertools::Itertools;
use std::ops::{Add, AddAssign, Neg, Sub};
use std::str::FromStr;

#[derive(Debug, PartialEq, Clone, Hash, Eq)]
pub struct SqlInterval {
    pub year: i32,
    pub month: i32,
    pub week: i32,
    pub day: i32,
    pub hour: i32,
    pub minute: i32,
    pub second: i32,
}

impl SqlInterval {
    pub fn new(
        year: i32,
        month: i32,
        week: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: i32,
    ) -> Self {
        Self {
            year,
            month,
            week,
            day,
            hour,
            minute,
            second,
        }
    }

    pub fn min_granularity(&self) -> Result<String, CubeError> {
        let res = if self.second != 0 {
            "second"
        } else if self.minute != 0 {
            "minute"
        } else if self.hour != 0 {
            "hour"
        } else if self.day != 0 {
            "day"
        } else if self.week != 0 {
            "week"
        } else if self.month != 0 {
            "month"
        } else if self.year != 0 {
            "year"
        } else {
            return Err(CubeError::internal(format!(
                "Attempt to get granularity from empty SqlInterval"
            )));
        };
        Ok(res.to_string())
    }

    pub fn to_sql(&self) -> String {
        let mut res = vec![];
        if self.year != 0 {
            res.push(format!("{} YEAR", self.year));
        }
        if self.month != 0 {
            res.push(format!("{} MONTH", self.month));
        }
        if self.week != 0 {
            res.push(format!("{} WEEK", self.week));
        }
        if self.day != 0 {
            res.push(format!("{} DAY", self.day));
        }
        if self.hour != 0 {
            res.push(format!("{} HOUR", self.hour));
        }
        if self.minute != 0 {
            res.push(format!("{} MINUTE", self.minute));
        }
        if self.second != 0 {
            res.push(format!("{} SECOND", self.second));
        }
        res.join(" ")
    }

    pub fn inverse(&self) -> Self {
        Self::new(
            -self.year,
            -self.month,
            -self.week,
            -self.day,
            -self.hour,
            -self.minute,
            -self.second,
        )
    }
}

impl Add for SqlInterval {
    type Output = SqlInterval;
    fn add(self, other: SqlInterval) -> SqlInterval {
        SqlInterval::new(
            self.year + other.year,
            self.month + other.month,
            self.week + other.week,
            self.day + other.day,
            self.hour + other.hour,
            self.minute + other.minute,
            self.second + other.second,
        )
    }
}

impl AddAssign for SqlInterval {
    fn add_assign(&mut self, other: SqlInterval) {
        self.year += other.year;
        self.month += other.month;
        self.week += other.week;
        self.day += other.day;
        self.hour += other.hour;
        self.minute += other.minute;
        self.second += other.second;
    }
}

impl Sub for SqlInterval {
    type Output = SqlInterval;
    fn sub(self, other: SqlInterval) -> SqlInterval {
        SqlInterval::new(
            self.year - other.year,
            self.month - other.month,
            self.week - other.week,
            self.day - other.day,
            self.hour - other.hour,
            self.minute - other.minute,
            self.second - other.second,
        )
    }
}

impl Neg for SqlInterval {
    type Output = SqlInterval;
    fn neg(self) -> SqlInterval {
        SqlInterval::new(
            -self.year,
            -self.month,
            -self.week,
            -self.day,
            -self.hour,
            -self.minute,
            -self.second,
        )
    }
}

impl Default for SqlInterval {
    fn default() -> Self {
        Self {
            second: 0,
            minute: 0,
            hour: 0,
            day: 0,
            week: 0,
            month: 0,
            year: 0,
        }
    }
}

impl FromStr for SqlInterval {
    type Err = CubeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut result = SqlInterval::default();
        for (value, unit) in s.split_whitespace().tuples() {
            let value = value
                .parse::<i32>()
                .map_err(|_| CubeError::user(format!("Invalid interval value: {}", value)))?;
            match unit {
                "second" | "seconds" => result.second = value,
                "minute" | "minutes" => result.minute = value,
                "hour" | "hours" => result.hour = value,
                "day" | "days" => result.day = value,
                "week" | "weeks" => result.week = value,
                "month" | "months" => result.month = value,
                "year" | "years" => result.year = value,
                other => return Err(CubeError::user(format!("Invalid interval unit: {}", other))),
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(
            SqlInterval::from_str("1 second").unwrap(),
            SqlInterval::new(0, 0, 0, 0, 0, 0, 1)
        );

        assert_eq!(
            SqlInterval::from_str("1 year 3 months 4 weeks 2 day 4 hours 2 minutes 1 second")
                .unwrap(),
            SqlInterval::new(1, 3, 4, 2, 4, 2, 1)
        );
    }
    #[test]
    fn test_arithmetic() {
        assert_eq!(
            SqlInterval::new(1, 3, 4, 2, 4, 2, 1) + SqlInterval::new(1, 3, 4, 2, 4, 2, 1),
            SqlInterval::new(2, 6, 8, 4, 8, 4, 2)
        );
        assert_eq!(
            SqlInterval::new(1, 3, 4, 2, 4, 2, 1) - SqlInterval::new(1, 4, 4, 2, 2, 2, 1),
            SqlInterval::new(0, -1, 0, 0, 2, 0, 0)
        );
        assert_eq!(
            -SqlInterval::new(1, 3, -4, 2, 4, 2, 1),
            SqlInterval::new(-1, -3, 4, -2, -4, -2, -1)
        );
    }
}
