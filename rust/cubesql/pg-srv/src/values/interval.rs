//! Interval value representation for PostgreSQL protocol

use crate::{ProtocolError, ToProtocolValue};
use bytes::{BufMut, BytesMut};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Default)]
pub struct IntervalValue {
    pub months: i32,
    pub days: i32,
    pub hours: i32,
    pub mins: i32,
    pub secs: i32,
    pub usecs: i32,
}

impl IntervalValue {
    pub fn new(months: i32, days: i32, hours: i32, mins: i32, secs: i32, usecs: i32) -> Self {
        Self {
            months,
            days,
            hours,
            mins,
            secs,
            usecs,
        }
    }

    pub fn is_zeroed(&self) -> bool {
        self.months == 0
            && self.days == 0
            && self.hours == 0
            && self.mins == 0
            && self.secs == 0
            && self.usecs == 0
    }

    pub fn extract_years_month(&self) -> (i32, i32) {
        let years = self.months / 12;
        let month = self.months % 12;

        (years, month)
    }

    pub fn as_iso_str(&self) -> String {
        if self.is_zeroed() {
            return "00:00:00".to_owned();
        }

        let mut res = "".to_owned();
        let (years, months) = self.extract_years_month();

        if years != 0 {
            if years == 1 {
                res.push_str(&format!("{:#?} year ", years))
            } else {
                res.push_str(&format!("{:#?} years ", years))
            }
        }

        if months != 0 {
            if months == 1 {
                res.push_str(&format!("{:#?} mon ", months));
            } else {
                res.push_str(&format!("{:#?} mons ", months));
            }
        }

        if self.days != 0 {
            if self.days == 1 {
                res.push_str(&format!("{:#?} day ", self.days));
            } else {
                res.push_str(&format!("{:#?} days ", self.days));
            }
        }

        if self.hours != 0 || self.mins != 0 || self.secs != 0 || self.usecs != 0 {
            if self.hours < 0 || self.mins < 0 || self.secs < 0 || self.usecs < 0 {
                res.push('-')
            };

            res.push_str(&format!(
                "{:02}:{:02}:{:02}",
                self.hours.abs(),
                self.mins.abs(),
                self.secs.abs()
            ));

            if self.usecs != 0 {
                res.push_str(&format!(".{:06}", self.usecs.abs()))
            }
        }

        res.trim().to_string()
    }

    pub fn as_postgresql_str(&self) -> String {
        let (years, months) = self.extract_years_month();

        // We manually format sign for the case where self.secs == 0, self.usecs < 0.
        // We follow assumptions about consistency of hours/mins/secs/usecs signs as in
        // as_iso_str here.
        format!(
            "{} years {} mons {} days {} hours {} mins {}{}.{} secs",
            years,
            months,
            self.days,
            self.hours,
            self.mins,
            if self.secs < 0 || self.usecs < 0 {
                "-"
            } else {
                ""
            },
            self.secs.abs(),
            if self.usecs == 0 {
                "00".to_string()
            } else {
                format!("{:06}", self.usecs.abs())
            }
        )
    }
}

impl Display for IntervalValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // TODO lift formatter higher, to as_postgresql_str
        // https://github.com/postgres/postgres/blob/REL_14_4/src/interfaces/ecpg/pgtypeslib/interval.c#L763
        f.write_str(&self.as_postgresql_str())
    }
}

impl ToProtocolValue for IntervalValue {
    // https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/timestamp.c#L958
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        self.to_string().to_text(buf)
    }

    // https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/timestamp.c#L1005
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let usecs = self.hours as i64 * 60 * 60 * 1_000_000
            + self.mins as i64 * 60 * 1_000_000
            + self.secs as i64 * 1_000_000
            + self.usecs as i64;

        buf.put_i32(16);
        buf.put_i64(usecs);
        buf.put_i32(self.days);
        buf.put_i32(self.months);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolError;

    #[test]
    fn test_interval_to_iso() -> Result<(), ProtocolError> {
        assert_eq!(
            IntervalValue::new(1, 0, 0, 0, 0, 0).as_iso_str(),
            "1 mon".to_string()
        );
        assert_eq!(
            IntervalValue::new(14, 0, 0, 0, 0, 0).as_iso_str(),
            "1 year 2 mons".to_string()
        );
        assert_eq!(
            IntervalValue::new(0, 1, 1, 1, 1, 1).as_iso_str(),
            "1 day 01:01:01.000001".to_string()
        );
        assert_eq!(
            IntervalValue::new(0, 0, -1, -1, -1, -1).as_iso_str(),
            "-01:01:01.000001".to_string()
        );
        assert_eq!(
            IntervalValue::new(0, 0, 0, 0, 0, 0).as_iso_str(),
            "00:00:00".to_string()
        );

        Ok(())
    }

    #[test]
    fn test_interval_to_postgres() -> Result<(), ProtocolError> {
        assert_eq!(
            IntervalValue::new(0, 0, 0, 0, 0, 0).to_string(),
            "0 years 0 mons 0 days 0 hours 0 mins 0.00 secs".to_string()
        );

        assert_eq!(
            IntervalValue::new(0, 0, 0, 0, 1, 23).to_string(),
            "0 years 0 mons 0 days 0 hours 0 mins 1.000023 secs".to_string()
        );

        assert_eq!(
            IntervalValue::new(0, 0, 0, 0, -1, -23).to_string(),
            "0 years 0 mons 0 days 0 hours 0 mins -1.000023 secs".to_string()
        );

        assert_eq!(
            IntervalValue::new(0, 0, 0, 0, -1, 0).to_string(),
            "0 years 0 mons 0 days 0 hours 0 mins -1.00 secs".to_string()
        );

        assert_eq!(
            IntervalValue::new(0, 0, -14, -5, -1, 0).to_string(),
            "0 years 0 mons 0 days -14 hours -5 mins -1.00 secs".to_string()
        );

        Ok(())
    }
}
