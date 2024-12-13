//! Encoding native values to the Protocol representation

use crate::{protocol::Format, ProtocolError};
use bytes::{BufMut, BytesMut};
#[cfg(feature = "with-chrono")]
use chrono::{NaiveDate, NaiveDateTime};
use std::{
    fmt::{Display, Formatter},
    io::{Error, ErrorKind},
};

/// This trait explains how to encode values to the protocol format
pub trait ToProtocolValue: std::fmt::Debug {
    // Converts raw value to native type in specific format
    fn to_protocol(&self, buf: &mut BytesMut, format: Format) -> Result<(), ProtocolError>
    where
        Self: Sized,
    {
        match format {
            Format::Text => self.to_text(buf),
            Format::Binary => self.to_binary(buf),
        }
    }

    /// Converts native type to raw value in text format
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>
    where
        Self: Sized;

    /// Converts native type to raw value in binary format
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>
    where
        Self: Sized;
}

impl ToProtocolValue for String {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_i32(self.len() as i32);
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_i32(self.len() as i32);
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }
}

impl ToProtocolValue for bool {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        if *self {
            "t".to_string().to_text(buf)
        } else {
            "f".to_string().to_text(buf)
        }
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_i32(1_i32);
        buf.extend_from_slice(if *self { &[1] } else { &[0] });

        Ok(())
    }
}

impl<T: ToProtocolValue> ToProtocolValue for Option<T> {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        match &self {
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(v) => v.to_text(buf)?,
        };

        Ok(())
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        match &self {
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(v) => v.to_binary(buf)?,
        };

        Ok(())
    }
}

macro_rules! impl_primitive {
    ($type: ident) => {
        impl ToProtocolValue for $type {
            fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
                self.to_string().to_text(buf)
            }

            fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
                buf.extend_from_slice(&(std::mem::size_of::<$type>() as u32).to_be_bytes());
                buf.extend_from_slice(&self.to_be_bytes());

                Ok(())
            }
        }
    };
}

impl_primitive!(i8);
impl_primitive!(i16);
impl_primitive!(i32);
impl_primitive!(i64);
impl_primitive!(f32);
impl_primitive!(f64);

// POSTGRES_EPOCH_JDATE
#[cfg(feature = "with-chrono")]
fn pg_base_date_epoch() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

#[cfg(feature = "with-chrono")]
impl ToProtocolValue for NaiveDate {
    // date_out - https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/date.c#L176
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        self.to_string().to_text(buf)
    }

    // date_send - https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/date.c#L223
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let n = self
            .signed_duration_since(pg_base_date_epoch().date())
            .num_days();
        if n > (i32::MAX as i64) {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "value too large to store in the binary format (i32), actual: {}",
                    n
                ),
            )
            .into());
        }

        buf.put_i32(4);
        buf.put_i32(n as i32);

        Ok(())
    }
}

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
        let years = (self.months as f64 / 12_f64).floor();
        let month = self.months as f64 - (years * 12_f64);

        (years as i32, month as i32)
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
    use crate::*;
    use bytes::BytesMut;

    fn assert_text_encode<T: ToProtocolValue>(value: T, expected: &[u8]) {
        let mut buf = BytesMut::new();
        value.to_text(&mut buf).unwrap();

        assert_eq!(buf.as_ref(), expected);
    }

    #[test]
    fn test_text_encoders() -> Result<(), ProtocolError> {
        assert_text_encode(true, &[0, 0, 0, 1, 116]);
        assert_text_encode(false, &[0, 0, 0, 1, 102]);
        assert_text_encode("str".to_string(), &[0, 0, 0, 3, 115, 116, 114]);

        Ok(())
    }

    fn assert_bind_encode<T: ToProtocolValue>(value: T, expected: &[u8]) {
        let mut buf = BytesMut::new();
        value.to_binary(&mut buf).unwrap();

        assert_eq!(buf.as_ref(), expected);
    }

    #[test]
    fn test_binary_encoders() -> Result<(), ProtocolError> {
        assert_bind_encode(true, &[0, 0, 0, 1, 1]);
        assert_bind_encode(false, &[0, 0, 0, 1, 0]);

        Ok(())
    }

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
            "01:01:01.000001".to_string()
        );
        assert_eq!(
            IntervalValue::new(0, 0, -1, 1, 1, 1).as_iso_str(),
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
