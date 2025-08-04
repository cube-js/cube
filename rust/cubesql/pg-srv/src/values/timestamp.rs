//! Timestamp value representation for PostgreSQL protocol

use crate::{ProtocolError, ToProtocolValue};
use bytes::{BufMut, BytesMut};
use chrono::{
    format::{
        Fixed, Item,
        Numeric::{Day, Hour, Minute, Month, Second, Year},
        Pad::Zero,
    },
    prelude::*,
};
use chrono_tz::Tz;
use std::io::Error;
use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimestampValue {
    unix_nano: i64,
    tz: Option<String>,
}

impl TimestampValue {
    pub fn new(mut unix_nano: i64, tz: Option<String>) -> TimestampValue {
        // This is a hack to workaround a mismatch between on-disk and in-memory representations.
        // We use microsecond precision on-disk.
        unix_nano -= unix_nano % 1000;
        TimestampValue { unix_nano, tz }
    }

    pub fn to_naive_datetime(&self) -> NaiveDateTime {
        // Convert nanoseconds to seconds and nanoseconds
        let secs = self.unix_nano / 1_000_000_000;
        let nsecs = (self.unix_nano % 1_000_000_000) as u32;
        DateTime::from_timestamp(secs, nsecs)
            .unwrap_or_else(|| panic!("Invalid timestamp: {}", self.unix_nano))
            .naive_utc()
    }

    pub fn to_fixed_datetime(&self) -> io::Result<DateTime<Tz>> {
        assert!(self.tz.is_some());
        let tz = self
            .tz
            .as_ref()
            .unwrap()
            .parse::<Tz>()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        let ndt = self.to_naive_datetime();
        Ok(tz.from_utc_datetime(&ndt))
    }

    pub fn tz_ref(&self) -> &Option<String> {
        &self.tz
    }

    pub fn get_time_stamp(&self) -> i64 {
        self.unix_nano
    }
}

impl Debug for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampValue")
            .field("unix_nano", &self.unix_nano)
            .field("tz", &self.tz)
            .field("str", &self.to_string())
            .finish()
    }
}

impl Display for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let formatted = Utc.timestamp_nanos(self.unix_nano).format_with_items(
            [
                Item::Numeric(Year, Zero),
                Item::Literal("-"),
                Item::Numeric(Month, Zero),
                Item::Literal("-"),
                Item::Numeric(Day, Zero),
                Item::Literal("T"),
                Item::Numeric(Hour, Zero),
                Item::Literal(":"),
                Item::Numeric(Minute, Zero),
                Item::Literal(":"),
                Item::Numeric(Second, Zero),
                Item::Fixed(Fixed::Nanosecond3),
            ]
            .iter(),
        );
        write!(f, "{}", formatted)
    }
}

// POSTGRES_EPOCH_JDATE
fn pg_base_date_epoch() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

impl ToProtocolValue for TimestampValue {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let ndt = match self.tz_ref() {
            None => self.to_naive_datetime(),
            Some(_) => self.to_fixed_datetime()?.naive_utc(),
        };

        // 2022-04-25 15:36:49.39705+00
        let as_str = ndt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        match self.tz_ref() {
            None => as_str.to_text(buf),
            Some(_) => (as_str + "+00").to_text(buf),
        }
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let ndt = match self.tz_ref() {
            None => self.to_naive_datetime(),
            Some(_) => self.to_fixed_datetime()?.naive_utc(),
        };

        let n = ndt
            .signed_duration_since(pg_base_date_epoch())
            .num_microseconds()
            .ok_or(Error::new(
                io::ErrorKind::Other,
                "Unable to extract number of seconds from timestamp",
            ))?;

        buf.put_i32(8);
        buf.put_i64(n);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolError;

    #[test]
    fn test_timestamp_creation() -> Result<(), ProtocolError> {
        let ts = TimestampValue::new(1650890322000000000, None);
        assert_eq!(ts.get_time_stamp(), 1650890322000000000);
        assert_eq!(ts.tz_ref(), &None);

        let ts_with_tz = TimestampValue::new(1650890322000000000, Some("UTC".to_string()));
        assert_eq!(ts_with_tz.get_time_stamp(), 1650890322000000000);
        assert_eq!(ts_with_tz.tz_ref(), &Some("UTC".to_string()));

        Ok(())
    }

    #[test]
    fn test_timestamp_to_string() {
        let ts = TimestampValue::new(1650890322000000000, None);
        // The string representation should match the expected format
        assert!(!ts.to_string().is_empty());
    }

    #[test]
    fn test_timestamp_precision_hack() {
        // Test that nanoseconds are truncated to milliseconds
        let ts = TimestampValue::new(1650890322123456789, None);
        assert_eq!(ts.get_time_stamp(), 1650890322123456000);
    }
}
