//! Timestamp value representation for PostgreSQL protocol

use crate::{
    protocol::{ErrorCode, ErrorResponse},
    FromProtocolValue, ProtocolError, ToProtocolValue,
};
use byteorder::{BigEndian, ByteOrder};
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
use std::backtrace::Backtrace;
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
// https://github.com/postgres/postgres/blob/REL_14_4/src/include/datatype/timestamp.h#L163
pub(crate) fn pg_base_date_epoch() -> NaiveDateTime {
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

    // https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/timestamp.c#L267
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

impl FromProtocolValue for TimestampValue {
    fn from_text(raw: &[u8]) -> Result<Self, ProtocolError> {
        let as_str = std::str::from_utf8(raw).map_err(|err| ProtocolError::ErrorResponse {
            source: ErrorResponse::error(ErrorCode::ProtocolViolation, err.to_string()),
            backtrace: Backtrace::capture(),
        })?;

        // Parse timestamp string in format "YYYY-MM-DD HH:MM:SS[.fff]", but PostgreSQL supports
        // more formats, so let's align this with parse_date_str function from cubesql crate.
        let parsed_datetime = NaiveDateTime::parse_from_str(as_str, "%Y-%m-%d %H:%M:%S")
            .or_else(|_| NaiveDateTime::parse_from_str(as_str, "%Y-%m-%d %H:%M:%S%.f"))
            .or_else(|_| NaiveDateTime::parse_from_str(as_str, "%Y-%m-%d %H:%M:%S%.f UTC"))
            .or_else(|_| NaiveDateTime::parse_from_str(as_str, "%Y-%m-%dT%H:%M:%S"))
            .or_else(|_| NaiveDateTime::parse_from_str(as_str, "%Y-%m-%dT%H:%M:%S%.f"))
            .or_else(|_| NaiveDateTime::parse_from_str(as_str, "%Y-%m-%dT%H:%M:%S%.fZ"))
            .or_else(|_| {
                NaiveDate::parse_from_str(as_str, "%Y-%m-%d").map(|date| {
                    date.and_hms_opt(0, 0, 0)
                        .expect("Unable to set time to 00:00:00")
                })
            })
            .map_err(|err| ProtocolError::ErrorResponse {
                source: ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!(
                        "Unable to parse timestamp from text: '{}', error: {}",
                        as_str, err
                    ),
                ),
                backtrace: Backtrace::capture(),
            })?;

        // Convert to Unix nanoseconds
        let unix_nano = parsed_datetime
            .and_utc()
            .timestamp_nanos_opt()
            .ok_or_else(|| ProtocolError::ErrorResponse {
                source: ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!("Timestamp out of range: '{}'", as_str),
                ),
                backtrace: Backtrace::capture(),
            })?;

        Ok(TimestampValue::new(unix_nano, None))
    }

    // https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/timestamp.c#L234
    fn from_binary(raw: &[u8]) -> Result<Self, ProtocolError> {
        if raw.len() != 8 {
            return Err(ProtocolError::ErrorResponse {
                source: ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!(
                        "Invalid binary timestamp length: expected 8 bytes, got {}",
                        raw.len()
                    ),
                ),
                backtrace: Backtrace::capture(),
            });
        }

        let pg_microseconds = BigEndian::read_i64(raw);

        // Convert PostgreSQL microseconds to Unix nanoseconds
        let unix_nano = pg_base_date_epoch()
            .and_utc()
            .timestamp_nanos_opt()
            .expect("Unable to get timestamp nanos for pg_base_date_epoch")
            + (pg_microseconds * 1_000);

        Ok(TimestampValue::new(unix_nano, None))
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

    #[test]
    fn test_invalid_timestamp_text() {
        // Test that invalid text formats return errors
        assert!(TimestampValue::from_text(b"invalid-date").is_err());
        assert!(TimestampValue::from_text(b"2025-13-45 25:70:99").is_err());
        assert!(TimestampValue::from_text(b"").is_err());
    }

    #[test]
    fn test_timestamp_from_text_various_formats() {
        // Test basic format without fractional seconds
        let ts1 = TimestampValue::from_text(b"2025-08-04 20:15:47").unwrap();
        assert_eq!(ts1.to_naive_datetime().to_string(), "2025-08-04 20:15:47");

        // Test PostgreSQL format with 6-digit fractional seconds
        let ts2 = TimestampValue::from_text(b"2025-08-04 20:16:54.853660").unwrap();
        assert_eq!(
            ts2.to_naive_datetime()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string(),
            "2025-08-04 20:16:54.853660"
        );

        // Test format with 3 fractional seconds
        let ts3 = TimestampValue::from_text(b"2025-08-04 20:15:47.953").unwrap();
        assert_eq!(
            ts3.to_naive_datetime()
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            "2025-08-04 20:15:47.953"
        );

        // Test ISO format with T separator
        let ts4 = TimestampValue::from_text(b"2025-08-04T20:15:47").unwrap();
        assert_eq!(ts4.to_naive_datetime().to_string(), "2025-08-04 20:15:47");

        // Test ISO format with T separator and fractional seconds
        let ts5 = TimestampValue::from_text(b"2025-08-04T20:15:47.953116").unwrap();
        assert_eq!(
            ts5.to_naive_datetime()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string(),
            "2025-08-04 20:15:47.953116"
        );
    }

    #[test]
    fn test_invalid_timestamp_binary() {
        // Test that invalid binary data returns errors
        assert!(TimestampValue::from_binary(&[1, 2, 3]).is_err()); // Wrong length
        assert!(TimestampValue::from_binary(&[]).is_err()); // Empty
    }
}
