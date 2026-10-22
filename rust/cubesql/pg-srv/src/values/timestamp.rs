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
            .map_err(|err| io::Error::other(err.to_string()))?;
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

/// Longest output of [`format_fixed`]: `YYYY-MM-DD HH:MM:SS.ffffff`.
const FIXED_MAX_LEN: usize = 26;

#[inline]
fn write_2(out: &mut [u8], pos: usize, v: u32) {
    out[pos] = b'0' + (v / 10) as u8;
    out[pos + 1] = b'0' + (v % 10) as u8;
}

/// Renders `YYYY-MM-DD<sep>HH:MM:SS.<frac_digits>` without going through chrono's format item
/// interpreter, which dominated text encoding of timestamp columns. Returns `None` for values the
/// fixed-width form cannot express (years outside 0..=9999 need a sign or a fifth digit, leap
/// seconds), so callers fall back to chrono.
fn format_fixed(
    ndt: &NaiveDateTime,
    date_time_sep: u8,
    frac_digits: u32,
    out: &mut [u8; FIXED_MAX_LEN],
) -> Option<usize> {
    debug_assert!((1..=6).contains(&frac_digits));

    let year = ndt.year();
    let nanosecond = ndt.nanosecond();
    if !(0..=9999).contains(&year) || nanosecond >= 1_000_000_000 {
        return None;
    }

    let year = year as u32;
    write_2(out, 0, year / 100);
    write_2(out, 2, year % 100);
    out[4] = b'-';
    write_2(out, 5, ndt.month());
    out[7] = b'-';
    write_2(out, 8, ndt.day());
    out[10] = date_time_sep;
    write_2(out, 11, ndt.hour());
    out[13] = b':';
    write_2(out, 14, ndt.minute());
    out[16] = b':';
    write_2(out, 17, ndt.second());
    out[19] = b'.';

    // Truncates like chrono's `%.6f` / `Nanosecond3`.
    let mut frac = nanosecond / 10u32.pow(9 - frac_digits);
    let len = 20 + frac_digits as usize;
    for pos in (20..len).rev() {
        out[pos] = b'0' + (frac % 10) as u8;
        frac /= 10;
    }

    Some(len)
}

impl Display for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let dt = Utc.timestamp_nanos(self.unix_nano);

        let mut out = [0u8; FIXED_MAX_LEN];
        if let Some(len) = format_fixed(&dt.naive_utc(), b'T', 3, &mut out) {
            return f.write_str(std::str::from_utf8(&out[..len]).expect("ASCII timestamp"));
        }

        let formatted = dt.format_with_items(
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
        let mut out = [0u8; FIXED_MAX_LEN];
        if let Some(len) = format_fixed(&ndt, b' ', 6, &mut out) {
            let suffix: &[u8] = if self.tz_ref().is_some() { b"+00" } else { b"" };
            buf.put_i32((len + suffix.len()) as i32);
            buf.extend_from_slice(&out[..len]);
            buf.extend_from_slice(suffix);

            return Ok(());
        }

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
            .ok_or(Error::other(
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

    fn chrono_text(ts: &TimestampValue) -> Vec<u8> {
        let mut s = ts
            .to_naive_datetime()
            .format("%Y-%m-%d %H:%M:%S%.6f")
            .to_string();
        if ts.tz_ref().is_some() {
            s += "+00";
        }

        let mut buf = BytesMut::new();
        s.to_text(&mut buf).unwrap();
        buf.to_vec()
    }

    fn chrono_display(ts: &TimestampValue) -> String {
        Utc.timestamp_nanos(ts.get_time_stamp())
            .format("%Y-%m-%dT%H:%M:%S%.3f")
            .to_string()
    }

    #[test]
    fn test_timestamp_fast_format_matches_chrono() {
        let mut values = vec![
            0,
            1650890322123456789,
            1709164800000000000, // 2024-02-29
            1704067199999999000, // 2023-12-31 23:59:59.999999
            -86_400_000_000_000,
            i64::MAX,
            i64::MIN + 1_000_000_000,
        ];
        // Deterministic LCG sweep over the whole i64 nanosecond range (1677..2262).
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        for _ in 0..20_000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            values.push(state as i64);
        }

        for unix_nano in values {
            for tz in [None, Some("UTC".to_string())] {
                let ts = TimestampValue::new(unix_nano, tz);
                assert_eq!(ts.to_string(), chrono_display(&ts), "{}", unix_nano);

                // to_naive_datetime panics for pre-epoch values with a fractional part.
                if unix_nano >= 0 || ts.get_time_stamp() % 1_000_000_000 == 0 {
                    let mut buf = BytesMut::new();
                    ts.to_text(&mut buf).unwrap();
                    assert_eq!(buf.to_vec(), chrono_text(&ts), "{}", unix_nano);
                }
            }
        }
    }

    #[test]
    fn test_timestamp_fast_format_out_of_range_year() {
        let mut out = [0u8; FIXED_MAX_LEN];
        for year in [-1, 10000] {
            let ndt = NaiveDate::from_ymd_opt(year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            assert_eq!(format_fixed(&ndt, b' ', 6, &mut out), None);
        }

        let ndt = NaiveDate::from_ymd_opt(9, 3, 4)
            .unwrap()
            .and_hms_micro_opt(5, 6, 7, 8)
            .unwrap();
        let len = format_fixed(&ndt, b' ', 6, &mut out).unwrap();
        assert_eq!(&out[..len], b"0009-03-04 05:06:07.000008");
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
