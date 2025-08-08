use crate::protocol::{ErrorCode, ErrorResponse};
use crate::timestamp::pg_base_date_epoch;
use crate::{FromProtocolValue, ProtocolError, ToProtocolValue};
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use chrono::NaiveDate;
use std::backtrace::Backtrace;
use std::io::{Error, ErrorKind};

pub type DateValue = NaiveDate;

impl ToProtocolValue for DateValue {
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

impl FromProtocolValue for DateValue {
    // date_in - https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/date.c#L111
    fn from_text(raw: &[u8]) -> Result<Self, ProtocolError> {
        let as_str = std::str::from_utf8(raw).map_err(|err| ProtocolError::ErrorResponse {
            source: ErrorResponse::error(ErrorCode::ProtocolViolation, err.to_string()),
            backtrace: Backtrace::capture(),
        })?;

        // Parse date string in format "YYYY-MM-DD"
        NaiveDate::parse_from_str(as_str, "%Y-%m-%d").map_err(|err| ProtocolError::ErrorResponse {
            source: ErrorResponse::error(
                ErrorCode::ProtocolViolation,
                format!("Unable to parse date from text '{}': {}", as_str, err),
            ),
            backtrace: Backtrace::capture(),
        })
    }

    // date_recv - https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/date.c#L207
    fn from_binary(raw: &[u8]) -> Result<Self, ProtocolError> {
        if raw.len() != 4 {
            return Err(ProtocolError::ErrorResponse {
                source: ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!(
                        "Invalid binary date format, expected 4 bytes, got {}",
                        raw.len()
                    ),
                ),
                backtrace: Backtrace::capture(),
            });
        }

        let days_since_epoch = BigEndian::read_i32(raw);
        let base_date = pg_base_date_epoch().date();

        base_date
            .checked_add_signed(chrono::Duration::days(days_since_epoch as i64))
            .ok_or_else(|| ProtocolError::ErrorResponse {
                source: ErrorResponse::error(
                    ErrorCode::ProtocolViolation,
                    format!(
                        "Date value {} days from epoch is out of range",
                        days_since_epoch
                    ),
                ),
                backtrace: Backtrace::capture(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Format;

    #[test]
    fn test_date_from_text() {
        let date_str = b"2025-08-08";
        let result = DateValue::from_protocol(date_str, Format::Text).unwrap();
        let expected = DateValue::from_ymd_opt(2025, 8, 8).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_date_from_binary() {
        // Create a date and encode it
        let date = DateValue::from_ymd_opt(2025, 8, 8).unwrap();
        let mut buf = BytesMut::new();
        date.to_binary(&mut buf).unwrap();

        // Skip the length prefix (4 bytes) to get the actual data
        let binary_data = &buf[4..];

        // Decode it back
        let result = DateValue::from_protocol(binary_data, Format::Binary).unwrap();
        assert_eq!(result, date);
    }

    #[test]
    fn test_date_from_text_invalid() {
        let invalid_date = b"not-a-date";
        let result = DateValue::from_protocol(invalid_date, Format::Text);
        assert!(result.is_err());
    }

    #[test]
    fn test_date_before_the_pg_epoch() {
        // Test date before epoch
        let before_epoch = DateValue::from_ymd_opt(1999, 12, 31).unwrap();
        let mut buf = BytesMut::new();
        before_epoch.to_binary(&mut buf).unwrap();
        let binary_data = &buf[4..];
        let result = DateValue::from_protocol(binary_data, Format::Binary).unwrap();
        assert_eq!(result, before_epoch);
    }
}
