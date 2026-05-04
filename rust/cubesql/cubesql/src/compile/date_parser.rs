use crate::compile::engine::df::scan::DataFusionError;
use chrono::{NaiveDate, NaiveDateTime};

pub fn parse_date_str(s: &str) -> Result<NaiveDateTime, DataFusionError> {
    if let Some(parsed) = parse_fast(s) {
        return Ok(parsed);
    }

    parse_with_chrono(s).map_err(|e| {
        DataFusionError::Internal(format!(
            "Can't parse date/time string literal {:?}: {}",
            s, e
        ))
    })
}

#[inline]
fn digit(b: u8) -> Option<u32> {
    let d = b.wrapping_sub(b'0');
    if d <= 9 {
        Some(d as u32)
    } else {
        None
    }
}

#[inline]
fn ascii_u32_2(b: &[u8], at: usize) -> Option<u32> {
    Some(digit(b[at])? * 10 + digit(b[at + 1])?)
}

#[inline]
fn ascii_u32_4(b: &[u8], at: usize) -> Option<u32> {
    Some(
        digit(b[at])? * 1000 + digit(b[at + 1])? * 100 + digit(b[at + 2])? * 10 + digit(b[at + 3])?,
    )
}

/// Recognises only `YYYY-MM-DDTHH:MM:SS.fff` (23 bytes, `T` separator,
/// 3-digit fraction). Returns `None` for any other length or layout — the
/// caller falls through to the chrono cascade.
fn parse_fast(s: &str) -> Option<NaiveDateTime> {
    let b = s.as_bytes();
    if b.len() != 23
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'.'
    {
        return None;
    }

    let year = ascii_u32_4(b, 0)? as i32;
    let month = ascii_u32_2(b, 5)?;
    let day = ascii_u32_2(b, 8)?;
    let hour = ascii_u32_2(b, 11)?;
    let minute = ascii_u32_2(b, 14)?;
    let second = ascii_u32_2(b, 17)?;
    let frac_millis = digit(b[20])? * 100 + digit(b[21])? * 10 + digit(b[22])?;

    NaiveDate::from_ymd_opt(year, month, day)?.and_hms_nano_opt(
        hour,
        minute,
        second,
        frac_millis * 1_000_000,
    )
}

fn parse_with_chrono(s: &str) -> chrono::ParseResult<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f UTC"))
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|date| date.and_hms_opt(0, 0, 0).unwrap())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd_hmsn(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32, n: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_nano_opt(h, mi, s, n)
            .unwrap()
    }

    #[test]
    fn fast_path_accepts_canonical_shape() {
        let cases: &[(&str, NaiveDateTime)] = &[
            ("2022-01-01T00:00:00.000", ymd_hmsn(2022, 1, 1, 0, 0, 0, 0)),
            (
                "2024-06-15T13:45:07.123",
                ymd_hmsn(2024, 6, 15, 13, 45, 7, 123_000_000),
            ),
            (
                "9999-12-31T23:59:59.999",
                ymd_hmsn(9999, 12, 31, 23, 59, 59, 999_000_000),
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_fast(input), Some(*expected), "fast path: {}", input);
            assert_eq!(
                parse_date_str(input).unwrap(),
                *expected,
                "wrapper: {}",
                input
            );
        }
    }

    #[test]
    fn fast_path_rejects_non_canonical() {
        // Wrong length / shape — must not be fast-parsed.
        let rejects = [
            "2022",
            "2022-01-01",
            "2022-01-01 00:00:00",
            "2022-01-01T00:00:00",
            "2022-01-01T00:00:00.000Z",
            "2022-01-01 00:00:00.000",
            "2022-01-01T00:00:00.123456",
            "2022-13-01T00:00:00.000",
            "2022-01-32T00:00:00.000",
            "2022/01/01T00:00:00.000",
            "2022-01-01x00:00:00.000",
            "2022-01-01T25:00:00.000",
            "2022-01-01T00:60:00.000",
            "2022-01-01T00:00:60.000",
        ];
        for s in rejects {
            assert!(parse_fast(s).is_none(), "unexpectedly fast-parsed: {:?}", s);
        }
    }

    #[test]
    fn wrapper_handles_other_shapes_via_chrono_fallback() {
        let cases: &[(&str, NaiveDateTime)] = &[
            ("2022-01-01", ymd_hmsn(2022, 1, 1, 0, 0, 0, 0)),
            ("2022-01-01 00:00:00", ymd_hmsn(2022, 1, 1, 0, 0, 0, 0)),
            ("2022-01-01T00:00:00", ymd_hmsn(2022, 1, 1, 0, 0, 0, 0)),
            ("2022-01-01T00:00:00.000Z", ymd_hmsn(2022, 1, 1, 0, 0, 0, 0)),
            (
                "2022-01-01 00:00:00.000 UTC",
                ymd_hmsn(2022, 1, 1, 0, 0, 0, 0),
            ),
            (
                "2024-06-15T13:45:07.123456789",
                ymd_hmsn(2024, 6, 15, 13, 45, 7, 123_456_789),
            ),
        ];

        for (input, expected) in cases {
            assert!(
                parse_fast(input).is_none(),
                "fast path should reject: {}",
                input
            );
            assert_eq!(
                parse_date_str(input).unwrap(),
                *expected,
                "wrapper: {}",
                input
            );
        }
    }

    #[test]
    fn rejects_propagate_through_wrapper() {
        for s in ["", "2022/01/01", "2022-01-01T00:00:00+02:00"] {
            assert!(parse_date_str(s).is_err(), "should error: {:?}", s);
        }
    }
}
