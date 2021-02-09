use chrono::format::Item::{Literal, Numeric, Space};
use chrono::format::Numeric::{Day, Hour, Minute, Month, Second, Year};
use chrono::format::Pad::Zero;
use chrono::format::{Item, Parsed};
use chrono::{DateTime, Local, ParseError, Utc};
use itertools::Itertools;

fn parse(s: &str, fmt: &[Item]) -> Result<Parsed, ParseError> {
    let mut parsed = Parsed::new();
    chrono::format::parse(&mut parsed, s, fmt.iter())?;
    Ok(parsed)
}

pub fn parse_timestamp_to_nanos(v: &str) -> Result<i64, ParseError> {
    // Option 1. UTC format.
    if v.ends_with("UTC") {
        // corresponds to: "%Y-%m-%d %H:%M:%S UTC".
        const UTC_FORMAT: [Item; 13] = [
            Numeric(Year, Zero),
            Literal("-"),
            Numeric(Month, Zero),
            Literal("-"),
            Numeric(Day, Zero),
            Space(" "),
            Numeric(Hour, Zero),
            Literal(":"),
            Numeric(Minute, Zero),
            Literal(":"),
            Numeric(Second, Zero),
            Space(" "),
            Literal("UTC"),
        ];
        return parse(v, &UTC_FORMAT)
            .and_then(|v| v.to_datetime_with_timezone(&Utc))
            .map(|v| v.timestamp_nanos());
    }

    // Option 2. Naive format.
    // corresponds to "%Y-%m-%d %H:%M:%S".
    const NAIVE_FORMAT: [Item; 11] = [
        Numeric(Year, Zero),
        Literal("-"),
        Numeric(Month, Zero),
        Literal("-"),
        Numeric(Day, Zero),
        Space(" "),
        Numeric(Hour, Zero),
        Literal(":"),
        Numeric(Minute, Zero),
        Literal(":"),
        Numeric(Second, Zero),
    ];
    if let Ok(nanos) = parse(v, &NAIVE_FORMAT)
        .and_then(|v| v.to_datetime_with_timezone(&Utc))
        .map(|v| v.timestamp_nanos())
    {
        // Note that we interpret as UTC, not as the local timestamp.
        return Ok(nanos);
    }

    // Option 3. RFC3339
    DateTime::parse_from_rfc3339(v).map(|v| v.timestamp_nanos())
}
