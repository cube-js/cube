use crate::compile::engine::df::scan::DataFusionError;
use chrono::{NaiveDate, NaiveDateTime};

pub fn parse_date_str(s: &str) -> Result<NaiveDateTime, DataFusionError> {
    let parsed = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f UTC"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ"))
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|date| date.and_hms_opt(0, 0, 0).unwrap())
        });

    parsed.map_err(|e| {
        DataFusionError::Internal(format!("Can't parse date/time string literal: {}", e))
    })
}
