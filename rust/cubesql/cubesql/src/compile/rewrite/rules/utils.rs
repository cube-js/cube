use datafusion::scalar::ScalarValue;

pub fn parse_granularity_string(granularity: &str, to_normalize: bool) -> Option<String> {
    if to_normalize {
        match granularity.to_lowercase().as_str() {
            "dow" | "doy" => Some("day".to_string()),
            "qtr" => Some("quarter".to_string()),
            _ => Some(granularity.to_lowercase()),
        }
    } else {
        match granularity.to_lowercase().as_str() {
            "qtr" => Some("quarter".to_string()),
            _ => Some(granularity.to_lowercase()),
        }
    }
}

pub fn parse_granularity(granularity: &ScalarValue, to_normalize: bool) -> Option<String> {
    match granularity {
        ScalarValue::Utf8(Some(granularity)) => {
            parse_granularity_string(&granularity, to_normalize)
        }
        _ => None,
    }
}

pub fn granularity_to_interval(granularity: &ScalarValue) -> Option<ScalarValue> {
    if let Some(granularity) = parse_granularity(granularity, false) {
        let interval = match granularity.as_str() {
            "second" => ScalarValue::IntervalDayTime(Some(1000)),
            "minute" => ScalarValue::IntervalDayTime(Some(60000)),
            "hour" => ScalarValue::IntervalDayTime(Some(3600000)),
            "day" => ScalarValue::IntervalDayTime(Some(4294967296)),
            "week" => ScalarValue::IntervalDayTime(Some(30064771072)),
            "month" => ScalarValue::IntervalYearMonth(Some(1)),
            "quarter" => ScalarValue::IntervalYearMonth(Some(3)),
            "year" => ScalarValue::IntervalYearMonth(Some(12)),
            _ => return None,
        };

        return Some(interval);
    }

    None
}
