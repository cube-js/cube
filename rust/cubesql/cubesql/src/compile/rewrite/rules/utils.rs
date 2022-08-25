use datafusion::scalar::ScalarValue;

pub fn parse_granularity(granularity: &ScalarValue, to_normalize: bool) -> Option<String> {
    match granularity {
        ScalarValue::Utf8(Some(granularity)) => {
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
        _ => None,
    }
}
