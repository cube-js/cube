use chrono::SecondsFormat;
use crate::types::{DBResponsePrimitive, DBResponseValue};

pub fn transform_value(value: DBResponseValue, type_: &str) -> DBResponsePrimitive {
    match value {
        DBResponseValue::DateTime(dt) if type_ == "time" || type_.is_empty() => {
            let formatted = dt.to_rfc3339_opts(SecondsFormat::Millis, true);
            DBResponsePrimitive::String(formatted)
        }
        DBResponseValue::Primitive(p) => p,
        DBResponseValue::Object { value } => value,
        _ => DBResponsePrimitive::Null,
    }
}

