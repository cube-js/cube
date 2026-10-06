use crate::table::TableValue;
use deepsize::{Context, DeepSizeOf};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Value of a cache item, queue payload or queue result.
///
/// Serialized as a plain string (`Text`) or as a blob (`Binary`), so rows written before binary
/// support existed deserialize as `Text`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum CacheValue {
    Text(String),
    Binary(Vec<u8>),
}

impl CacheValue {
    pub fn len(&self) -> usize {
        match self {
            CacheValue::Text(s) => s.len(),
            CacheValue::Binary(b) => b.len(),
        }
    }

    pub fn is_binary(&self) -> bool {
        matches!(self, CacheValue::Binary(_))
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            CacheValue::Text(s) => Some(s),
            CacheValue::Binary(_) => None,
        }
    }

    /// Binary is rendered the same way TableValue::Bytes is rendered for clients
    pub fn to_display_string(&self) -> String {
        match self {
            CacheValue::Text(s) => s.clone(),
            CacheValue::Binary(b) => format!("0x{}", hex::encode_upper(b)),
        }
    }

    pub fn into_table_value(self) -> TableValue {
        match self {
            CacheValue::Text(s) => TableValue::String(s),
            CacheValue::Binary(b) => TableValue::Bytes(b),
        }
    }
}

impl From<String> for CacheValue {
    fn from(value: String) -> Self {
        CacheValue::Text(value)
    }
}

impl From<&str> for CacheValue {
    fn from(value: &str) -> Self {
        CacheValue::Text(value.to_string())
    }
}

impl From<Vec<u8>> for CacheValue {
    fn from(value: Vec<u8>) -> Self {
        CacheValue::Binary(value)
    }
}

impl DeepSizeOf for CacheValue {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            CacheValue::Text(s) => s.deep_size_of_children(context),
            CacheValue::Binary(b) => b.deep_size_of_children(context),
        }
    }
}

impl Serialize for CacheValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CacheValue::Text(s) => serializer.serialize_str(s),
            CacheValue::Binary(b) => serializer.serialize_bytes(b),
        }
    }
}

struct CacheValueVisitor;

impl<'de> Visitor<'de> for CacheValueVisitor {
    type Value = CacheValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string or bytes")
    }

    fn visit_str<E: Error>(self, v: &str) -> Result<Self::Value, E> {
        Ok(CacheValue::Text(v.to_string()))
    }

    fn visit_string<E: Error>(self, v: String) -> Result<Self::Value, E> {
        Ok(CacheValue::Text(v))
    }

    fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Self::Value, E> {
        Ok(CacheValue::Binary(v.to_vec()))
    }

    fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Self::Value, E> {
        Ok(CacheValue::Binary(v))
    }
}

impl<'de> Deserialize<'de> for CacheValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // String/bytes visitors are interchangeable in serde (a UTF-8 blob would pass as a
        // string), so the stored type has to drive the choice
        deserializer.deserialize_any(CacheValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Row {
        value: CacheValue,
    }

    #[derive(Serialize)]
    struct LegacyRow {
        value: String,
    }

    fn round_trip(value: CacheValue) -> CacheValue {
        let bytes = flexbuffers::to_vec(&Row { value }).unwrap();
        let row: Row = flexbuffers::from_slice(&bytes).unwrap();
        row.value
    }

    #[test]
    fn test_flexbuffers_round_trip() {
        assert_eq!(
            round_trip(CacheValue::Text("text".to_string())),
            CacheValue::Text("text".to_string())
        );
        assert_eq!(
            round_trip(CacheValue::Binary(vec![0, 159, 146, 150])),
            CacheValue::Binary(vec![0, 159, 146, 150])
        );
        // Valid UTF-8 must not turn into Text
        assert_eq!(
            round_trip(CacheValue::Binary(b"utf8".to_vec())),
            CacheValue::Binary(b"utf8".to_vec())
        );
        assert_eq!(
            round_trip(CacheValue::Binary(vec![])),
            CacheValue::Binary(vec![])
        );
    }

    #[test]
    fn test_reads_legacy_string() {
        let bytes = flexbuffers::to_vec(&LegacyRow {
            value: "legacy".to_string(),
        })
        .unwrap();
        let row: Row = flexbuffers::from_slice(&bytes).unwrap();

        assert_eq!(row.value, CacheValue::Text("legacy".to_string()));
    }

    #[test]
    fn test_display_and_table_value() {
        let binary = CacheValue::Binary(vec![0x0a, 0xff]);
        assert_eq!(binary.to_display_string(), "0x0AFF");
        assert_eq!(
            binary.into_table_value(),
            TableValue::Bytes(vec![0x0a, 0xff])
        );

        let text = CacheValue::Text("ab".to_string());
        assert_eq!(text.to_display_string(), "ab");
        assert_eq!(
            text.into_table_value(),
            TableValue::String("ab".to_string())
        );
    }
}
