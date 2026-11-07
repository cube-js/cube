use crate::metastore::ColumnType;
use crate::util::decimal::{Decimal, Decimal96};
use crate::util::int96::Int96;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::cube_ext::ordfloat::OrdF64;
use chrono::{DateTime, SecondsFormat, Utc};
use deepsize::{Context, DeepSizeOf};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Formatter};

pub mod data;
pub mod parquet;
pub mod redistribute;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug, Hash, PartialOrd)]
pub enum TableValue {
    Null,
    String(String),
    Int(i64),
    Int96(Int96),
    Decimal(Decimal),
    Decimal96(Decimal96),
    Float(OrdF64),
    Bytes(Vec<u8>),
    Timestamp(TimestampValue),
    Boolean(bool),
}

impl DeepSizeOf for TableValue {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        match self {
            TableValue::Null => 0,
            TableValue::String(v) => v.deep_size_of_children(context),
            TableValue::Int(_) => 0,
            TableValue::Int96(_) => 0,
            TableValue::Decimal(_) => 0,
            TableValue::Decimal96(_) => 0,
            TableValue::Float(_) => 0,
            TableValue::Bytes(v) => v.deep_size_of_children(context),
            TableValue::Timestamp(_) => 0,
            TableValue::Boolean(_) => 0,
        }
    }
}

impl TableValue {
    pub fn from_columns(a: &[ArrayRef], row: usize) -> Vec<TableValue> {
        a.iter()
            .map(|c| TableValue::from_array(c.as_ref(), row))
            .collect_vec()
    }

    pub fn from_array(a: &dyn Array, row: usize) -> TableValue {
        if !a.is_valid(row) {
            return TableValue::Null;
        }
        match a.data_type() {
            DataType::Int64 => {
                TableValue::Int(a.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
            }
            // DataType::Int96 => TableValue::Int96(Int96::new(
            //     a.as_any().downcast_ref::<Int96Array>().unwrap().value(row),
            // )),
            DataType::Utf8 => TableValue::String(
                a.as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row)
                    .to_string(),
            ),
            DataType::Binary => TableValue::Bytes(
                a.as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row)
                    .to_vec(),
            ),
            DataType::Decimal128(_, _) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Float64 => TableValue::Float(
                a.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
                    .into(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                TableValue::Timestamp(TimestampValue::from_micros(
                    a.as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(row),
                ))
            }
            DataType::Boolean => TableValue::Boolean(
                a.as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(row),
            ),
            other => panic!(
                "unexpected array type when converting to TableValue: {:?}",
                other
            ),
        }
    }

    /// Render the value as a string, using `column_type` for context-dependent
    /// variants (currently `Decimal` / `Decimal96`, where scale lives on the
    /// column rather than on the value). Falls back to `Display` otherwise.
    pub fn format_with(&self, column_type: &ColumnType) -> String {
        match (self, column_type) {
            (TableValue::Decimal(v), ColumnType::Decimal { scale, .. }) => {
                v.to_string(*scale as u8)
            }
            (TableValue::Decimal96(v), ColumnType::Decimal96 { scale, .. }) => {
                v.to_string(*scale as u8)
            }
            (v, _) => v.to_string(),
        }
    }
}

impl fmt::Display for TableValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TableValue::Null => f.write_str("NULL"),
            TableValue::String(v) => f.write_str(v),
            TableValue::Int(v) => write!(f, "{}", v),
            TableValue::Int96(v) => f.write_str(&v.to_string()),
            // Scale-unaware fallbacks; `format_with` produces the proper form.
            TableValue::Decimal(v) => write!(f, "{}", v.raw_value()),
            TableValue::Decimal96(v) => write!(f, "{}", v.raw_value()),
            TableValue::Float(v) => write!(f, "{}", v.0),
            TableValue::Bytes(v) => {
                f.write_str("0x")?;
                for b in v {
                    write!(f, "{:02x}", b)?;
                }
                Ok(())
            }
            TableValue::Timestamp(v) => f.write_str(&v.to_string()),
            TableValue::Boolean(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[serde(from = "TimestampValueSerde", into = "TimestampValueSerde")]
pub struct TimestampValue {
    unix_micro: i64,
}

// Stored form. `unix_nano` saturates past the nanosecond range, so binaries that only read it see
// such values clamped but ordered. `unix_micro` is exact; where it is absent, `unix_nano` is.
#[derive(Serialize, Deserialize)]
struct TimestampValueSerde {
    unix_nano: i64,
    #[serde(default)]
    unix_micro: Option<i64>,
}

impl From<TimestampValueSerde> for TimestampValue {
    fn from(v: TimestampValueSerde) -> Self {
        match v.unix_micro {
            Some(unix_micro) => TimestampValue { unix_micro },
            None => TimestampValue::from_nanos(v.unix_nano),
        }
    }
}

impl From<TimestampValue> for TimestampValueSerde {
    fn from(v: TimestampValue) -> Self {
        let unix_nano = v
            .unix_micro
            .checked_mul(1000)
            .unwrap_or(if v.unix_micro < 0 { i64::MIN } else { i64::MAX });
        TimestampValueSerde {
            unix_nano,
            unix_micro: Some(v.unix_micro),
        }
    }
}

impl TimestampValue {
    pub fn from_micros(unix_micro: i64) -> TimestampValue {
        TimestampValue { unix_micro }
    }

    /// Sub-microsecond digits are dropped: tables store microseconds.
    pub fn from_nanos(unix_nano: i64) -> TimestampValue {
        TimestampValue {
            unix_micro: unix_nano / 1000,
        }
    }

    pub fn micros(&self) -> i64 {
        self.unix_micro
    }

    /// `None` past chrono's range of about 262,000 years, which i64 microseconds exceed.
    pub fn to_datetime(&self) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_micros(self.unix_micro)
    }
}

impl Debug for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampValue")
            .field("unix_micro", &self.unix_micro)
            .field("str", &self.to_string())
            .finish()
    }
}

impl ToString for TimestampValue {
    fn to_string(&self) -> String {
        match self.to_datetime() {
            Some(dt) => dt.to_rfc3339_opts(SecondsFormat::Millis, true),
            None => format!("{} microseconds since epoch", self.unix_micro),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, DeepSizeOf, PartialOrd)]
pub struct Row {
    values: Vec<TableValue>,
}

impl Row {
    pub fn new(values: Vec<TableValue>) -> Row {
        Row { values }
    }

    pub fn push(&mut self, val: TableValue) {
        self.values.push(val);
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn values(&self) -> &Vec<TableValue> {
        &self.values
    }
}

pub fn cmp_same_types(l: &TableValue, r: &TableValue) -> Ordering {
    match (l, r) {
        (TableValue::Null, TableValue::Null) => Ordering::Equal,
        (TableValue::Null, _) => Ordering::Less,
        (_, TableValue::Null) => Ordering::Greater,
        (TableValue::String(a), TableValue::String(b)) => a.cmp(b),
        (TableValue::Int(a), TableValue::Int(b)) => a.cmp(b),
        (TableValue::Decimal(a), TableValue::Decimal(b)) => a.cmp(b),
        (TableValue::Float(a), TableValue::Float(b)) => a.cmp(b),
        (TableValue::Bytes(a), TableValue::Bytes(b)) => a.cmp(b),
        (TableValue::Timestamp(a), TableValue::Timestamp(b)) => a.cmp(b),
        (TableValue::Boolean(a), TableValue::Boolean(b)) => a.cmp(b),
        (a, b) => panic!("Can't compare {:?} to {:?}", a, b),
    }
}

#[cfg(test)]
mod tests {
    use crate::table::{TableValue, TimestampValue};
    use crate::util::decimal::Decimal;
    use deepsize::DeepSizeOf;
    use serde::{Deserialize, Serialize};

    // The nanosecond-only shape that older binaries read.
    #[derive(Serialize, Deserialize, Debug)]
    struct NanosTimestampValue {
        unix_nano: i64,
    }

    fn to_flexbuffer<T: Serialize>(v: &T) -> Vec<u8> {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        v.serialize(&mut s).unwrap();
        s.take_buffer()
    }

    #[test]
    fn timestamp_serialization_compatibility() {
        let old = to_flexbuffer(&NanosTimestampValue {
            unix_nano: 1_577_836_800_123_456_000,
        });
        let v =
            TimestampValue::deserialize(flexbuffers::Reader::get_root(&old[..]).unwrap()).unwrap();
        assert_eq!(v, TimestampValue::from_micros(1_577_836_800_123_456));

        for (micros, nanos_seen_by_old_readers) in [
            (1_577_836_800_123_456, 1_577_836_800_123_456_000),
            (14_579_308_800_000_000, i64::MAX),  // 2432-01-01
            (-14_831_769_600_000_000, i64::MIN), // 1500-01-01
        ] {
            let b = to_flexbuffer(&TimestampValue::from_micros(micros));
            let old =
                NanosTimestampValue::deserialize(flexbuffers::Reader::get_root(&b[..]).unwrap())
                    .unwrap();
            assert_eq!(old.unix_nano, nanos_seen_by_old_readers);
            let new = TimestampValue::deserialize(flexbuffers::Reader::get_root(&b[..]).unwrap())
                .unwrap();
            assert_eq!(new.micros(), micros);
        }

        assert_eq!(
            TimestampValue::from_micros(253_402_214_400_000_000).to_string(),
            "9999-12-31T00:00:00.000Z"
        );
    }

    #[test]
    fn serialization() {
        for v in &[
            TableValue::Null,
            TableValue::String("foo".into()),
            TableValue::Int(123),
            TableValue::Decimal(Decimal::new(123)),
            TableValue::Float(12_f64.into()),
            TableValue::Bytes(vec![1, 2, 3]),
            TableValue::Timestamp(TimestampValue::from_micros(123)),
            // 9999-12-31, past the nanosecond range.
            TableValue::Timestamp(TimestampValue::from_micros(253_402_214_400_000_000)),
            TableValue::Boolean(false),
        ] {
            let b = bincode::serialize(v).expect(&format!("could not serialize {:?}", v));
            let v2: TableValue =
                bincode::deserialize(&b).expect(&format!("could not deserialize {:?}", v));
            assert_eq!(v, &v2);

            let mut s = flexbuffers::FlexbufferSerializer::new();
            v.serialize(&mut s)
                .expect(&format!("could not serialize {:?}", v));
            let b = s.take_buffer();
            let v2 = TableValue::deserialize(flexbuffers::Reader::get_root(&b).unwrap())
                .expect(&format!("could not deserialize {:?}", v));
            assert_eq!(v, &v2);
        }
    }

    #[test]
    fn table_value_deep_size_of() {
        for (v, expected_size) in [
            (TableValue::Null, 32_usize),
            (TableValue::Int(1), 32_usize),
            (TableValue::Decimal(Decimal::new(1)), 32_usize),
            (TableValue::String("foo".into()), 35_usize),
            (TableValue::String("foofoo".into()), 38_usize),
        ] {
            assert_eq!(v.deep_size_of(), expected_size, "size for {:?}", v);
        }
    }
}
