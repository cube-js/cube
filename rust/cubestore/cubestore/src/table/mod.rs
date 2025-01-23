use crate::util::decimal::{Decimal, Decimal96};
use crate::util::int96::Int96;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float64Array, Int64Array,
    StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};

use crate::cube_ext::ordfloat::OrdF64;
use chrono::{SecondsFormat, TimeZone, Utc};
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
                TableValue::Timestamp(TimestampValue::new(
                    1000 * a
                        .as_any()
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
}

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimestampValue {
    unix_nano: i64,
}

impl TimestampValue {
    pub fn new(mut unix_nano: i64) -> TimestampValue {
        // This is a hack to workaround a mismatch between on-disk and in-memory representations.
        // We use millisecond precision on-disk.
        unix_nano -= unix_nano % 1000;
        TimestampValue { unix_nano }
    }

    pub fn get_time_stamp(&self) -> i64 {
        self.unix_nano
    }
}

impl Debug for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampValue")
            .field("unix_nano", &self.unix_nano)
            .field("str", &self.to_string())
            .finish()
    }
}

impl ToString for TimestampValue {
    fn to_string(&self) -> String {
        Utc.timestamp_nanos(self.unix_nano)
            .to_rfc3339_opts(SecondsFormat::Millis, true)
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

    #[test]
    fn serialization() {
        for v in &[
            TableValue::Null,
            TableValue::String("foo".into()),
            TableValue::Int(123),
            TableValue::Decimal(Decimal::new(123)),
            TableValue::Float(12_f64.into()),
            TableValue::Bytes(vec![1, 2, 3]),
            TableValue::Timestamp(TimestampValue::new(123)),
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
