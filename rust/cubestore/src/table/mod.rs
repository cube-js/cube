use crate::util::decimal::Decimal;
use crate::util::ordfloat::OrdF64;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, Int64Decimal0Array,
    Int64Decimal10Array, Int64Decimal1Array, Int64Decimal2Array, Int64Decimal3Array,
    Int64Decimal4Array, Int64Decimal5Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use chrono::{SecondsFormat, TimeZone, Utc};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Formatter};

pub mod data;
pub(crate) mod parquet;
pub mod redistribute;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Debug)]
pub enum TableValue {
    Null,
    String(String),
    Int(i64),
    Decimal(Decimal),
    Float(OrdF64),
    Bytes(Vec<u8>),
    Timestamp(TimestampValue),
    Boolean(bool),
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
            DataType::Int64Decimal(0) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal0Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64Decimal(1) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal1Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64Decimal(2) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal2Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64Decimal(3) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal3Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64Decimal(4) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal4Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64Decimal(5) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal5Array>()
                    .unwrap()
                    .value(row),
            )),
            DataType::Int64Decimal(10) => TableValue::Decimal(Decimal::new(
                a.as_any()
                    .downcast_ref::<Int64Decimal10Array>()
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

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct Row {
    values: Vec<TableValue>,
}

pub struct RowSortKey<'a> {
    row: &'a Row,
    sort_key_size: usize,
}

impl Row {
    pub fn new(values: Vec<TableValue>) -> Row {
        Row { values }
    }

    pub fn sort_key(&self, sort_key_size: u64) -> RowSortKey {
        RowSortKey {
            row: self,
            sort_key_size: sort_key_size as usize,
        }
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

impl<'a> PartialEq for RowSortKey<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.sort_key_size != other.sort_key_size {
            return false;
        }
        for i in 0..self.sort_key_size {
            if self.row.values[i] != other.row.values[i] {
                return false;
            }
        }
        true
    }
}

impl<'a> Eq for RowSortKey<'a> {}

impl<'a> PartialOrd for RowSortKey<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.sort_key_size != other.sort_key_size {
            return None;
        }
        for i in 0..self.sort_key_size {
            let ord = cmp_same_types(&self.row.values[i], &other.row.values[i]);
            if ord != Ordering::Equal {
                return Some(ord);
            }
        }
        Some(Ordering::Equal)
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

impl<'a> Ord for RowSortKey<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::table::{TableValue, TimestampValue};
    use crate::util::decimal::Decimal;
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
}
