use crate::table::data::{Rows, RowsView};
use crate::util::decimal::Decimal;
use crate::util::ordfloat::OrdF64;
use crate::CubeError;
use chrono::{SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Debug, Formatter};

pub mod data;
pub(crate) mod parquet;

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

#[derive(Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimestampValue {
    unix_nano: i64,
}

impl TimestampValue {
    pub fn new(unix_nano: i64) -> TimestampValue {
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

pub trait TableStore {
    fn merge_rows<'a>(
        &'a self,
        source_file: Option<&'a str>,
        dest_files: Vec<String>,
        rows: RowsView<'a>,
        sort_key_size: usize,
    ) -> Result<Vec<(u64, (Row, Row))>, CubeError>;

    fn read_rows(&self, file: &str) -> Result<Rows, CubeError>;

    fn read_filtered_rows(
        &self,
        file: &str,
        columns: &Vec<crate::metastore::Column>,
        limit: usize,
    ) -> Result<Rows, CubeError>;

    // fn scan_node(
    //     &self,
    //     file: &str,
    //     columns: &Vec<Column>,
    //     row_group_filter: Option<Arc<dyn Fn(&RowGroupMetaData) -> bool + Send + Sync>>,
    // ) -> Result<Arc<dyn ExecutionPlan + Send + Sync>, CubeError>;
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
