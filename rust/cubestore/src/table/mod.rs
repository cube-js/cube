use crate::CubeError;
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::Arc;
use crate::metastore::Column;
use ::parquet::file::metadata::RowGroupMetaData;
use chrono::{Utc, SecondsFormat, TimeZone};
use bigdecimal::BigDecimal;

pub(crate) mod parquet;

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Debug, Hash)]
pub enum TableValue {
    Null,
    String(String),
    Int(i64),
    Decimal(BigDecimal),
    Bytes(Vec<u8>),
    Timestamp(TimestampValue),
    Boolean(bool),
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Debug, Hash)]
pub struct TimestampValue {
    unix_nano: i64
}

impl TimestampValue {
    pub fn new(unix_nano: i64) -> TimestampValue {
        TimestampValue { unix_nano }
    }

    pub fn get_time_stamp(&self) -> i64 {
        self.unix_nano
    }
}

impl ToString for TimestampValue {
    fn to_string(&self) -> String {
        Utc.timestamp_nanos(self.unix_nano).to_rfc3339_opts(SecondsFormat::Millis, true)
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
pub struct Row {
    values: Vec<TableValue>
}

pub struct RowSortKey<'a> {
    row: &'a Row,
    sort_key_size: usize
}

impl Row {
    pub fn new(values: Vec<TableValue>) -> Row {
        Row { values }
    }

    pub fn sort_key(&self, sort_key_size: u64) -> RowSortKey {
        RowSortKey {
            row: self,
            sort_key_size: sort_key_size as usize
        }
    }

    pub fn push(& mut self, val: TableValue) {
        &self.values.push(val);
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
            if self.row.values[i] != other.row.values[i] {
                return self.row.values[i].partial_cmp(&other.row.values[i]);
            }
        }
        Some(Ordering::Equal)
    }
}

impl<'a> Ord for RowSortKey<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum ColumnType {
    String,
    Int,
    Double,
    Bytes,
    Timestamp,
    Boolean
}

pub trait TableStore {
    fn merge_rows<'a>(&'a self, source_file: Option<&'a str>, dest_files: Vec<String>, rows: Vec<Row>, sort_key_size: u64) -> Result<Vec<(u64, (Row, Row))>, CubeError>;

    fn read_rows(&self, file: &str) -> Result<Vec<Row>, CubeError>;

    fn read_filtered_rows(&self, file: &str, columns: &Vec<crate::metastore::Column>, limit: usize) -> Result<Vec<Row>, CubeError>;

    fn scan_node(&self, file: &str, columns: &Vec<Column>, row_group_filter: Option<Arc<dyn Fn(&RowGroupMetaData) -> bool + Send + Sync>>) -> Result<Arc<dyn ExecutionPlan + Send + Sync>, CubeError>;
}
