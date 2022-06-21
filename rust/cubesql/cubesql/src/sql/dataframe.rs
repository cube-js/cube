use chrono::{
    format::{
        Fixed, Item,
        Numeric::{Day, Hour, Minute, Month, Second, Year},
        Pad::Zero,
    },
    prelude::*,
};
use chrono_tz::Tz;
use comfy_table::{Cell, Table};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, DecimalArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray, ListArray,
        StringArray, TimestampMicrosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::{DataType, IntervalUnit, TimeUnit},
    record_batch::RecordBatch,
    temporal_conversions,
};
use rust_decimal::prelude::*;
use std::{
    fmt::{self, Debug, Formatter},
    io,
};

use super::{ColumnFlags, ColumnType};

use crate::{
    make_string_interval_day_time, make_string_interval_month_day_nano,
    make_string_interval_year_month, CubeError,
};

#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    column_type: ColumnType,
    column_flags: ColumnFlags,
}

impl Column {
    pub fn new(name: String, column_type: ColumnType, column_flags: ColumnFlags) -> Column {
        Column {
            name,
            column_type,
            column_flags,
        }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_type(&self) -> ColumnType {
        self.column_type.clone()
    }

    pub fn get_flags(&self) -> ColumnFlags {
        self.column_flags
    }
}

#[derive(Debug)]
pub struct Row {
    values: Vec<TableValue>,
}

impl Row {
    pub fn new(values: Vec<TableValue>) -> Row {
        Row { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn values(&self) -> &Vec<TableValue> {
        &self.values
    }

    pub fn push(&mut self, val: TableValue) {
        self.values.push(val);
    }
}

#[derive(Debug)]
pub enum TableValue {
    Null,
    String(String),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Boolean(bool),
    Float32(f32),
    Float64(f64),
    List(ArrayRef),
    Decimal128(Decimal128Value),
    Timestamp(TimestampValue),
}

impl ToString for TableValue {
    fn to_string(&self) -> String {
        match &self {
            TableValue::Null => "NULL".to_string(),
            TableValue::String(v) => v.clone(),
            TableValue::Int16(v) => v.to_string(),
            TableValue::Int32(v) => v.to_string(),
            TableValue::Int64(v) => v.to_string(),
            TableValue::Boolean(v) => v.to_string(),
            TableValue::Float32(v) => v.to_string(),
            TableValue::Float64(v) => v.to_string(),
            TableValue::Timestamp(v) => v.to_string(),
            TableValue::Decimal128(v) => v.to_string(),
            TableValue::List(v) => {
                let mut values: Vec<String> = Vec::with_capacity(v.len());

                macro_rules! write_native_array_as_text {
                    ($ARRAY:expr, $ARRAY_TYPE: ident) => {{
                        let arr = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

                        for i in 0..$ARRAY.len() {
                            if arr.is_null(i) {
                                values.push("NULL".to_string());
                            } else {
                                values.push(arr.value(i).to_string());
                            }
                        }
                    }};
                }

                match v.data_type() {
                    DataType::Float16 => write_native_array_as_text!(v, Float16Array),
                    DataType::Float32 => write_native_array_as_text!(v, Float32Array),
                    DataType::Float64 => write_native_array_as_text!(v, Float64Array),
                    DataType::Int8 => write_native_array_as_text!(v, Int8Array),
                    DataType::Int16 => write_native_array_as_text!(v, Int16Array),
                    DataType::Int32 => write_native_array_as_text!(v, Int32Array),
                    DataType::Int64 => write_native_array_as_text!(v, Int64Array),
                    DataType::UInt8 => write_native_array_as_text!(v, UInt8Array),
                    DataType::UInt16 => write_native_array_as_text!(v, UInt16Array),
                    DataType::UInt32 => write_native_array_as_text!(v, UInt32Array),
                    DataType::UInt64 => write_native_array_as_text!(v, UInt64Array),
                    DataType::Boolean => write_native_array_as_text!(v, BooleanArray),
                    DataType::Utf8 => write_native_array_as_text!(v, StringArray),
                    DataType::LargeUtf8 => write_native_array_as_text!(v, LargeStringArray),
                    dt => unimplemented!("Unable to convert List of {} to string", dt),
                }

                "{".to_string() + &values.join(",") + "}"
            }
        }
    }
}

#[derive(Debug)]
pub struct DataFrame {
    columns: Vec<Column>,
    data: Vec<Row>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>, data: Vec<Row>) -> DataFrame {
        DataFrame { columns, data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_rows(&self) -> &Vec<Row> {
        &self.data
    }

    pub fn mut_rows(&mut self) -> &mut Vec<Row> {
        &mut self.data
    }

    pub fn into_rows(self) -> Vec<Row> {
        self.data
    }

    pub fn print(&self) -> String {
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");

        let mut header = vec![];
        for column in self.get_columns() {
            header.push(Cell::new(&column.get_name()));
        }
        table.set_header(header);

        for row in self.get_rows().iter() {
            let mut table_row = vec![];

            for value in row.values().iter() {
                table_row.push(value.to_string());
            }

            table.add_row(table_row);
        }

        table.trim_fmt()
    }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TimestampValue {
    unix_nano: i64,
    tz: Option<String>,
}

impl TimestampValue {
    pub fn new(mut unix_nano: i64, tz: Option<String>) -> TimestampValue {
        // This is a hack to workaround a mismatch between on-disk and in-memory representations.
        // We use millisecond precision on-disk.
        unix_nano -= unix_nano % 1000;
        TimestampValue { unix_nano, tz }
    }

    pub fn to_naive_datetime(&self) -> NaiveDateTime {
        assert!(self.tz.is_none());

        temporal_conversions::timestamp_ns_to_datetime(self.unix_nano)
    }

    pub fn to_fixed_datetime(&self) -> io::Result<DateTime<Tz>> {
        assert!(self.tz.is_some());

        let tz = self
            .tz
            .as_ref()
            .unwrap()
            .parse::<Tz>()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;

        let ndt = temporal_conversions::timestamp_ns_to_datetime(self.unix_nano);
        Ok(tz.from_utc_datetime(&ndt))
    }

    pub fn tz_ref(&self) -> &Option<String> {
        &self.tz
    }

    pub fn get_time_stamp(&self) -> i64 {
        self.unix_nano
    }
}

impl Debug for TimestampValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimestampValue")
            .field("unix_nano", &self.unix_nano)
            .field("tz", &self.tz)
            .field("str", &self.to_string())
            .finish()
    }
}

impl ToString for TimestampValue {
    fn to_string(&self) -> String {
        Utc.timestamp_nanos(self.unix_nano)
            .format_with_items(
                [
                    Item::Numeric(Year, Zero),
                    Item::Literal("-"),
                    Item::Numeric(Month, Zero),
                    Item::Literal("-"),
                    Item::Numeric(Day, Zero),
                    Item::Literal("T"),
                    Item::Numeric(Hour, Zero),
                    Item::Literal(":"),
                    Item::Numeric(Minute, Zero),
                    Item::Literal(":"),
                    Item::Numeric(Second, Zero),
                    Item::Fixed(Fixed::Nanosecond3),
                ]
                .iter(),
            )
            .to_string()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Decimal128Value {
    n: i128,
    // number of digits after .
    scale: usize,
}

impl Decimal128Value {
    pub fn new(n: i128, scale: usize) -> Self {
        Self { n, scale }
    }

    pub fn as_decimal(&self) -> Result<Decimal, CubeError> {
        Ok(Decimal::try_from_i128_with_scale(
            self.n,
            self.scale as u32,
        )?)
    }
}

impl ToString for Decimal128Value {
    fn to_string(&self) -> String {
        let as_str = self.n.to_string();

        if self.scale == 0 {
            as_str
        } else {
            let (sign, rest) = as_str.split_at(if self.n >= 0 { 0 } else { 1 });

            if rest.len() > self.scale {
                let (whole, decimal) = as_str.split_at(as_str.len() - self.scale);
                format!("{}.{}", whole, decimal)
            } else {
                // String has to be padded
                format!("{}0.{:0>w$}", sign, rest, w = self.scale)
            }
        }
    }
}

macro_rules! convert_array_cast_native {
    ($V: expr, (Vec<u8>)) => {{
        $V.to_vec()
    }};
    ($V: expr, $T: ty) => {{
        $V as $T
    }};
}

macro_rules! convert_array {
    ($ARRAY:expr, $NUM_ROWS:expr, $ROWS:expr, $ARRAY_TYPE: ident, $TABLE_TYPE: ident, $NATIVE: tt) => {{
        let a = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        for i in 0..$NUM_ROWS {
            $ROWS[i].push(if a.is_null(i) {
                TableValue::Null
            } else {
                TableValue::$TABLE_TYPE(convert_array_cast_native!(a.value(i), $NATIVE))
            });
        }
    }};
}

pub fn arrow_to_column_type(arrow_type: DataType) -> Result<ColumnType, CubeError> {
    match arrow_type {
        DataType::Binary => Ok(ColumnType::Blob),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnType::String),
        DataType::Timestamp(_, _) => Ok(ColumnType::String),
        DataType::Interval(_) => Ok(ColumnType::String),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok(ColumnType::Double),
        DataType::Boolean => Ok(ColumnType::Boolean),
        DataType::List(field) => Ok(ColumnType::List(field)),
        DataType::Int32 | DataType::UInt32 => Ok(ColumnType::Int32),
        DataType::Decimal(_, _) => Ok(ColumnType::Int32),
        DataType::Int8
        | DataType::Int16
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt64 => Ok(ColumnType::Int64),
        x => Err(CubeError::internal(format!("unsupported type {:?}", x))),
    }
}

pub fn batch_to_dataframe(batches: &Vec<RecordBatch>) -> Result<DataFrame, CubeError> {
    let mut cols = vec![];
    let mut all_rows = vec![];

    for batch in batches.iter() {
        if cols.is_empty() {
            let schema = batch.schema().clone();
            for (_i, field) in schema.fields().iter().enumerate() {
                cols.push(Column::new(
                    field.name().clone(),
                    arrow_to_column_type(field.data_type().clone())?,
                    ColumnFlags::empty(),
                ));
            }
        }
        if batch.num_rows() == 0 {
            continue;
        }
        let mut rows = vec![];

        for _ in 0..batch.num_rows() {
            rows.push(Row::new(Vec::with_capacity(batch.num_columns())));
        }

        for column_index in 0..batch.num_columns() {
            let array = batch.column(column_index);
            let num_rows = batch.num_rows();
            match array.data_type() {
                DataType::UInt16 => convert_array!(array, num_rows, rows, UInt16Array, Int16, i16),
                DataType::Int16 => convert_array!(array, num_rows, rows, Int16Array, Int16, i16),
                DataType::UInt32 => convert_array!(array, num_rows, rows, UInt32Array, Int32, i32),
                DataType::Int32 => convert_array!(array, num_rows, rows, Int32Array, Int32, i32),
                DataType::UInt64 => convert_array!(array, num_rows, rows, UInt64Array, Int64, i64),
                DataType::Int64 => convert_array!(array, num_rows, rows, Int64Array, Int64, i64),
                DataType::Boolean => {
                    convert_array!(array, num_rows, rows, BooleanArray, Boolean, bool)
                }
                DataType::Float32 => {
                    convert_array!(array, num_rows, rows, Float32Array, Float32, f32)
                }
                DataType::Float64 => {
                    convert_array!(array, num_rows, rows, Float64Array, Float64, f64)
                }
                DataType::Utf8 => {
                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::String(a.value(i).to_string())
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(
                                a.value(i) * 1000_i64,
                                tz.clone(),
                            ))
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(a.value(i), tz.clone()))
                        });
                    }
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalDayTimeArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        if let Some(as_str) = make_string_interval_day_time!(a, i) {
                            rows[i].push(TableValue::String(as_str));
                        } else {
                            rows[i].push(TableValue::Null);
                        }
                    }
                }
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalYearMonthArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        if let Some(as_str) = make_string_interval_year_month!(a, i) {
                            rows[i].push(TableValue::String(as_str));
                        } else {
                            rows[i].push(TableValue::Null);
                        }
                    }
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalMonthDayNanoArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        if let Some(as_str) = make_string_interval_month_day_nano!(a, i) {
                            rows[i].push(TableValue::String(as_str));
                        } else {
                            rows[i].push(TableValue::Null);
                        }
                    }
                }
                DataType::Decimal(_, s) => {
                    let a = array.as_any().downcast_ref::<DecimalArray>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Decimal128(Decimal128Value::new(a.value(i), *s))
                        });
                    }
                }
                DataType::List(_) => {
                    let a = array.as_any().downcast_ref::<ListArray>().unwrap();

                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::List(a.value(i))
                        });
                    }
                }
                x => panic!("Unsupported data type: {:?}", x),
            }
        }
        all_rows.append(&mut rows);
    }

    Ok(DataFrame::new(cols, all_rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dataframe_print() {
        let frame = DataFrame::new(
            vec![Column::new(
                "test".to_string(),
                ColumnType::String,
                ColumnFlags::empty(),
            )],
            vec![Row::new(vec![TableValue::String("simple_str".to_string())])],
        );

        assert_eq!(
            frame.print(),
            "+------------+\n\
            | test       |\n\
            +------------+\n\
            | simple_str |\n\
            +------------+"
        );
    }
}
