use chrono::prelude::*;
use comfy_table::{Cell, Table};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Date64Array, DecimalArray, Float16Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray,
        ListArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, IntervalUnit, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use rust_decimal::prelude::*;
use serde::{Serialize, Serializer};
use std::fmt::Debug;

use super::{ColumnFlags, ColumnType};
use crate::CubeError;

#[derive(Clone, Debug, Serialize)]
pub struct Column {
    name: String,
    column_type: ColumnType,
    #[serde(skip_serializing)]
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

#[derive(Debug, Serialize)]
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

    pub fn to_values(self) -> Vec<TableValue> {
        self.values
    }

    pub fn push(&mut self, val: TableValue) {
        self.values.push(val);
    }
}

// Type aliases for compatibility - actual implementations are in pg-srv
pub type IntervalValue = pg_srv::IntervalValue;
pub type TimestampValue = pg_srv::TimestampValue;

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
    List(ListValue),
    Decimal128(Decimal128Value),
    Date(NaiveDate),
    Timestamp(TimestampValue),
    Interval(IntervalValue),
}

impl Serialize for TableValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            TableValue::Null => serializer.serialize_none(),
            TableValue::Boolean(val) => serializer.serialize_bool(val),
            TableValue::String(ref val) => serializer.serialize_str(val),
            TableValue::Int16(val) => serializer.serialize_str(&val.to_string()),
            TableValue::Int32(val) => serializer.serialize_str(&val.to_string()),
            TableValue::Int64(val) => serializer.serialize_str(&val.to_string()),
            TableValue::Float32(val) => serializer.serialize_str(&val.to_string()),
            TableValue::Float64(val) => serializer.serialize_str(&val.to_string()),
            TableValue::Decimal128(ref val) => serializer.serialize_str(val.to_string().as_str()),
            TableValue::Timestamp(ref val) => serializer.serialize_str(val.to_string().as_str()),
            TableValue::Interval(ref val) => serializer.serialize_str(val.to_string().as_str()),
            TableValue::Date(ref val) => serializer.serialize_str(val.to_string().as_str()),
            TableValue::List(ref val) => serializer.serialize_str(val.to_string().as_str()),
        }
    }
}

impl ToString for TableValue {
    fn to_string(&self) -> String {
        match self {
            TableValue::Null => "NULL".to_string(),
            TableValue::String(v) => v.clone(),
            TableValue::Int16(v) => v.to_string(),
            TableValue::Int32(v) => v.to_string(),
            TableValue::Int64(v) => v.to_string(),
            TableValue::Boolean(v) => v.to_string(),
            TableValue::Float32(v) => v.to_string(),
            TableValue::Float64(v) => v.to_string(),
            TableValue::Date(v) => v.to_string(),
            TableValue::Timestamp(v) => v.to_string(),
            TableValue::Decimal128(v) => v.to_string(),
            TableValue::Interval(v) => v.to_string(),
            TableValue::List(v) => v.to_string(),
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

    pub fn to_rows(self) -> Vec<Row> {
        self.data
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

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct ListValue {
    pub v: ArrayRef,
}

impl ListValue {
    pub fn new(v: ArrayRef) -> Self {
        Self { v }
    }
}

impl ToString for ListValue {
    fn to_string(&self) -> String {
        let mut values: Vec<String> = Vec::with_capacity(self.v.len());

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

        match self.v.data_type() {
            DataType::Float16 => write_native_array_as_text!(self.v, Float16Array),
            DataType::Float32 => write_native_array_as_text!(self.v, Float32Array),
            DataType::Float64 => write_native_array_as_text!(self.v, Float64Array),
            DataType::Int8 => write_native_array_as_text!(self.v, Int8Array),
            DataType::Int16 => write_native_array_as_text!(self.v, Int16Array),
            DataType::Int32 => write_native_array_as_text!(self.v, Int32Array),
            DataType::Int64 => write_native_array_as_text!(self.v, Int64Array),
            DataType::UInt8 => write_native_array_as_text!(self.v, UInt8Array),
            DataType::UInt16 => write_native_array_as_text!(self.v, UInt16Array),
            DataType::UInt32 => write_native_array_as_text!(self.v, UInt32Array),
            DataType::UInt64 => write_native_array_as_text!(self.v, UInt64Array),
            DataType::Boolean => write_native_array_as_text!(self.v, BooleanArray),
            DataType::Utf8 => write_native_array_as_text!(self.v, StringArray),
            DataType::LargeUtf8 => write_native_array_as_text!(self.v, LargeStringArray),
            dt => unimplemented!("Unable to convert List of {} to string", dt),
        }

        "{".to_string() + &values.join(",") + "}"
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
        DataType::Date32 => Ok(ColumnType::Date(false)),
        DataType::Date64 => Ok(ColumnType::Date(true)),
        DataType::Timestamp(_, _) => Ok(ColumnType::Timestamp),
        DataType::Interval(unit) => Ok(ColumnType::Interval(unit)),
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
        DataType::Null => Ok(ColumnType::String),
        x => Err(CubeError::internal(format!("unsupported type {:?}", x))),
    }
}

pub fn batches_to_dataframe(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<DataFrame, CubeError> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    let mut all_rows = vec![];

    for field in schema.fields().iter() {
        cols.push(Column::new(
            field.name().clone(),
            arrow_to_column_type(field.data_type().clone())?,
            ColumnFlags::empty(),
        ));
    }

    for batch in batches.into_iter() {
        if batch.num_rows() == 0 {
            continue;
        }

        let mut rows = Vec::with_capacity(batch.num_rows());

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
                DataType::Date32 => {
                    let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Date(a.value_as_date(i).expect(
                                "value_as_date must return Option with NaiveDate for Date32Array",
                            ))
                        });
                    }
                }
                DataType::Date64 => {
                    let a = array.as_any().downcast_ref::<Date64Array>().unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Date(a.value_as_date(i).expect(
                                "value_as_date must return Option with NaiveDate for Date64Array",
                            ))
                        });
                    }
                }
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Timestamp(TimestampValue::new(
                                a.value(i) * 1_000_000_i64,
                                tz.clone(),
                            ))
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
                        if a.is_null(i) {
                            rows[i].push(TableValue::Null);
                        } else {
                            let value: u64 = a.value(i) as u64;
                            let days: i32 = ((value & 0xFFFFFFFF00000000) >> 32) as i32;
                            let milliseconds_part: i32 = (value & 0xFFFFFFFF) as i32;

                            let secs = milliseconds_part / 1000;
                            let milliseconds_remainder = milliseconds_part % 1000;
                            let mins = secs / 60;
                            let hours = mins / 60;

                            let secs = secs - (mins * 60);
                            let mins = mins - (hours * 60);

                            rows[i].push(TableValue::Interval(IntervalValue::new(
                                0,
                                days,
                                hours,
                                mins,
                                secs,
                                milliseconds_remainder * 1000,
                            )));
                        }
                    }
                }
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalYearMonthArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        if a.is_null(i) {
                            rows[i].push(TableValue::Null);
                        } else {
                            rows[i].push(TableValue::Interval(IntervalValue::new(
                                a.value(i),
                                0,
                                0,
                                0,
                                0,
                                0,
                            )));
                        }
                    }
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalMonthDayNanoArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        if a.is_null(i) {
                            rows[i].push(TableValue::Null);
                        } else {
                            let value: u128 = a.value(i) as u128;
                            let months: i32 =
                                ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
                            let days: i32 = ((value & 0xFFFFFFFF0000000000000000) >> 64) as i32;
                            let nanoseconds_part: i64 = (value & 0xFFFFFFFFFFFFFFFF) as i64;

                            let secs = nanoseconds_part / 1_000_000_000;
                            let secs_nano_fraction = (nanoseconds_part % 1_000_000_000) as i32;

                            let mins = secs / 60;
                            let hours = mins / 60;

                            let secs = secs - (mins * 60);
                            let mins = mins - (hours * 60);

                            let whole_usecs = secs_nano_fraction / 1000;
                            let nanos_remainder = secs_nano_fraction % 1000;

                            // Postgres supposedly believes in rounding to even.  Supposedly because they
                            // might also mix up fractional seconds with base-2 floating point, affecting
                            // microsecond rounding.
                            let usecs: i32;
                            if secs_nano_fraction < 0 {
                                usecs = whole_usecs
                                    - (nanos_remainder - (whole_usecs & 1) < -500) as i32;
                            } else {
                                usecs = whole_usecs
                                    + (nanos_remainder + (whole_usecs & 1) > 500) as i32;
                            }

                            rows[i].push(TableValue::Interval(IntervalValue::new(
                                months,
                                days,
                                hours as i32,
                                mins as i32,
                                secs as i32,
                                usecs,
                            )));
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
                            TableValue::List(ListValue::new(a.value(i)))
                        });
                    }
                }
                DataType::Null => {
                    for i in 0..num_rows {
                        rows[i].push(TableValue::Null)
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

    #[test]
    fn test_dataframe_tablevalue_serializer() {
        let frame = DataFrame::new(
            vec![
                Column::new(
                    "null_col".to_string(),
                    ColumnType::Boolean,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "bool_col".to_string(),
                    ColumnType::Boolean,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "string_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "int16_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "int32_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "int64_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "float32_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "float64_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "decimal128_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "timestamp_col".to_string(),
                    ColumnType::Timestamp,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "interval_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
                Column::new(
                    "date_col".to_string(),
                    ColumnType::String,
                    ColumnFlags::empty(),
                ),
            ],
            vec![Row::new(vec![
                TableValue::Null,
                TableValue::Boolean(true),
                TableValue::String("simple_str".to_string()),
                TableValue::Int16(123),
                TableValue::Int32(12345),
                TableValue::Int64(123456789),
                TableValue::Float32(1.23),
                TableValue::Float64(1.23456789),
                TableValue::Decimal128(Decimal128Value::new(123456789, 2)),
                TableValue::Timestamp(TimestampValue::new(737942400 * 1_000_000_000, None)),
                TableValue::Interval(IntervalValue::new(1, 2, 3, 4, 5, 6)),
                TableValue::Date(NaiveDate::from_ymd_opt(1993, 5, 21).unwrap()),
            ])],
        );

        insta::assert_snapshot!(
            "table_value_serializer",
            serde_json::to_string(&frame.data).unwrap()
        );
    }

    #[test]
    fn test_arrow_to_column_type() {
        let cases = vec![
            (DataType::Binary, ColumnType::Blob),
            (DataType::Utf8, ColumnType::String),
            (DataType::LargeUtf8, ColumnType::String),
            (DataType::Date32, ColumnType::Date(false)),
            (DataType::Date64, ColumnType::Date(true)),
            (
                DataType::Timestamp(TimeUnit::Second, None),
                ColumnType::Timestamp,
            ),
            (
                DataType::Interval(IntervalUnit::YearMonth),
                ColumnType::Interval(IntervalUnit::YearMonth),
            ),
            (DataType::Float16, ColumnType::Double),
            (DataType::Float32, ColumnType::Double),
            (DataType::Float64, ColumnType::Double),
            (DataType::Boolean, ColumnType::Boolean),
            (DataType::Int32, ColumnType::Int32),
            (DataType::UInt32, ColumnType::Int32),
            (DataType::Int8, ColumnType::Int64),
            (DataType::Int16, ColumnType::Int64),
            (DataType::Int64, ColumnType::Int64),
            (DataType::UInt8, ColumnType::Int64),
            (DataType::UInt16, ColumnType::Int64),
            (DataType::UInt64, ColumnType::Int64),
            (DataType::Null, ColumnType::String),
        ];

        for (arrow_type, expected_column_type) in cases {
            let result = arrow_to_column_type(arrow_type.clone()).unwrap();
            assert_eq!(result, expected_column_type, "Failed for {:?}", arrow_type);
        }
    }
}
