use chrono::prelude::*;
use comfy_table::{Cell, Table};
use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Date32Array, Date64Array, DecimalArray, Float16Array,
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray,
        ListArray, PrimitiveArray, StringArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
        UInt8Array,
    },
    datatypes::{ArrowPrimitiveType, DataType, IntervalUnit, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use rust_decimal::prelude::*;
use serde::{Serialize, Serializer};
use std::fmt::{self, Debug};

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
            TableValue::Int16(val) => serializer.collect_str(&val),
            TableValue::Int32(val) => serializer.collect_str(&val),
            TableValue::Int64(val) => serializer.collect_str(&val),
            TableValue::Float32(val) => serializer.collect_str(&val),
            TableValue::Float64(val) => serializer.collect_str(&val),
            TableValue::Decimal128(ref val) => serializer.collect_str(val),
            TableValue::Timestamp(ref val) => serializer.collect_str(val),
            TableValue::Interval(ref val) => serializer.collect_str(val),
            TableValue::Date(ref val) => serializer.collect_str(val),
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

impl fmt::Display for Decimal128Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Parts are written one by one: format_args! padding is ~2x slower on this hot path
        const ZEROS: &str = "0000000000000000000000000000000000000000";

        let mut buf = itoa::Buffer::new();
        let digits = buf.format(self.n.unsigned_abs());

        if self.n < 0 {
            f.write_str("-")?;
        }

        if digits.len() > self.scale {
            let (whole, fraction) = digits.split_at(digits.len() - self.scale);
            f.write_str(whole)?;
            if !fraction.is_empty() {
                f.write_str(".")?;
                f.write_str(fraction)?;
            }
            Ok(())
        } else {
            let padding = self.scale - digits.len();
            f.write_str("0.")?;
            for _ in 0..padding / ZEROS.len() {
                f.write_str(ZEROS)?;
            }
            f.write_str(&ZEROS[..padding % ZEROS.len()])?;
            f.write_str(digits)
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
        x => Err(CubeError::unsupported(format!("unsupported type {:?}", x))),
    }
}

fn interval_year_month_value(months: i32) -> IntervalValue {
    IntervalValue::new(months, 0, 0, 0, 0, 0)
}

fn interval_day_time_value(value: i64) -> IntervalValue {
    let value = value as u64;
    let days: i32 = ((value & 0xFFFFFFFF00000000) >> 32) as i32;
    let milliseconds_part: i32 = (value & 0xFFFFFFFF) as i32;

    let secs = milliseconds_part / 1000;
    let milliseconds_remainder = milliseconds_part % 1000;
    let mins = secs / 60;
    let hours = mins / 60;

    let secs = secs - (mins * 60);
    let mins = mins - (hours * 60);

    IntervalValue::new(0, days, hours, mins, secs, milliseconds_remainder * 1000)
}

fn interval_month_day_nano_value(value: i128) -> IntervalValue {
    let value = value as u128;
    let months: i32 = ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
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
    let usecs: i32 = if secs_nano_fraction < 0 {
        whole_usecs - (nanos_remainder - (whole_usecs & 1) < -500) as i32
    } else {
        whole_usecs + (nanos_remainder + (whole_usecs & 1) > 500) as i32
    };

    IntervalValue::new(months, days, hours as i32, mins as i32, secs as i32, usecs)
}

/// Serializes an Arrow array as a JSON array straight from its buffers, with the
/// same value for every cell as `batches_to_dataframe` + `TableValue::serialize`
/// would produce, but without materializing rows.
pub struct ArrowColumn<'a>(pub &'a ArrayRef);

macro_rules! serialize_column {
    ($SERIALIZER:expr, $ARRAY:expr, $ARRAY_TYPE:ident, |$A:ident, $I:ident| $VALUE:expr) => {{
        let $A = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        if $A.null_count() == 0 {
            $SERIALIZER.collect_seq((0..$A.len()).map(|$I| $VALUE))
        } else {
            $SERIALIZER.collect_seq((0..$A.len()).map(|$I| {
                if $A.is_null($I) {
                    None
                } else {
                    Some($VALUE)
                }
            }))
        }
    }};
}

/// An integer as a JSON string, like `TableValue::Int*`, formatted with `itoa`
/// rather than through `collect_str`'s `fmt::Write` adapter.
struct IntAsStr(i64);

impl Serialize for IntAsStr {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(itoa::Buffer::new().format(self.0))
    }
}

/// A float as a JSON string holding exactly its `Display` text, which is what
/// `TableValue` sends, with the common shapes written without float formatting:
///
/// - A whole number below 2^53 (2^24 for `f32`) is exact as an integer, and `Display`
///   prints it without a fraction; ids and counts arrive as such floats.
/// - A value that is the float nearest to `k / 1000` (money, rates) prints as that
///   decimal with trailing zeros trimmed: `k / 1000` round-trips, and while the float
///   spacing stays below `1e-3` no other decimal with as few digits is as close, so it
///   is the shortest round-trip form `Display` picks.
///
/// `-0.0` keeps its sign in `Display` and, like everything else, takes `Display`.
struct FloatAsStr<T>(T);

trait FastFloat: fmt::Display + Copy {
    /// `Some(k)` when the value is the float nearest to `k / 10^scale` for one of the
    /// shapes above (`scale` 0 or 3).
    fn as_decimal(self) -> Option<(i64, u32)>;
}

macro_rules! impl_fast_float {
    ($T:ty, $EXACT_INT:expr, $MILLI_LIMIT:expr) => {
        impl FastFloat for $T {
            fn as_decimal(self) -> Option<(i64, u32)> {
                if self == 0.0 && self.is_sign_negative() {
                    return None;
                }
                let abs = self.abs();
                if abs < $EXACT_INT && self.fract() == 0.0 {
                    return Some((self as i64, 0));
                }
                if abs < $MILLI_LIMIT {
                    let k = (self * 1000.0).round();
                    if k / 1000.0 == self {
                        return Some((k as i64, 3));
                    }
                }
                None
            }
        }
    };
}

// The spacing of floats stays below 1e-3 under 2^52 * 1e-3 for f64 and 2^23 * 1e-3 for f32.
impl_fast_float!(f64, 9_007_199_254_740_992.0, 1e12);
impl_fast_float!(f32, 16_777_216.0, 8000.0);

impl<T: FastFloat> Serialize for FloatAsStr<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0.as_decimal() {
            Some((int, 0)) => IntAsStr(int).serialize(serializer),
            Some((milli, _)) => {
                let mut out = [0u8; 24];
                let mut w = 0;
                if milli < 0 {
                    out[0] = b'-';
                    w = 1;
                }
                let milli = milli.unsigned_abs();
                let mut int = itoa::Buffer::new();
                let int = int.format(milli / 1000).as_bytes();
                out[w..w + int.len()].copy_from_slice(int);
                w += int.len();
                let mut frac = milli % 1000;
                if frac != 0 {
                    let mut digits = 3;
                    while frac % 10 == 0 {
                        frac /= 10;
                        digits -= 1;
                    }
                    out[w] = b'.';
                    for i in (0..digits).rev() {
                        out[w + 1 + i] = b'0' + (frac % 10) as u8;
                        frac /= 10;
                    }
                    w += 1 + digits;
                }
                // Only ASCII digits, `-` and `.`.
                serializer.serialize_str(std::str::from_utf8(&out[..w]).unwrap())
            }
            None => serializer.collect_str(&self.0),
        }
    }
}

/// A timestamp column as `TableValue` writes timestamps (`TimestampValue`'s `Display`:
/// UTC, truncated to microseconds toward zero, milliseconds shown, zone not printed).
/// Neighbouring rows usually share a day, so the date is formatted once per day
/// rather than per cell, and the time of day is plain integer math.
struct TimestampColumn<'a, T: ArrowPrimitiveType<Native = i64>> {
    array: &'a PrimitiveArray<T>,
    nanos_per_unit: i64,
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;

impl<T: ArrowPrimitiveType<Native = i64>> Serialize for TimestampColumn<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let date = std::cell::Cell::new((i64::MIN, [0u8; 10]));
        let cell = |i: usize| TimestampCell {
            nanos: self.array.value(i) * self.nanos_per_unit,
            date: &date,
        };
        if self.array.null_count() == 0 {
            serializer.collect_seq((0..self.array.len()).map(cell))
        } else {
            serializer.collect_seq(
                (0..self.array.len()).map(|i| (!self.array.is_null(i)).then(|| cell(i))),
            )
        }
    }
}

struct TimestampCell<'a> {
    nanos: i64,
    /// The last formatted day and its `YYYY-MM-DD`.
    date: &'a std::cell::Cell<(i64, [u8; 10])>,
}

impl Serialize for TimestampCell<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        fn put(buf: &mut [u8], mut value: u64) {
            for digit in buf.iter_mut().rev() {
                *digit = b'0' + (value % 10) as u8;
                value /= 10;
            }
        }

        let nanos = self.nanos - self.nanos % 1000;
        let day = nanos.div_euclid(NANOS_PER_DAY);
        let of_day = nanos.rem_euclid(NANOS_PER_DAY) as u64;

        let (cached_day, mut date) = self.date.get();
        if cached_day != day {
            // Not from `day * NANOS_PER_DAY`: that overflows for the earliest day.
            let ndt = Utc.timestamp_nanos(nanos);
            // i64 nanoseconds span 1677..=2262, so the year is always 4 digits.
            put(&mut date[0..4], ndt.year() as u64);
            date[4] = b'-';
            put(&mut date[5..7], ndt.month() as u64);
            date[7] = b'-';
            put(&mut date[8..10], ndt.day() as u64);
            self.date.set((day, date));
        }

        let secs = of_day / 1_000_000_000;
        let mut out = *b"0000-00-00T00:00:00.000";
        out[..10].copy_from_slice(&date);
        put(&mut out[11..13], secs / 3600);
        put(&mut out[14..16], secs / 60 % 60);
        put(&mut out[17..19], secs % 60);
        put(&mut out[20..23], of_day % 1_000_000_000 / 1_000_000);

        // Only ASCII digits and separators.
        serializer.serialize_str(std::str::from_utf8(&out).unwrap())
    }
}

impl Serialize for ArrowColumn<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let array = self.0;
        match array.data_type() {
            DataType::UInt16 => serialize_column!(serializer, array, UInt16Array, |a, i| {
                IntAsStr((a.value(i) as i16).into())
            }),
            DataType::Int16 => serialize_column!(serializer, array, Int16Array, |a, i| {
                IntAsStr(a.value(i).into())
            }),
            DataType::UInt32 => serialize_column!(serializer, array, UInt32Array, |a, i| {
                IntAsStr((a.value(i) as i32).into())
            }),
            DataType::Int32 => serialize_column!(serializer, array, Int32Array, |a, i| {
                IntAsStr(a.value(i).into())
            }),
            DataType::UInt64 => serialize_column!(serializer, array, UInt64Array, |a, i| {
                IntAsStr(a.value(i) as i64)
            }),
            DataType::Int64 => serialize_column!(serializer, array, Int64Array, |a, i| {
                IntAsStr(a.value(i))
            }),
            DataType::Float32 => serialize_column!(serializer, array, Float32Array, |a, i| {
                FloatAsStr(a.value(i))
            }),
            DataType::Float64 => serialize_column!(serializer, array, Float64Array, |a, i| {
                FloatAsStr(a.value(i))
            }),
            // `Option<bool>` / `Option<&str>` serialize exactly like `TableValue::Boolean`
            // / `TableValue::String`, without the per-cell `String` copy the latter needs.
            DataType::Boolean => serializer.collect_seq(
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter(),
            ),
            DataType::Utf8 => {
                serializer.collect_seq(array.as_any().downcast_ref::<StringArray>().unwrap().iter())
            }
            DataType::Date32 => serialize_column!(serializer, array, Date32Array, |a, i| {
                TableValue::Date(
                    a.value_as_date(i)
                        .expect("value_as_date must return Option with NaiveDate for Date32Array"),
                )
            }),
            DataType::Date64 => serialize_column!(serializer, array, Date64Array, |a, i| {
                TableValue::Date(
                    a.value_as_date(i)
                        .expect("value_as_date must return Option with NaiveDate for Date64Array"),
                )
            }),
            DataType::Timestamp(TimeUnit::Millisecond, _) => TimestampColumn {
                array: array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap(),
                nanos_per_unit: 1_000_000,
            }
            .serialize(serializer),
            DataType::Timestamp(TimeUnit::Microsecond, _) => TimestampColumn {
                array: array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap(),
                nanos_per_unit: 1000,
            }
            .serialize(serializer),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => TimestampColumn {
                array: array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap(),
                nanos_per_unit: 1,
            }
            .serialize(serializer),
            DataType::Interval(IntervalUnit::DayTime) => {
                serialize_column!(serializer, array, IntervalDayTimeArray, |a, i| {
                    TableValue::Interval(interval_day_time_value(a.value(i)))
                })
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                serialize_column!(serializer, array, IntervalYearMonthArray, |a, i| {
                    TableValue::Interval(interval_year_month_value(a.value(i)))
                })
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                serialize_column!(serializer, array, IntervalMonthDayNanoArray, |a, i| {
                    TableValue::Interval(interval_month_day_nano_value(a.value(i)))
                })
            }
            DataType::Decimal(_, s) => {
                serialize_column!(serializer, array, DecimalArray, |a, i| {
                    TableValue::Decimal128(Decimal128Value::new(a.value(i), *s))
                })
            }
            DataType::List(_) => serialize_column!(serializer, array, ListArray, |a, i| {
                TableValue::List(ListValue::new(a.value(i)))
            }),
            DataType::Null => serializer.collect_seq((0..array.len()).map(|_| ())),
            x => Err(serde::ser::Error::custom(format!(
                "Unsupported data type: {:?}",
                x
            ))),
        }
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
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Interval(interval_day_time_value(a.value(i)))
                        });
                    }
                }
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalYearMonthArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Interval(interval_year_month_value(a.value(i)))
                        });
                    }
                }
                DataType::Interval(IntervalUnit::MonthDayNano) => {
                    let a = array
                        .as_any()
                        .downcast_ref::<IntervalMonthDayNanoArray>()
                        .unwrap();
                    for i in 0..num_rows {
                        rows[i].push(if a.is_null(i) {
                            TableValue::Null
                        } else {
                            TableValue::Interval(interval_month_day_nano_value(a.value(i)))
                        });
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
    fn test_decimal128_value_display() {
        assert_eq!(Decimal128Value::new(0, 0).to_string(), "0");
        assert_eq!(Decimal128Value::new(0, 2).to_string(), "0.00");
        assert_eq!(Decimal128Value::new(5, 0).to_string(), "5");
        assert_eq!(Decimal128Value::new(-5, 0).to_string(), "-5");
        assert_eq!(Decimal128Value::new(1, 5).to_string(), "0.00001");
        assert_eq!(Decimal128Value::new(-1, 5).to_string(), "-0.00001");
        assert_eq!(Decimal128Value::new(123456789, 2).to_string(), "1234567.89");
        assert_eq!(
            Decimal128Value::new(-123456789, 2).to_string(),
            "-1234567.89"
        );
        assert_eq!(Decimal128Value::new(123, 3).to_string(), "0.123");
        assert_eq!(Decimal128Value::new(-123, 3).to_string(), "-0.123");
        assert_eq!(Decimal128Value::new(1000, 3).to_string(), "1.000");
        assert_eq!(Decimal128Value::new(-1000, 3).to_string(), "-1.000");

        assert_eq!(
            Decimal128Value::new(i128::MAX, 0).to_string(),
            "170141183460469231731687303715884105727"
        );
        assert_eq!(
            Decimal128Value::new(i128::MAX, 38).to_string(),
            "1.70141183460469231731687303715884105727"
        );
        assert_eq!(
            Decimal128Value::new(i128::MAX, 39).to_string(),
            "0.170141183460469231731687303715884105727"
        );
        assert_eq!(
            Decimal128Value::new(i128::MAX, 45).to_string(),
            "0.000000170141183460469231731687303715884105727"
        );
        assert_eq!(
            Decimal128Value::new(i128::MIN, 0).to_string(),
            "-170141183460469231731687303715884105728"
        );
        assert_eq!(
            Decimal128Value::new(i128::MIN, 39).to_string(),
            "-0.170141183460469231731687303715884105728"
        );
        assert_eq!(
            Decimal128Value::new(u64::MAX as i128, 0).to_string(),
            "18446744073709551615"
        );
        assert_eq!(
            Decimal128Value::new(u64::MAX as i128 + 1, 20).to_string(),
            "0.18446744073709551616"
        );
        assert_eq!(
            Decimal128Value::new(-(u64::MAX as i128) - 1, 2).to_string(),
            "-184467440737095516.16"
        );
        assert_eq!(
            Decimal128Value::new(1, 41).to_string(),
            format!("0.{}1", "0".repeat(40))
        );
        assert_eq!(
            Decimal128Value::new(-1, 100).to_string(),
            format!("-0.{}1", "0".repeat(99))
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

    #[test]
    fn test_arrow_column_matches_dataframe_rows() {
        use datafusion::arrow::{
            array::DecimalBuilder,
            datatypes::{Field, Int64Type},
        };
        use std::sync::Arc;

        let mut decimal = DecimalBuilder::new(2, 10, 2);
        decimal.append_value(-12345).unwrap();
        decimal.append_null().unwrap();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt16Array::from(vec![Some(65535), None])),
            Arc::new(Int16Array::from(vec![Some(-7), None])),
            Arc::new(UInt32Array::from(vec![Some(7), None])),
            Arc::new(Int32Array::from(vec![Some(-70000), None])),
            Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None])),
            Arc::new(Float32Array::from(vec![Some(1.25), None])),
            Arc::new(Float64Array::from(vec![Some(0.1 + 0.2), None])),
            Arc::new(BooleanArray::from(vec![Some(false), None])),
            Arc::new(StringArray::from(vec![Some("a\"b\n"), None])),
            Arc::new(Date32Array::from(vec![Some(8541), None])),
            Arc::new(Date64Array::from(vec![Some(737942400000), None])),
            Arc::new(TimestampMillisecondArray::from_opt_vec(
                vec![Some(737942400123), None],
                None,
            )),
            Arc::new(TimestampMicrosecondArray::from_opt_vec(
                vec![Some(737942400123456), None],
                None,
            )),
            Arc::new(TimestampNanosecondArray::from_opt_vec(
                vec![Some(737942400123456789), None],
                Some("UTC".to_string()),
            )),
            Arc::new(IntervalYearMonthArray::from(vec![Some(14), None])),
            Arc::new(IntervalDayTimeArray::from(vec![
                Some((3_i64 << 32) | 3_723_004),
                None,
            ])),
            Arc::new(IntervalMonthDayNanoArray::from(vec![
                Some((1_i128 << 96) | (2_i128 << 64) | 3_723_000_000_500),
                None,
            ])),
            Arc::new(decimal.finish()),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1), None]),
                None,
            ])),
            Arc::new(datafusion::arrow::array::NullArray::new(2)),
        ];
        // Edge values of the types `ArrowColumn` formats by hand rather than through
        // `TableValue`.
        let edge_columns: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                Some(f32::NAN),
                Some(f32::NEG_INFINITY),
                Some(-0.0),
                Some(f32::MAX),
                Some(1e-7),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1e300),
                Some(-1e-300),
                Some(f64::INFINITY),
                Some(f64::MIN_POSITIVE),
                Some(123456789.0),
            ])),
            Arc::new(TimestampNanosecondArray::from_opt_vec(
                vec![
                    Some(-1),
                    Some(-1_500_000_000),
                    Some(i64::MIN),
                    Some(i64::MAX),
                    Some(0),
                ],
                None,
            )),
            Arc::new(TimestampMillisecondArray::from_opt_vec(
                vec![
                    Some(-1),
                    Some(-86_400_001),
                    Some(9_223_372_036_853),
                    Some(-9_223_372_036_854),
                    None,
                ],
                Some("+05:00".to_string()),
            )),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                Some(i64::MAX),
                Some(0),
                Some(-1),
                None,
            ])),
        ];

        for columns in [columns, edge_columns] {
            let schema = Arc::new(Schema::new(
                columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| Field::new(&format!("c{}", i), c.data_type().clone(), true))
                    .collect(),
            ));
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

            let frame = batches_to_dataframe(&schema, vec![batch.clone()]).unwrap();
            for (idx, column) in batch.columns().iter().enumerate() {
                let expected: Vec<&TableValue> =
                    frame.get_rows().iter().map(|r| &r.values()[idx]).collect();

                assert_eq!(
                    serde_json::to_string(&ArrowColumn(column)).unwrap(),
                    serde_json::to_string(&expected).unwrap(),
                    "column {} ({:?})",
                    idx,
                    column.data_type()
                );
            }
        }
    }

    #[test]
    fn test_float_as_str_matches_display() {
        use rand::{Rng, SeedableRng};

        fn check<T: FastFloat + fmt::Debug>(v: T) {
            assert_eq!(
                serde_json::to_string(&FloatAsStr(v)).unwrap(),
                serde_json::to_string(&v.to_string()).unwrap(),
                "{:?}",
                v
            );
        }

        for v in [
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            0.3,
            0.1 + 0.2,
            1e-7,
            1e15,
            1e16,
            1.5e16,
            123456789.0,
            1e21,
            1e22,
            1e23,
            1e-300,
            1e300,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            f64::EPSILON,
            5e-324,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            562949953421312.25,
            9007199254740991.0,
            9007199254740992.0,
            -9007199254740992.0,
            1e16,
        ] {
            check(v);
        }
        for v in [
            0.0f32,
            -0.0,
            1.0,
            0.1,
            1e-7,
            1e10,
            f32::MAX,
            f32::MIN_POSITIVE,
            1e-45,
            1557507.25,
            16777215.0,
            16777216.0,
            -16777216.0,
        ] {
            check(v);
        }

        let mut rng = rand::rngs::StdRng::seed_from_u64(12380);
        for _ in 0..2_000_000 {
            check(f64::from_bits(rng.gen()));
            check(f32::from_bits(rng.gen()));
        }
        // Money- and count-like values, the common case, and whole numbers around
        // the limits of exact integers.
        for i in 0..1_000_000i64 {
            check(i as f64 / 100.0);
            check(-(i as f64) / 1000.0);
            check(i as f64 / 10.0 + 1e9);
            check((i * 104729) as f64 / 1000.0);
            check(i as f64 / 1000.0 + 999_999_000_000.0);
            check(i as f32 / 1000.0);
            check(-(i as f32) / 100.0);
            check((i * 7919) as f64);
            check(-(i as f64));
            check(i as f32 / 100.0);
            check(i as f32);
            check((1i64 << 53) as f64 + (i - 500_000) as f64 * 2.0);
            check((1i64 << 24) as f32 + (i % 1000 - 500) as f32 * 2.0);
        }
    }

    #[test]
    fn test_timestamp_column_matches_timestamp_value() {
        use rand::{Rng, SeedableRng};

        let mut rng = rand::rngs::StdRng::seed_from_u64(12503);
        // Runs of nearby values reuse the cached date; jumps and nulls break the runs.
        let mut nanos = Vec::new();
        let mut t: i64 = rng.gen_range(-5_000_000_000_000_000_000..5_000_000_000_000_000_000);
        for _ in 0..200_000 {
            match rng.gen_range(0..10) {
                0 => nanos.push(None),
                1 => {
                    t = rng.gen_range(-9_000_000_000_000_000_000..9_000_000_000_000_000_000);
                    nanos.push(Some(t));
                }
                _ => {
                    t = t.saturating_add(rng.gen_range(-3_600_000_000_000..3_600_000_000_000));
                    nanos.push(Some(t));
                }
            }
        }

        let expected = |scale: i64| -> String {
            let values: Vec<TableValue> = nanos
                .iter()
                .map(|v| match v {
                    Some(v) => TableValue::Timestamp(TimestampValue::new(v / scale * scale, None)),
                    None => TableValue::Null,
                })
                .collect();
            serde_json::to_string(&values).unwrap()
        };
        let scaled = |scale: i64| -> Vec<Option<i64>> {
            nanos.iter().map(|v| v.map(|v| v / scale)).collect()
        };

        let ns: ArrayRef =
            std::sync::Arc::new(TimestampNanosecondArray::from_opt_vec(scaled(1), None));
        let us: ArrayRef =
            std::sync::Arc::new(TimestampMicrosecondArray::from_opt_vec(scaled(1000), None));
        let ms: ArrayRef = std::sync::Arc::new(TimestampMillisecondArray::from_opt_vec(
            scaled(1_000_000),
            None,
        ));
        assert_eq!(
            serde_json::to_string(&ArrowColumn(&ns)).unwrap(),
            expected(1)
        );
        assert_eq!(
            serde_json::to_string(&ArrowColumn(&us)).unwrap(),
            expected(1000)
        );
        assert_eq!(
            serde_json::to_string(&ArrowColumn(&ms)).unwrap(),
            expected(1_000_000)
        );
    }
}
