use crate::query_message_parser::ParseError;
use crate::query_result_transform::{DBResponsePrimitive, TIMESTAMP_ITEMS};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, StringArray, StringViewArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::NaiveDateTime;
use serde::Serialize;
use serde_json::Value;
use std::fmt::Display;

/// Borrowed view of one result cell. Serializes to exactly the same JSON as the
/// corresponding [`DBResponsePrimitive`], but string cells borrow their bytes
/// (from an Arrow buffer or a materialized primitive) instead of owning them,
/// so a serialize pass over an Arrow column does zero per-cell allocations.
#[derive(Debug, Clone)]
pub enum DBResponseValueRef<'a> {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Str(&'a str),
    /// Value rendered at read time (Arrow decimals). Owning is unavoidable here:
    /// the text doesn't exist in the source buffer.
    StringOwned(String),
    Timestamp(NaiveDateTime),
    Uncommon(&'a Value),
}

impl<'a> From<&'a DBResponsePrimitive> for DBResponseValueRef<'a> {
    fn from(value: &'a DBResponsePrimitive) -> Self {
        match value {
            DBResponsePrimitive::Null => DBResponseValueRef::Null,
            DBResponsePrimitive::Boolean(b) => DBResponseValueRef::Boolean(*b),
            DBResponsePrimitive::Int64(n) => DBResponseValueRef::Int64(*n),
            DBResponsePrimitive::UInt64(n) => DBResponseValueRef::UInt64(*n),
            DBResponsePrimitive::Float64(n) => DBResponseValueRef::Float64(*n),
            DBResponsePrimitive::String(s) => DBResponseValueRef::Str(s),
            DBResponsePrimitive::Timestamp(dt) => DBResponseValueRef::Timestamp(*dt),
            DBResponsePrimitive::Uncommon(v) => DBResponseValueRef::Uncommon(v),
        }
    }
}

impl DBResponseValueRef<'_> {
    pub fn into_primitive(self) -> DBResponsePrimitive {
        match self {
            DBResponseValueRef::Null => DBResponsePrimitive::Null,
            DBResponseValueRef::Boolean(b) => DBResponsePrimitive::Boolean(b),
            DBResponseValueRef::Int64(n) => DBResponsePrimitive::Int64(n),
            DBResponseValueRef::UInt64(n) => DBResponsePrimitive::UInt64(n),
            DBResponseValueRef::Float64(n) => DBResponsePrimitive::Float64(n),
            DBResponseValueRef::Str(s) => DBResponsePrimitive::String(s.to_owned()),
            DBResponseValueRef::StringOwned(s) => DBResponsePrimitive::String(s),
            DBResponseValueRef::Timestamp(dt) => DBResponsePrimitive::Timestamp(dt),
            DBResponseValueRef::Uncommon(v) => DBResponsePrimitive::Uncommon(v.clone()),
        }
    }
}

// Must stay in lockstep with `Serialize for DBResponsePrimitive`: numeric
// variants render as JSON strings, timestamps use the fixed ISO format.
impl Serialize for DBResponseValueRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            DBResponseValueRef::Null => serializer.serialize_none(),
            DBResponseValueRef::Boolean(b) => serializer.serialize_bool(*b),
            DBResponseValueRef::Int64(n) => serializer.collect_str(n),
            DBResponseValueRef::UInt64(n) => serializer.collect_str(n),
            DBResponseValueRef::Float64(n) => serializer.collect_str(n),
            DBResponseValueRef::Str(s) => serializer.serialize_str(s),
            DBResponseValueRef::StringOwned(s) => serializer.serialize_str(s),
            DBResponseValueRef::Timestamp(dt) => {
                serializer.collect_str(&dt.format_with_items(TIMESTAMP_ITEMS.iter()))
            }
            DBResponseValueRef::Uncommon(v) => v.serialize(serializer),
        }
    }
}

impl Display for DBResponseValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DBResponseValueRef::Null => f.write_str("null"),
            DBResponseValueRef::Boolean(b) => write!(f, "{}", b),
            DBResponseValueRef::Int64(n) => write!(f, "{}", n),
            DBResponseValueRef::UInt64(n) => write!(f, "{}", n),
            DBResponseValueRef::Float64(n) => write!(f, "{}", n),
            DBResponseValueRef::Str(s) => f.write_str(s),
            DBResponseValueRef::StringOwned(s) => f.write_str(s),
            DBResponseValueRef::Timestamp(dt) => {
                write!(f, "{}", dt.format_with_items(TIMESTAMP_ITEMS.iter()))
            }
            DBResponseValueRef::Uncommon(v) => {
                let s = serde_json::to_string(v).unwrap_or_else(|_| v.to_string());
                f.write_str(&s)
            }
        }
    }
}

// Mirrors `PartialEq for DBResponsePrimitive` by value: `Str` and `StringOwned`
// compare as the same string kind, everything else is variant-strict.
impl PartialEq for DBResponseValueRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        use DBResponseValueRef::*;
        match (self, other) {
            (Null, Null) => true,
            (Boolean(a), Boolean(b)) => a == b,
            (Int64(a), Int64(b)) => a == b,
            (UInt64(a), UInt64(b)) => a == b,
            (Float64(a), Float64(b)) => a == b,
            (Str(a), Str(b)) => a == b,
            (Str(a), StringOwned(b)) | (StringOwned(b), Str(a)) => *a == b.as_str(),
            (StringOwned(a), StringOwned(b)) => a == b,
            (Timestamp(a), Timestamp(b)) => a == b,
            (Uncommon(a), Uncommon(b)) => a == b,
            _ => false,
        }
    }
}

/// Format a decimal `mantissa` with `scale` fractional digits, stripping trailing
/// fractional zeros. Generic over the mantissa's `Display`, so it renders any Arrow
/// decimal width (`i32`/`i64`/`i128`/`i256`) directly — Decimal256 needs no fallback
/// to Arrow's own string conversion.
///
/// e.g. `(25987600, 5) -> "259.876"`, `(6199200000, 5) -> "61992"`,
/// `(-250, 3) -> "-0.25"`, `(25, 5) -> "0.00025"`.
pub fn decimal_to_string<T: std::fmt::Display>(mantissa: T, scale: u32) -> String {
    let raw = mantissa.to_string();
    if scale == 0 {
        return raw;
    }

    let scale = scale as usize;
    let (sign, digits) = match raw.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", raw.as_str()),
    };

    let (int_part, frac) = if digits.len() > scale {
        let (int_part, frac) = digits.split_at(digits.len() - scale);
        (int_part, frac.to_string())
    } else {
        let pad = "0".repeat(scale - digits.len());
        ("0", format!("{pad}{digits}"))
    };

    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        format!("{sign}{int_part}")
    } else {
        format!("{sign}{int_part}.{frac}")
    }
}

/// One logical result column backed by the Arrow arrays it arrived in (one
/// chunk per IPC record batch). Values are read straight out of the Arrow
/// buffers on demand — parsing and the columnar transform never materialize
/// per-cell primitives, and cloning the column is an `Arc` bump per chunk.
#[derive(Debug, Clone)]
pub struct ArrowColumn {
    chunks: Vec<ArrayRef>,
    len: usize,
}

impl ArrowColumn {
    /// Validates every chunk's data type up front, so downstream readers can
    /// assume the whole column is convertible.
    pub fn try_new(chunks: Vec<ArrayRef>) -> Result<Self, ParseError> {
        for chunk in &chunks {
            TypedChunk::try_new(chunk.as_ref())?;
        }
        let len = chunks.iter().map(|c| c.len()).sum();
        Ok(Self { chunks, len })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True if any chunk holds string values. `transform_value` only rewrites
    /// `String` cells (member type `"time"`), so a column without string chunks
    /// passes the transform untouched and can be shared as-is.
    pub fn has_string_values(&self) -> bool {
        self.chunks.iter().any(|c| {
            matches!(
                c.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
        })
    }

    pub fn iter_values(&self) -> impl Iterator<Item = DBResponseValueRef<'_>> + '_ {
        self.chunks.iter().flat_map(|chunk| {
            let typed = TypedChunk::try_new(chunk.as_ref())
                .expect("chunk types validated in ArrowColumn::try_new");
            (0..chunk.len()).map(move |i| typed.value(i))
        })
    }
}

/// Arrow array downcast once per chunk, so per-cell reads are a direct typed
/// access instead of a `data_type()` dispatch + `downcast_ref` per value.
#[derive(Clone, Copy)]
enum TypedChunk<'a> {
    Null,
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float16(&'a Float16Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Utf8View(&'a StringViewArray),
    Date32(&'a Date32Array),
    Date64(&'a Date64Array),
    TimestampSecond(&'a TimestampSecondArray),
    TimestampMillisecond(&'a TimestampMillisecondArray),
    TimestampMicrosecond(&'a TimestampMicrosecondArray),
    TimestampNanosecond(&'a TimestampNanosecondArray),
    Decimal128 {
        array: &'a Decimal128Array,
        scale: u32,
    },
    Decimal256 {
        array: &'a Decimal256Array,
        scale: u32,
    },
}

macro_rules! downcast_chunk {
    ($array:expr, $ty:ty) => {
        $array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
            ParseError::ArrowError(format!(
                "Failed to downcast Arrow array to {}",
                stringify!($ty)
            ))
        })?
    };
}

impl<'a> TypedChunk<'a> {
    fn try_new(array: &'a dyn Array) -> Result<Self, ParseError> {
        Ok(match array.data_type() {
            DataType::Null => TypedChunk::Null,
            DataType::Boolean => TypedChunk::Boolean(downcast_chunk!(array, BooleanArray)),
            DataType::Int8 => TypedChunk::Int8(downcast_chunk!(array, Int8Array)),
            DataType::Int16 => TypedChunk::Int16(downcast_chunk!(array, Int16Array)),
            DataType::Int32 => TypedChunk::Int32(downcast_chunk!(array, Int32Array)),
            DataType::Int64 => TypedChunk::Int64(downcast_chunk!(array, Int64Array)),
            DataType::UInt8 => TypedChunk::UInt8(downcast_chunk!(array, UInt8Array)),
            DataType::UInt16 => TypedChunk::UInt16(downcast_chunk!(array, UInt16Array)),
            DataType::UInt32 => TypedChunk::UInt32(downcast_chunk!(array, UInt32Array)),
            DataType::UInt64 => TypedChunk::UInt64(downcast_chunk!(array, UInt64Array)),
            DataType::Float16 => TypedChunk::Float16(downcast_chunk!(array, Float16Array)),
            DataType::Float32 => TypedChunk::Float32(downcast_chunk!(array, Float32Array)),
            DataType::Float64 => TypedChunk::Float64(downcast_chunk!(array, Float64Array)),
            DataType::Utf8 => TypedChunk::Utf8(downcast_chunk!(array, StringArray)),
            DataType::LargeUtf8 => TypedChunk::LargeUtf8(downcast_chunk!(array, LargeStringArray)),
            DataType::Utf8View => TypedChunk::Utf8View(downcast_chunk!(array, StringViewArray)),
            DataType::Date32 => TypedChunk::Date32(downcast_chunk!(array, Date32Array)),
            DataType::Date64 => TypedChunk::Date64(downcast_chunk!(array, Date64Array)),
            DataType::Timestamp(TimeUnit::Second, _) => {
                TypedChunk::TimestampSecond(downcast_chunk!(array, TimestampSecondArray))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                TypedChunk::TimestampMillisecond(downcast_chunk!(array, TimestampMillisecondArray))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                TypedChunk::TimestampMicrosecond(downcast_chunk!(array, TimestampMicrosecondArray))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                TypedChunk::TimestampNanosecond(downcast_chunk!(array, TimestampNanosecondArray))
            }
            DataType::Decimal128(_, scale) => TypedChunk::Decimal128 {
                array: downcast_chunk!(array, Decimal128Array),
                scale: (*scale).max(0) as u32,
            },
            DataType::Decimal256(_, scale) => TypedChunk::Decimal256 {
                array: downcast_chunk!(array, Decimal256Array),
                scale: (*scale).max(0) as u32,
            },
            other => return Err(ParseError::UnsupportedArrowType(format!("{other:?}"))),
        })
    }

    fn value(&self, i: usize) -> DBResponseValueRef<'a> {
        macro_rules! read {
            ($a:expr, $variant:ident, $conv:expr) => {{
                if $a.is_null(i) {
                    DBResponseValueRef::Null
                } else {
                    DBResponseValueRef::$variant($conv($a.value(i)))
                }
            }};
        }
        macro_rules! read_datetime {
            ($a:expr) => {{
                if $a.is_null(i) {
                    DBResponseValueRef::Null
                } else {
                    match $a.value_as_datetime(i) {
                        Some(dt) => DBResponseValueRef::Timestamp(dt),
                        None => DBResponseValueRef::Null,
                    }
                }
            }};
        }

        match self {
            TypedChunk::Null => DBResponseValueRef::Null,
            TypedChunk::Boolean(a) => read!(a, Boolean, |v| v),
            TypedChunk::Int8(a) => read!(a, Int64, |v| v as i64),
            TypedChunk::Int16(a) => read!(a, Int64, |v| v as i64),
            TypedChunk::Int32(a) => read!(a, Int64, |v| v as i64),
            TypedChunk::Int64(a) => read!(a, Int64, |v| v),
            TypedChunk::UInt8(a) => read!(a, UInt64, |v| v as u64),
            TypedChunk::UInt16(a) => read!(a, UInt64, |v| v as u64),
            TypedChunk::UInt32(a) => read!(a, UInt64, |v| v as u64),
            TypedChunk::UInt64(a) => read!(a, UInt64, |v| v),
            TypedChunk::Float16(a) => {
                if a.is_null(i) {
                    DBResponseValueRef::Null
                } else {
                    DBResponseValueRef::Float64(a.value(i).to_f64())
                }
            }
            TypedChunk::Float32(a) => read!(a, Float64, |v| v as f64),
            TypedChunk::Float64(a) => read!(a, Float64, |v| v),
            TypedChunk::Utf8(a) => read!(a, Str, |v| v),
            TypedChunk::LargeUtf8(a) => read!(a, Str, |v| v),
            TypedChunk::Utf8View(a) => read!(a, Str, |v| v),
            TypedChunk::Date32(a) => read_datetime!(a),
            TypedChunk::Date64(a) => read_datetime!(a),
            TypedChunk::TimestampSecond(a) => read_datetime!(a),
            TypedChunk::TimestampMillisecond(a) => read_datetime!(a),
            TypedChunk::TimestampMicrosecond(a) => read_datetime!(a),
            TypedChunk::TimestampNanosecond(a) => read_datetime!(a),
            TypedChunk::Decimal128 { array, scale } => {
                if array.is_null(i) {
                    DBResponseValueRef::Null
                } else {
                    DBResponseValueRef::StringOwned(decimal_to_string(array.value(i), *scale))
                }
            }
            TypedChunk::Decimal256 { array, scale } => {
                if array.is_null(i) {
                    DBResponseValueRef::Null
                } else {
                    DBResponseValueRef::StringOwned(decimal_to_string(array.value(i), *scale))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decimal_to_string() {
        for (mantissa, scale, expected) in [
            (6199200000i128, 5u32, "61992"),
            (25987600, 5, "259.876"),
            (1500, 3, "1.5"),
            (-250, 3, "-0.25"),
            (0, 5, "0"),
            (21098000, 5, "210.98"),
            (100, 0, "100"),
            (0, 0, "0"),
            (-5, 0, "-5"),
            (25, 5, "0.00025"),
            (-1, 0, "-1"),
            (i128::MAX, 0, "170141183460469231731687303715884105727"),
        ] {
            assert_eq!(
                decimal_to_string(mantissa, scale),
                expected,
                "mantissa={mantissa} scale={scale}"
            );
        }
    }
}
