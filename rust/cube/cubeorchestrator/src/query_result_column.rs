//! Column storage for [`crate::query_message_parser::QueryResult`].
//!
//! A column arrives either already materialized as [`DBResponsePrimitive`] cells
//! (the legacy CubeStore `HttpResultSet` and the JS→Rust columnar transport) or as
//! Arrow memory straight off the wire (`HttpQueryResultArrow`). Arrow columns are
//! kept as-is and decoded per cell on read, so a result set is never materialized
//! just to be re-cloned into the transform output.

use crate::{
    query_message_parser::ParseError,
    query_result_transform::{
        is_identity_transform, transform_value, ColumnarArray, DBResponsePrimitive,
    },
};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeStringArray, PrimitiveArray, StringArray, StringViewArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{ArrowTemporalType, DataType, TimeUnit};
use std::{borrow::Cow, convert::Infallible};

/// One logical column of a query result.
#[derive(Debug, Clone)]
pub enum QueryResultColumn {
    /// Cells already materialized as primitives: legacy `HttpResultSet` rows and
    /// `JsRawColumnarData` coming from the JS drivers.
    Columnar(ColumnarArray),
    /// Cells still held in Arrow memory, decoded on read.
    Arrow(ArrowArray),
}

impl QueryResultColumn {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            QueryResultColumn::Columnar(c) => c.len(),
            QueryResultColumn::Arrow(a) => a.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Per-cell accessor for this column. Resolving it once per column keeps the
    /// Arrow type dispatch out of the per-cell path.
    pub fn reader(&self) -> Result<ColumnReader<'_>, ParseError> {
        match self {
            QueryResultColumn::Columnar(c) => Ok(ColumnReader::Primitives(c.as_slice())),
            QueryResultColumn::Arrow(a) => Ok(ColumnReader::Arrow(a.cell_reader()?)),
        }
    }

    /// Materialize the column as primitives. Only for callers that genuinely need
    /// an owned slice — the read paths use [`QueryResultColumn::reader`] instead.
    pub fn to_columnar(&self) -> Result<ColumnarArray, ParseError> {
        match self {
            QueryResultColumn::Columnar(c) => Ok(c.clone()),
            QueryResultColumn::Arrow(a) => a.to_columnar(),
        }
    }
}

impl From<ColumnarArray> for QueryResultColumn {
    #[inline]
    fn from(c: ColumnarArray) -> Self {
        QueryResultColumn::Columnar(c)
    }
}

impl From<Vec<DBResponsePrimitive>> for QueryResultColumn {
    #[inline]
    fn from(v: Vec<DBResponsePrimitive>) -> Self {
        QueryResultColumn::Columnar(ColumnarArray::from(v))
    }
}

impl From<ArrowArray> for QueryResultColumn {
    #[inline]
    fn from(a: ArrowArray) -> Self {
        QueryResultColumn::Arrow(a)
    }
}

/// A single logical column backed by Arrow memory.
#[derive(Debug, Clone)]
pub struct ArrowArray(ArrayRef);

impl ArrowArray {
    /// Wrap an Arrow array, rejecting types the cell reader cannot decode.
    ///
    /// Validation goes through [`ArrowArray::cell_reader`] so the set of supported
    /// types has a single definition, and an unsupported type is reported while
    /// parsing rather than halfway through a transform. Empty arrays are accepted
    /// whatever their type: no cell is ever read, matching the previous behaviour
    /// where a zero-row column of an unsupported type parsed fine because the
    /// per-cell loop never ran.
    pub fn try_new(array: ArrayRef) -> Result<Self, ParseError> {
        let this = Self(array);
        this.cell_reader()?;
        Ok(this)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn data_type(&self) -> &DataType {
        self.0.data_type()
    }

    #[inline]
    pub fn array(&self) -> &ArrayRef {
        &self.0
    }

    pub fn to_columnar(&self) -> Result<ColumnarArray, ParseError> {
        let mut out = ColumnarArray::with_capacity(self.len());
        // `member_type` is empty: materializing must not apply the `time` reformat
        // that a transform would.
        let Ok(()) = self
            .cell_reader()?
            .for_each_transformed::<Infallible>("", |value| {
                out.push(value.into_owned());
                Ok(())
            });
        Ok(out)
    }

    /// Downcast to the concrete Arrow array once, for the whole column.
    fn cell_reader(&self) -> Result<ArrowCellReader<'_>, ParseError> {
        let array = self.0.as_ref();

        if array.is_empty() {
            return Ok(ArrowCellReader::Empty);
        }

        macro_rules! downcast {
            ($variant:ident, $ty:ty) => {{
                let a = array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                    ParseError::ArrowError(format!(
                        "Failed to downcast Arrow array to {}",
                        stringify!($ty)
                    ))
                })?;
                ArrowCellReader::$variant(a)
            }};
        }

        macro_rules! downcast_decimal {
            ($variant:ident, $ty:ty) => {{
                let a = array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                    ParseError::ArrowError(format!(
                        "Failed to downcast Arrow array to {}",
                        stringify!($ty)
                    ))
                })?;
                ArrowCellReader::$variant(a, a.scale().max(0) as u32)
            }};
        }

        let reader = match array.data_type() {
            DataType::Null => ArrowCellReader::Null(array.len()),
            DataType::Boolean => downcast!(Boolean, BooleanArray),
            DataType::Int8 => downcast!(Int8, Int8Array),
            DataType::Int16 => downcast!(Int16, Int16Array),
            DataType::Int32 => downcast!(Int32, Int32Array),
            DataType::Int64 => downcast!(Int64, Int64Array),
            DataType::UInt8 => downcast!(UInt8, UInt8Array),
            DataType::UInt16 => downcast!(UInt16, UInt16Array),
            DataType::UInt32 => downcast!(UInt32, UInt32Array),
            DataType::UInt64 => downcast!(UInt64, UInt64Array),
            DataType::Float16 => downcast!(Float16, Float16Array),
            DataType::Float32 => downcast!(Float32, Float32Array),
            DataType::Float64 => downcast!(Float64, Float64Array),
            DataType::Utf8 => downcast!(Utf8, StringArray),
            DataType::LargeUtf8 => downcast!(LargeUtf8, LargeStringArray),
            DataType::Utf8View => downcast!(Utf8View, StringViewArray),
            DataType::Date32 => downcast!(Date32, Date32Array),
            DataType::Date64 => downcast!(Date64, Date64Array),
            DataType::Timestamp(TimeUnit::Second, _) => {
                downcast!(TimestampSecond, TimestampSecondArray)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                downcast!(TimestampMillisecond, TimestampMillisecondArray)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                downcast!(TimestampMicrosecond, TimestampMicrosecondArray)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                downcast!(TimestampNanosecond, TimestampNanosecondArray)
            }
            DataType::Decimal128(_, _) => downcast_decimal!(Decimal128, Decimal128Array),
            DataType::Decimal256(_, _) => downcast_decimal!(Decimal256, Decimal256Array),
            other => return Err(ParseError::UnsupportedArrowType(format!("{other:?}"))),
        };

        Ok(reader)
    }
}

/// Per-cell accessor for one column, resolved once per column so the per-cell path
/// is a jump table plus an index — never a `DataType` match and a downcast.
pub enum ColumnReader<'a> {
    Primitives(&'a [DBResponsePrimitive]),
    Arrow(ArrowCellReader<'a>),
}

impl ColumnReader<'_> {
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            ColumnReader::Primitives(s) => s.len(),
            ColumnReader::Arrow(a) => a.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Value at `row`. Panics when out of range, like the slice indexing it
    /// replaces — `QueryResult::try_new` guarantees every column holds `row_count`
    /// cells.
    #[inline]
    pub fn value(&self, row: usize) -> DBResponsePrimitive {
        match self {
            ColumnReader::Primitives(s) => s[row].clone(),
            ColumnReader::Arrow(a) => a.value(row),
        }
    }

    /// Value at `row` rendered as text, or `None` when the cell is null. Borrows
    /// when the underlying storage already holds a string, so string columns cross
    /// into JS without an intermediate allocation.
    pub fn value_as_str(&self, row: usize) -> Option<Cow<'_, str>> {
        match self {
            ColumnReader::Primitives(s) => match &s[row] {
                DBResponsePrimitive::Null => None,
                DBResponsePrimitive::String(s) => Some(Cow::Borrowed(s)),
                other => Some(Cow::Owned(other.to_string())),
            },
            ColumnReader::Arrow(a) => a.value_as_str(row),
        }
    }

    /// The cell at `row`, transformed for `member_type`, passed to `f` by
    /// reference. A materialized column whose member type needs no transform is
    /// handed over borrowed, so a reader that only forwards the value — the
    /// row-major serializers — never clones it.
    #[inline]
    pub fn with_transformed<R>(
        &self,
        row: usize,
        member_type: &str,
        f: impl FnOnce(&DBResponsePrimitive) -> R,
    ) -> R {
        match self {
            ColumnReader::Primitives(cells) if is_identity_transform(member_type) => f(&cells[row]),
            _ => f(&transform_value(self.value(row), member_type)),
        }
    }

    /// Hand every cell, transformed for `member_type`, to `visit` in row order.
    /// The Arrow type dispatch happens once, outside the row loop — so prefer this
    /// over [`ColumnReader::value`] whenever a whole column is being consumed.
    /// Cells that need no transform arrive borrowed, the rest owned.
    pub fn for_each_transformed<E>(
        &self,
        member_type: &str,
        mut visit: impl FnMut(Cow<'_, DBResponsePrimitive>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            ColumnReader::Primitives(cells) => {
                if is_identity_transform(member_type) {
                    for cell in cells.iter() {
                        visit(Cow::Borrowed(cell))?;
                    }
                } else {
                    for cell in cells.iter() {
                        visit(Cow::Owned(transform_value(cell.clone(), member_type)))?;
                    }
                }
                Ok(())
            }
            ColumnReader::Arrow(a) => a.for_each_transformed(member_type, visit),
        }
    }

    /// Append every cell, transformed for `member_type`, to `out`.
    pub fn append_transformed(&self, out: &mut ColumnarArray, member_type: &str) {
        out.reserve(self.len());

        let Ok(()) = self.for_each_transformed::<Infallible>(member_type, |value| {
            out.push(value.into_owned());
            Ok(())
        });
    }
}

/// The concrete Arrow array behind one column.
pub enum ArrowCellReader<'a> {
    /// Zero-length column of any type — no cell is ever read.
    Empty,
    /// `DataType::Null` column, which carries nothing but its row count.
    Null(usize),
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
    /// Array plus its scale, used to render the mantissa as a decimal string.
    Decimal128(&'a Decimal128Array, u32),
    Decimal256(&'a Decimal256Array, u32),
}

/// Read `$array[$row]` as `$make(value)`, or `Null` when the cell is null.
macro_rules! read_cell {
    ($array:expr, $row:expr, $make:expr) => {{
        let a = $array;
        if a.is_null($row) {
            DBResponsePrimitive::Null
        } else {
            #[allow(clippy::redundant_closure_call)]
            ($make)(a.value($row))
        }
    }};
}

impl ArrowCellReader<'_> {
    pub fn len(&self) -> usize {
        match self {
            ArrowCellReader::Empty => 0,
            ArrowCellReader::Null(len) => *len,
            ArrowCellReader::Boolean(a) => a.len(),
            ArrowCellReader::Int8(a) => a.len(),
            ArrowCellReader::Int16(a) => a.len(),
            ArrowCellReader::Int32(a) => a.len(),
            ArrowCellReader::Int64(a) => a.len(),
            ArrowCellReader::UInt8(a) => a.len(),
            ArrowCellReader::UInt16(a) => a.len(),
            ArrowCellReader::UInt32(a) => a.len(),
            ArrowCellReader::UInt64(a) => a.len(),
            ArrowCellReader::Float16(a) => a.len(),
            ArrowCellReader::Float32(a) => a.len(),
            ArrowCellReader::Float64(a) => a.len(),
            ArrowCellReader::Utf8(a) => a.len(),
            ArrowCellReader::LargeUtf8(a) => a.len(),
            ArrowCellReader::Utf8View(a) => a.len(),
            ArrowCellReader::Date32(a) => a.len(),
            ArrowCellReader::Date64(a) => a.len(),
            ArrowCellReader::TimestampSecond(a) => a.len(),
            ArrowCellReader::TimestampMillisecond(a) => a.len(),
            ArrowCellReader::TimestampMicrosecond(a) => a.len(),
            ArrowCellReader::TimestampNanosecond(a) => a.len(),
            ArrowCellReader::Decimal128(a, _) => a.len(),
            ArrowCellReader::Decimal256(a, _) => a.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn value(&self, row: usize) -> DBResponsePrimitive {
        match self {
            // `Empty` columns hold no cells and `Null` columns hold nothing but
            // nulls, so neither needs a bounds check to answer.
            ArrowCellReader::Empty | ArrowCellReader::Null(_) => DBResponsePrimitive::Null,
            ArrowCellReader::Boolean(a) => read_cell!(a, row, DBResponsePrimitive::Boolean),
            ArrowCellReader::Int8(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::Int64(v as i64))
            }
            ArrowCellReader::Int16(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::Int64(v as i64))
            }
            ArrowCellReader::Int32(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::Int64(v as i64))
            }
            ArrowCellReader::Int64(a) => read_cell!(a, row, DBResponsePrimitive::Int64),
            ArrowCellReader::UInt8(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::UInt64(v as u64))
            }
            ArrowCellReader::UInt16(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::UInt64(v as u64))
            }
            ArrowCellReader::UInt32(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::UInt64(v as u64))
            }
            ArrowCellReader::UInt64(a) => read_cell!(a, row, DBResponsePrimitive::UInt64),
            // `half` is not a direct dependency, so the closure form of
            // `read_cell!` can't name `f16` for inference — spell the read out.
            ArrowCellReader::Float16(a) => {
                if a.is_null(row) {
                    DBResponsePrimitive::Null
                } else {
                    DBResponsePrimitive::Float64(a.value(row).to_f64())
                }
            }
            ArrowCellReader::Float32(a) => {
                read_cell!(a, row, |v| DBResponsePrimitive::Float64(v as f64))
            }
            ArrowCellReader::Float64(a) => read_cell!(a, row, DBResponsePrimitive::Float64),
            ArrowCellReader::Utf8(a) => {
                read_cell!(a, row, |v: &str| DBResponsePrimitive::String(v.to_owned()))
            }
            ArrowCellReader::LargeUtf8(a) => {
                read_cell!(a, row, |v: &str| DBResponsePrimitive::String(v.to_owned()))
            }
            ArrowCellReader::Utf8View(a) => {
                read_cell!(a, row, |v: &str| DBResponsePrimitive::String(v.to_owned()))
            }
            ArrowCellReader::Date32(a) => datetime_cell(*a, row),
            ArrowCellReader::Date64(a) => datetime_cell(*a, row),
            ArrowCellReader::TimestampSecond(a) => datetime_cell(*a, row),
            ArrowCellReader::TimestampMillisecond(a) => datetime_cell(*a, row),
            ArrowCellReader::TimestampMicrosecond(a) => datetime_cell(*a, row),
            ArrowCellReader::TimestampNanosecond(a) => datetime_cell(*a, row),
            ArrowCellReader::Decimal128(a, scale) => {
                read_cell!(a, row, |v| DBResponsePrimitive::String(decimal_to_string(
                    v, *scale
                )))
            }
            ArrowCellReader::Decimal256(a, scale) => {
                read_cell!(a, row, |v| DBResponsePrimitive::String(decimal_to_string(
                    v, *scale
                )))
            }
        }
    }

    /// Text rendering of `row`, borrowing from the Arrow buffer for string types.
    fn value_as_str(&self, row: usize) -> Option<Cow<'_, str>> {
        macro_rules! borrowed_str {
            ($array:expr) => {{
                let a = $array;
                if a.is_null(row) {
                    None
                } else {
                    Some(Cow::Borrowed(a.value(row)))
                }
            }};
        }

        match self {
            ArrowCellReader::Utf8(a) => borrowed_str!(a),
            ArrowCellReader::LargeUtf8(a) => borrowed_str!(a),
            ArrowCellReader::Utf8View(a) => borrowed_str!(a),
            other => match other.value(row) {
                DBResponsePrimitive::Null => None,
                // A freshly decoded primitive, so `String` can be unwrapped
                // instead of re-rendered through `Display`.
                DBResponsePrimitive::String(s) => Some(Cow::Owned(s)),
                value => Some(Cow::Owned(value.to_string())),
            },
        }
    }

    /// Column-major read with the type match hoisted out of the row loop.
    fn for_each_transformed<E>(
        &self,
        member_type: &str,
        mut visit: impl FnMut(Cow<'_, DBResponsePrimitive>) -> Result<(), E>,
    ) -> Result<(), E> {
        let len = self.len();

        macro_rules! fill {
            ($read:expr) => {{
                for row in 0..len {
                    visit(Cow::Owned(transform_value($read(row), member_type)))?;
                }
            }};
        }

        macro_rules! fill_with {
            ($array:expr, $make:expr) => {{
                let a = $array;
                fill!(|row| read_cell!(a, row, $make))
            }};
        }

        match self {
            ArrowCellReader::Empty => {}
            ArrowCellReader::Null(_) => {
                for _ in 0..len {
                    visit(Cow::Owned(DBResponsePrimitive::Null))?;
                }
            }
            ArrowCellReader::Boolean(a) => fill_with!(a, DBResponsePrimitive::Boolean),
            ArrowCellReader::Int8(a) => fill_with!(a, |v| DBResponsePrimitive::Int64(v as i64)),
            ArrowCellReader::Int16(a) => fill_with!(a, |v| DBResponsePrimitive::Int64(v as i64)),
            ArrowCellReader::Int32(a) => fill_with!(a, |v| DBResponsePrimitive::Int64(v as i64)),
            ArrowCellReader::Int64(a) => fill_with!(a, DBResponsePrimitive::Int64),
            ArrowCellReader::UInt8(a) => fill_with!(a, |v| DBResponsePrimitive::UInt64(v as u64)),
            ArrowCellReader::UInt16(a) => fill_with!(a, |v| DBResponsePrimitive::UInt64(v as u64)),
            ArrowCellReader::UInt32(a) => fill_with!(a, |v| DBResponsePrimitive::UInt64(v as u64)),
            ArrowCellReader::UInt64(a) => fill_with!(a, DBResponsePrimitive::UInt64),
            ArrowCellReader::Float16(a) => fill!(|row| if a.is_null(row) {
                DBResponsePrimitive::Null
            } else {
                DBResponsePrimitive::Float64(a.value(row).to_f64())
            }),
            ArrowCellReader::Float32(a) => {
                fill_with!(a, |v| DBResponsePrimitive::Float64(v as f64))
            }
            ArrowCellReader::Float64(a) => fill_with!(a, DBResponsePrimitive::Float64),
            ArrowCellReader::Utf8(a) => {
                fill_with!(a, |v: &str| DBResponsePrimitive::String(v.to_owned()))
            }
            ArrowCellReader::LargeUtf8(a) => {
                fill_with!(a, |v: &str| DBResponsePrimitive::String(v.to_owned()))
            }
            ArrowCellReader::Utf8View(a) => {
                fill_with!(a, |v: &str| DBResponsePrimitive::String(v.to_owned()))
            }
            ArrowCellReader::Date32(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::Date64(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampSecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampMillisecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampMicrosecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampNanosecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::Decimal128(a, scale) => {
                fill_with!(a, |v| DBResponsePrimitive::String(decimal_to_string(
                    v, *scale
                )))
            }
            ArrowCellReader::Decimal256(a, scale) => {
                fill_with!(a, |v| DBResponsePrimitive::String(decimal_to_string(
                    v, *scale
                )))
            }
        }

        Ok(())
    }
}

/// Read a date/timestamp cell as [`DBResponsePrimitive::Timestamp`]. The timezone
/// an Arrow timestamp field may carry is ignored, as it was before this refactor.
#[inline]
fn datetime_cell<T>(array: &PrimitiveArray<T>, row: usize) -> DBResponsePrimitive
where
    T: ArrowTemporalType,
    i64: From<T::Native>,
{
    if array.is_null(row) {
        return DBResponsePrimitive::Null;
    }

    match array.value_as_datetime(row) {
        Some(dt) => DBResponsePrimitive::Timestamp(dt),
        None => DBResponsePrimitive::Null,
    }
}

/// Format a decimal `mantissa` with `scale` fractional digits, stripping trailing
/// fractional zeros. Generic over the mantissa's `Display`, so it renders any Arrow
/// decimal width (`i32`/`i64`/`i128`/`i256`) directly — Decimal256 needs no fallback
/// to Arrow's own string conversion.
///
/// e.g. `(25987600, 5) -> "259.876"`, `(6199200000, 5) -> "61992"`,
/// `(-250, 3) -> "-0.25"`, `(25, 5) -> "0.00025"`.
pub(crate) fn decimal_to_string<T: std::fmt::Display>(mantissa: T, scale: u32) -> String {
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
