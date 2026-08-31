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
use arrow::datatypes::{i256, ArrowTemporalType, DataType, TimeUnit};
use serde::{Serialize, Serializer};
use std::{
    borrow::Cow,
    convert::Infallible,
    fmt::{self, Write as _},
};

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

/// A cell on its way out of a column.
///
/// [`DBResponsePrimitive`] owns its `String`, so building one from an Arrow `Utf8`
/// or decimal column costs an allocation per cell. Callers that only render a cell
/// and drop it — the response serializers — take this instead, which borrows text
/// where it already lives and renders a decimal straight into the output.
pub enum CellRef<'a> {
    /// A cell already materialized in the column.
    Primitive(&'a DBResponsePrimitive),
    /// Text borrowed straight from the column's buffer.
    Str(&'a str),
    /// A decimal still in its Arrow form, rendered on the way out.
    Decimal128 {
        mantissa: i128,
        scale: u32,
    },
    Decimal256 {
        mantissa: i256,
        scale: u32,
    },
    /// Decoded on read: booleans, numbers, timestamps.
    Owned(DBResponsePrimitive),
}

impl CellRef<'_> {
    /// The same cell as an owned primitive, for callers that keep it.
    #[inline]
    pub fn into_owned(self) -> DBResponsePrimitive {
        match self {
            CellRef::Primitive(value) => value.clone(),
            CellRef::Str(text) => DBResponsePrimitive::String(text.to_owned()),
            CellRef::Decimal128 { mantissa, scale } => {
                DBResponsePrimitive::String(decimal_to_string(mantissa, scale))
            }
            CellRef::Decimal256 { mantissa, scale } => {
                DBResponsePrimitive::String(decimal_to_string(mantissa, scale))
            }
            CellRef::Owned(value) => value,
        }
    }
}

/// Mirrors `Serialize for DBResponsePrimitive`: `Str` and the decimals render
/// exactly as that impl's `String` arm would — a JSON string, from the same
/// [`DecimalText`] the owned path formats with — and every other cell delegates to
/// it outright.
impl Serialize for CellRef<'_> {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            CellRef::Primitive(value) => value.serialize(serializer),
            CellRef::Str(text) => serializer.serialize_str(text),
            CellRef::Decimal128 { mantissa, scale } => serializer.collect_str(&DecimalText {
                mantissa: *mantissa,
                scale: *scale,
            }),
            CellRef::Decimal256 { mantissa, scale } => serializer.collect_str(&DecimalText {
                mantissa: *mantissa,
                scale: *scale,
            }),
            CellRef::Owned(value) => value.serialize(serializer),
        }
    }
}

/// Read `$array[$row]` as borrowed text, or a null cell.
macro_rules! borrowed_str_cell {
    ($array:expr, $row:expr) => {{
        let a = $array;
        if a.is_null($row) {
            CellRef::Owned(DBResponsePrimitive::Null)
        } else {
            CellRef::Str(a.value($row))
        }
    }};
}

/// Read `$array[$row]` as an unrendered decimal, or a null cell.
macro_rules! decimal_cell {
    ($array:expr, $row:expr, $variant:ident, $scale:expr) => {{
        let a = $array;
        if a.is_null($row) {
            CellRef::Owned(DBResponsePrimitive::Null)
        } else {
            CellRef::$variant {
                mantissa: a.value($row),
                scale: $scale,
            }
        }
    }};
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

    /// The cell at `row`, transformed for `member_type`, handed to `f`. Cells that
    /// need no transform are passed borrowed — from the column's own storage or
    /// straight out of the Arrow buffer — so a caller that only renders the value
    /// never allocates for it.
    #[inline]
    pub fn with_transformed<R>(
        &self,
        row: usize,
        member_type: &str,
        f: impl FnOnce(CellRef<'_>) -> R,
    ) -> R {
        if is_identity_transform(member_type) {
            match self {
                ColumnReader::Primitives(cells) => return f(CellRef::Primitive(&cells[row])),
                ColumnReader::Arrow(a) => {
                    if let Some(cell) = a.cell_without_alloc(row) {
                        return f(cell);
                    }
                }
            }
        }

        f(CellRef::Owned(transform_value(
            self.value(row),
            member_type,
        )))
    }

    /// Hand every cell, transformed for `member_type`, to `visit` in row order.
    /// The Arrow type dispatch happens once, outside the row loop — so prefer this
    /// over [`ColumnReader::value`] whenever a whole column is being consumed.
    /// Cells that need no transform arrive borrowed, the rest owned.
    pub fn for_each_transformed<E>(
        &self,
        member_type: &str,
        mut visit: impl FnMut(CellRef<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            ColumnReader::Primitives(cells) => {
                if is_identity_transform(member_type) {
                    for cell in cells.iter() {
                        visit(CellRef::Primitive(cell))?;
                    }
                } else {
                    for cell in cells.iter() {
                        visit(CellRef::Owned(transform_value(cell.clone(), member_type)))?;
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

    /// The cell at `row` for the types whose [`ArrowCellReader::value`] would
    /// allocate — text, which is borrowed instead, and decimals, which are left
    /// unrendered. `None` for every other type, whose owned form allocates nothing
    /// anyway, so the caller just decodes it.
    #[inline]
    fn cell_without_alloc(&self, row: usize) -> Option<CellRef<'_>> {
        match self {
            ArrowCellReader::Utf8(a) => Some(borrowed_str_cell!(a, row)),
            ArrowCellReader::LargeUtf8(a) => Some(borrowed_str_cell!(a, row)),
            ArrowCellReader::Utf8View(a) => Some(borrowed_str_cell!(a, row)),
            ArrowCellReader::Decimal128(a, scale) => {
                Some(decimal_cell!(a, row, Decimal128, *scale))
            }
            ArrowCellReader::Decimal256(a, scale) => {
                Some(decimal_cell!(a, row, Decimal256, *scale))
            }
            _ => None,
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
        mut visit: impl FnMut(CellRef<'_>) -> Result<(), E>,
    ) -> Result<(), E> {
        let len = self.len();

        macro_rules! fill {
            ($read:expr) => {{
                for row in 0..len {
                    visit(CellRef::Owned(transform_value($read(row), member_type)))?;
                }
            }};
        }

        macro_rules! fill_with {
            ($array:expr, $make:expr) => {{
                let a = $array;
                fill!(|row| read_cell!(a, row, $make))
            }};
        }

        /// Text columns are handed over borrowed unless the member type asks for
        /// a transform, which needs an owned `String` to rewrite.
        macro_rules! fill_str {
            ($array:expr) => {{
                let a = $array;
                if is_identity_transform(member_type) {
                    for row in 0..len {
                        visit(borrowed_str_cell!(a, row))?;
                    }
                } else {
                    fill!(
                        |row| read_cell!(a, row, |v: &str| DBResponsePrimitive::String(
                            v.to_owned()
                        ))
                    )
                }
            }};
        }

        /// Decimals likewise: handed over unrendered when nothing has to rewrite
        /// them, so their digits go straight into the output.
        macro_rules! fill_decimal {
            ($array:expr, $variant:ident, $scale:expr, $make:expr) => {{
                let a = $array;
                if is_identity_transform(member_type) {
                    for row in 0..len {
                        visit(decimal_cell!(a, row, $variant, $scale))?;
                    }
                } else {
                    fill!(|row| read_cell!(a, row, $make))
                }
            }};
        }

        match self {
            ArrowCellReader::Empty => {}
            ArrowCellReader::Null(_) => {
                for _ in 0..len {
                    visit(CellRef::Owned(DBResponsePrimitive::Null))?;
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
            ArrowCellReader::Utf8(a) => fill_str!(a),
            ArrowCellReader::LargeUtf8(a) => fill_str!(a),
            ArrowCellReader::Utf8View(a) => fill_str!(a),
            ArrowCellReader::Date32(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::Date64(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampSecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampMillisecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampMicrosecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::TimestampNanosecond(a) => fill!(|row| datetime_cell(*a, row)),
            ArrowCellReader::Decimal128(a, scale) => {
                fill_decimal!(a, Decimal128, *scale, |v| DBResponsePrimitive::String(
                    decimal_to_string(v, *scale)
                ))
            }
            ArrowCellReader::Decimal256(a, scale) => {
                fill_decimal!(a, Decimal256, *scale, |v| DBResponsePrimitive::String(
                    decimal_to_string(v, *scale)
                ))
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

/// Renders a decimal from its `mantissa` and `scale`: the mantissa's digits with a
/// point inserted `scale` places from the right and trailing fractional zeros
/// stripped. Generic over the mantissa's `Display`, so it covers every Arrow
/// decimal width (`i32`/`i64`/`i128`/`i256`) — Decimal256 needs no fallback to
/// Arrow's own string conversion.
///
/// Rendering through `fmt::Write` and a stack buffer means no allocation at all:
/// `collect_str` streams this straight into the response, and
/// [`decimal_to_string`] is the same text collected into a `String`.
///
/// e.g. `(25987600, 5) -> "259.876"`, `(6199200000, 5) -> "61992"`,
/// `(-250, 3) -> "-0.25"`, `(25, 5) -> "0.00025"`.
pub(crate) struct DecimalText<T> {
    pub mantissa: T,
    pub scale: u32,
}

impl<T: fmt::Display> fmt::Display for DecimalText<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.mantissa);
        }

        let mut rendered = DigitBuf::default();
        write!(&mut rendered, "{}", self.mantissa)?;
        let raw = rendered.as_str()?;

        let scale = self.scale as usize;
        let (sign, digits) = match raw.strip_prefix('-') {
            Some(rest) => ("-", rest),
            None => ("", raw),
        };

        if digits.len() > scale {
            let (int_part, frac) = digits.split_at(digits.len() - scale);
            let frac = frac.trim_end_matches('0');
            f.write_str(sign)?;
            f.write_str(int_part)?;
            if !frac.is_empty() {
                f.write_char('.')?;
                f.write_str(frac)?;
            }
            return Ok(());
        }

        // Fewer digits than the scale, so the value is `0.` followed by the digits
        // padded out to `scale`. Trailing zeros are stripped from the digits, which
        // is where any of them can be.
        let frac = digits.trim_end_matches('0');
        f.write_str(sign)?;
        if frac.is_empty() {
            return f.write_str("0");
        }

        f.write_str("0.")?;
        for _ in 0..scale - digits.len() {
            f.write_char('0')?;
        }
        f.write_str(frac)
    }
}

/// Fixed-size sink for a mantissa's digits. `i256::MIN` is 78 digits plus a sign,
/// so this covers every Arrow decimal width with room to spare; a mantissa that
/// somehow overflowed it would surface as a formatting error rather than bad text.
struct DigitBuf {
    bytes: [u8; 96],
    len: usize,
}

impl Default for DigitBuf {
    fn default() -> Self {
        Self {
            bytes: [0; 96],
            len: 0,
        }
    }
}

impl DigitBuf {
    fn as_str(&self) -> Result<&str, fmt::Error> {
        std::str::from_utf8(&self.bytes[..self.len]).map_err(|_| fmt::Error)
    }
}

impl fmt::Write for DigitBuf {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let end = self.len + s.len();
        if end > self.bytes.len() {
            return Err(fmt::Error);
        }

        self.bytes[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// [`DecimalText`] collected into a `String`, for the paths that keep the value.
pub(crate) fn decimal_to_string<T: fmt::Display>(mantissa: T, scale: u32) -> String {
    DecimalText { mantissa, scale }.to_string()
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

    /// The allocation-free renderer must agree with the straightforward
    /// String-building version everywhere, not just on the cases above. This is
    /// that version, kept only as the oracle below.
    fn decimal_to_string_reference(raw: String, scale: u32) -> String {
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

    #[test]
    fn test_decimal_to_string_matches_reference() {
        let mantissas: Vec<i128> = [
            0i128,
            1,
            5,
            9,
            10,
            99,
            100,
            101,
            1_000,
            1_005,
            10_000_000,
            123_456_789,
            999_999_999_999,
            1_000_000_000_000,
            i128::MAX,
            i128::MIN,
        ]
        .into_iter()
        // `-i128::MIN` overflows, so only negate what can be negated.
        .flat_map(|m| [Some(m), m.checked_neg()])
        .flatten()
        .collect();

        for mantissa in mantissas {
            for scale in [0u32, 1, 2, 3, 5, 9, 12, 20, 38, 39, 40] {
                assert_eq!(
                    decimal_to_string(mantissa, scale),
                    decimal_to_string_reference(mantissa.to_string(), scale),
                    "mantissa={mantissa} scale={scale}"
                );
            }
        }
    }

    /// The widest mantissa Arrow can hand over must still render, i.e. the stack
    /// buffer has to be large enough for it.
    #[test]
    fn test_decimal_to_string_i256_extremes() {
        for mantissa in [i256::MAX, i256::MIN, i256::from_i128(-1), i256::ZERO] {
            for scale in [0u32, 2, 38, 76] {
                assert_eq!(
                    decimal_to_string(mantissa, scale),
                    decimal_to_string_reference(mantissa.to_string(), scale),
                    "mantissa={mantissa} scale={scale}"
                );
            }
        }
    }

    /// Serializing an unrendered decimal must produce exactly the JSON the owned
    /// `String` cell produces.
    #[test]
    fn test_decimal_cell_serializes_like_owned_string() {
        for (mantissa, scale) in [
            (239996i128, 2u32),
            (-250, 3),
            (0, 5),
            (i128::MIN, 38),
            (6199200000, 5),
        ] {
            let unrendered = CellRef::Decimal128 { mantissa, scale };
            let owned = DBResponsePrimitive::String(decimal_to_string(mantissa, scale));

            assert_eq!(
                serde_json::to_string(&unrendered).unwrap(),
                serde_json::to_string(&owned).unwrap(),
                "mantissa={mantissa} scale={scale}"
            );
        }

        let big = i256::MAX;
        assert_eq!(
            serde_json::to_string(&CellRef::Decimal256 {
                mantissa: big,
                scale: 4
            })
            .unwrap(),
            serde_json::to_string(&DBResponsePrimitive::String(decimal_to_string(big, 4))).unwrap(),
        );
    }
}
