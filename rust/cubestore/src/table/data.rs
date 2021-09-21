//! CubeStore needs to store large chunks of data in memory sometimes. We want to do this
//! efficiently and minimize the overhead of memory allocations and
//!
//! There are two flavours of data that we use throughout the program:
//!   - heap-allocated: [TableValue] and [Row];
//!   - arena-allocated: [TableValueR], [RowR], [Rows] and [MutRows].
//!
//! One should prefer arena-allocated, they will generally be more efficient. When allocating many
//! values, using arena-allocated is a requirement to avoid overhead of heap allocations.
//! You might need to fallback to heap-allocated if you need the `Serialize` trait, but only do so
//! for small quantities.
//!
//! This module helps with creating and processing arena-allocated data. [MutRows] deals with saving
//! all values a set of rows into the arena. Use it to create arena-allocated rows.
//!
//! [Rows] allows the reorder and filter rows or columns without copying the underlying data
//! (strings and byte arrays). One can convert [MutRows] to [Rows] with [`MutRows::freeze`].
//!
//! To iterate over produced rows use [RowsView], also see [`Rows::view`].
use std::cmp::Ordering;
use std::mem::ManuallyDrop;
use std::ops::Index;
use std::sync::Arc;

use bumpalo::Bump;
use itertools::Itertools;

use crate::metastore::{Column, ColumnType};
use crate::table::{Row, TableValue, TimestampValue};
use crate::util::decimal::Decimal;
use crate::util::ordfloat::OrdF64;
use arrow::array::{Array, ArrayBuilder, ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::cube_match_array;

use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use std::fmt;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum TableValueR<'a> {
    Null,
    String(&'a str),
    Int(i64),
    Decimal(Decimal),
    Float(OrdF64),
    Bytes(&'a [u8]),
    Timestamp(TimestampValue),
    Boolean(bool),
}

impl TableValueR<'_> {
    pub fn from_heap_allocated(v: &'a TableValue) -> TableValueR<'a> {
        match v {
            TableValue::Null => TableValueR::Null,
            TableValue::String(s) => TableValueR::String(&s),
            TableValue::Int(i) => TableValueR::Int(*i),
            TableValue::Decimal(d) => TableValueR::Decimal(*d),
            TableValue::Float(f) => TableValueR::Float(*f),
            TableValue::Bytes(b) => TableValueR::Bytes(&b),
            TableValue::Timestamp(v) => TableValueR::Timestamp(v.clone()),
            TableValue::Boolean(v) => TableValueR::Boolean(*v),
        }
    }
}

impl Default for TableValueR<'_> {
    fn default() -> Self {
        TableValueR::Null
    }
}

pub type RowR<'a> = [TableValueR<'a>];

pub struct Rows {
    num_columns: usize,
    arena: Arc<Bump>,
    values: TableValueVec,
}

unsafe impl Send for Rows {}
unsafe impl Sync for Rows {}

impl Rows {
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub fn num_rows(&self) -> usize {
        self.values.len / self.num_columns
    }

    pub fn len(&self) -> usize {
        self.num_rows()
    }

    pub fn view(&'a self) -> RowsView<'a> {
        let values = unsafe { self.values.as_slice() };
        RowsView::new(values, self.num_columns)
    }

    pub fn all_values(&'a self) -> &[TableValueR<'a>] {
        unsafe { self.values.as_slice() }
    }

    pub fn remap_columns(&self, new_columns: &[usize]) -> Rows {
        assert!(
            new_columns.iter().all(|c| *c < self.num_columns),
            "invalid column number"
        );
        let num_rows = self.num_rows();
        let num_columns = new_columns.len();

        let rows = self.view();
        let mut values = vec![TableValueR::default(); num_rows * num_columns];
        for i in 0..num_rows {
            let r = &rows[i];
            for j in 0..num_columns {
                values[i * num_columns + j] = r[new_columns[j]];
            }
        }

        Rows {
            num_columns: self.num_columns,
            arena: self.arena.clone(),
            values: unsafe { TableValueVec::new(values) },
        }
    }

    pub fn copy_some_rows(&self, row_indicies: &[usize]) -> Rows {
        let mut values = Vec::with_capacity(row_indicies.len() * self.num_columns);
        for i in 0..row_indicies.len() {
            values.extend_from_slice(&self.view()[row_indicies[i]])
        }

        Rows {
            num_columns: self.num_columns,
            arena: self.arena.clone(),
            values: unsafe { TableValueVec::new(values) },
        }
    }
}

#[derive(Clone, Copy)]
pub struct RowsView<'data> {
    num_columns: usize,
    values: &'data [TableValueR<'data>],
}

impl<'data> RowsView<'data> {
    pub fn new(data: &'data [TableValueR<'data>], num_columns: usize) -> RowsView<'data> {
        assert_eq!(data.len() % num_columns, 0);
        RowsView {
            num_columns,
            values: data,
        }
    }

    pub fn slice(&self, start: usize, end: usize) -> RowsView<'data> {
        RowsView::new(
            &self.values[self.num_columns * start..self.num_columns * end],
            self.num_columns,
        )
    }

    pub fn iter<'a>(&'a self) -> impl Iterator<Item = &'a RowR<'data>> + 'a {
        (0..self.len()).map(move |i| &self[i])
    }

    pub fn len(&self) -> usize {
        // TODO: avoid division here, store row count instead.
        self.values.len() / self.num_columns
    }

    pub fn num_rows(&self) -> usize {
        self.len()
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    #[cfg(test)]
    pub fn from_heap_allocated(
        buffer: &'a mut Vec<TableValueR<'a>>,
        num_columns: usize,
        rows: &'a [Row],
    ) -> RowsView<'a> {
        let n = rows.len();

        buffer.clear();
        buffer.resize(num_columns * n, TableValueR::default());

        for i in 0..n {
            for j in 0..num_columns {
                buffer[i * num_columns + j] =
                    TableValueR::from_heap_allocated(&rows[i].values()[j]);
            }
        }

        RowsView::new(buffer, num_columns)
    }

    #[cfg(test)]
    pub fn convert_to_heap_allocated(&self) -> Vec<Row> {
        let mut res = Vec::new();
        for r in self.iter() {
            res.push(convert_row_to_heap_allocated(r));
        }
        res
    }
}

impl<'a> Index<usize> for RowsView<'a> {
    type Output = RowR<'a>;
    fn index(&self, index: usize) -> &Self::Output {
        &self.values[index * self.num_columns..index * self.num_columns + self.num_columns]
    }
}

pub struct MutRows {
    num_columns: usize,
    arena: Bump,
    values: TableValueVec,
}

impl MutRows {
    pub fn new(num_columns: usize) -> MutRows {
        MutRows {
            num_columns,
            arena: Bump::new(),
            values: unsafe { TableValueVec::new(Vec::new()) },
        }
    }

    pub fn with_capacity(num_columns: usize, num_rows: usize) -> MutRows {
        MutRows {
            num_columns,
            arena: Bump::new(),
            values: unsafe { TableValueVec::new(Vec::with_capacity(num_columns * num_rows)) },
        }
    }

    pub fn freeze(self) -> Rows {
        Rows {
            num_columns: self.num_columns,
            arena: Arc::new(self.arena),
            values: self.values,
        }
    }

    pub fn num_columns(&self) -> usize {
        self.num_columns
    }

    pub fn num_rows(&self) -> usize {
        self.values.len / self.num_columns
    }

    pub fn len(&self) -> usize {
        self.num_rows()
    }

    pub fn rows(&'a self) -> RowsView<'a> {
        let values = unsafe { self.values.as_slice() };
        RowsView::new(values, self.num_columns)
    }

    pub fn truncate(&mut self, rows: usize) {
        unsafe {
            let num_columns = self.num_columns;
            self.values.on_mut_vec(|v| {
                v.truncate(rows * num_columns);
            });
        }
    }

    pub fn add_row(&'a mut self) -> InsertedRow<'a> {
        let mut start = 0;
        let mut end = 0;

        let row: &mut [TableValueR];
        unsafe {
            let num_columns = self.num_columns;
            self.values.on_mut_vec(|v| {
                start = v.len();
                end = v.len() + num_columns;
                v.resize(end, TableValueR::default());
            });
            row = &mut self.values.as_mut_slice()[start..end];
        }
        InsertedRow {
            arena: &self.arena,
            values: row,
        }
    }

    pub fn add_row_copy(&'a mut self, r: &RowR<'b>) -> InsertedRow<'a> {
        assert_eq!(self.num_columns, r.len());
        let num_columns = self.num_columns;
        let mut new = self.add_row();
        for i in 0..num_columns {
            new.set_interned(i, r[i]);
        }
        new
    }

    pub fn add_rows(&'a mut self, n: usize) -> InsertedRows<'a> {
        let mut start = 0;
        let mut end = 0;

        let values: &mut [TableValueR];
        unsafe {
            let num_columns = self.num_columns;
            self.values.on_mut_vec(|v| {
                start = v.len();
                end = v.len() + n * num_columns;
                v.resize(end, TableValueR::default());
            });
            values = &mut self.values.as_mut_slice()[start..end];
        }
        InsertedRows {
            num_columns: self.num_columns,
            arena: &self.arena,
            values,
        }
    }

    pub fn add_from_slice(&'a mut self, rows: RowsView<'a>) {
        let num_columns = self.num_columns;
        assert_eq!(num_columns, rows.num_columns);
        for r in rows.iter() {
            let mut newr = self.add_row();
            for i in 0..num_columns {
                newr.set_interned(i, r[i]);
            }
        }
    }

    pub fn add_row_heap_allocated(&mut self, r: &Row) {
        let n = self.num_columns;
        assert_eq!(r.values.len(), n);
        let mut newr = self.add_row();
        for i in 0..n {
            newr.set_interned(i, TableValueR::from_heap_allocated(&r.values()[i]));
        }
    }

    pub fn from_heap_allocated(num_columns: usize, rows: &[Row]) -> MutRows {
        let mut c = MutRows::with_capacity(num_columns, rows.len());
        for r in rows {
            c.add_row_heap_allocated(&r);
        }
        c
    }
}

pub struct InsertedRow<'a> {
    arena: &'a Bump,
    values: &'a mut [TableValueR<'a>],
}

impl<'a> InsertedRow<'a> {
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn get(&self, i: usize) -> &TableValueR<'a> {
        &self.values[i]
    }

    pub fn set_interned(&'b mut self, i: usize, v: TableValueR<'b>) {
        Self::assign_interned(&mut self.values[i], v, self.arena);
    }

    fn assign_interned(target: &mut TableValueR<'a>, source: TableValueR<'b>, arena: &'a Bump) {
        *target = match source {
            TableValueR::Boolean(v) => TableValueR::Boolean(v),
            TableValueR::Bytes(s) => TableValueR::Bytes(arena.alloc_slice_copy(s)),
            TableValueR::Decimal(d) => TableValueR::Decimal(d),
            TableValueR::Float(v) => TableValueR::Float(v),
            TableValueR::Int(v) => TableValueR::Int(v),
            TableValueR::Null => TableValueR::Null,
            TableValueR::String(s) => TableValueR::String(arena.alloc_str(s)),
            TableValueR::Timestamp(v) => TableValueR::Timestamp(v),
        };
    }
}

pub struct InsertedRows<'a> {
    num_columns: usize,
    arena: &'a Bump,
    values: &'a mut [TableValueR<'a>],
}

impl<'a> InsertedRows<'a> {
    pub fn len(&self) -> usize {
        self.values.len() / self.num_columns
    }

    pub fn get(&self, i: usize, j: usize) -> TableValueR<'a> {
        assert!(i < self.len());
        assert!(j < self.num_columns);
        return self.values[i * self.num_columns + j];
    }

    pub fn set_interned(&'b mut self, i: usize, j: usize, v: TableValueR<'b>) {
        assert!(i < self.len());
        assert!(j < self.num_columns);
        InsertedRow::assign_interned(&mut self.values[i * self.num_columns + j], v, self.arena);
    }
}

pub fn cmp_partition_key(key_size: usize, l: &[TableValue], r: &[ArrayRef], ri: usize) -> Ordering {
    for i in 0..key_size {
        let o = cmp_partition_column_same_type(&l[i], r[i].as_ref(), ri);
        if o != Ordering::Equal {
            return o;
        }
    }
    return Ordering::Equal;
}

pub fn cmp_partition_column_same_type(l: &TableValue, r: &dyn Array, ri: usize) -> Ordering {
    // Optimization to avoid memory allocations.
    match l {
        TableValue::Null => match r.is_valid(ri) {
            true => return Ordering::Less,
            false => return Ordering::Equal,
        },
        TableValue::String(l) => {
            let r = r.as_any().downcast_ref::<StringArray>().unwrap();
            if !r.is_valid(ri) {
                return Ordering::Greater;
            }
            return l.as_str().cmp(r.value(ri));
        }
        _ => {}
    }

    cmp_same_types(
        &TableValueR::from_heap_allocated(l),
        &TableValueR::from_heap_allocated(&TableValue::from_array(r, ri)),
    )
}

pub fn cmp_row_key_heap(sort_key_size: usize, l: &[TableValue], r: &[TableValueR]) -> Ordering {
    for i in 0..sort_key_size {
        let c = cmp_same_types(&TableValueR::from_heap_allocated(&l[i]), &r[i]);
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}

pub fn cmp_row_key(sort_key_size: usize, l: &[TableValueR], r: &[TableValueR]) -> Ordering {
    for i in 0..sort_key_size {
        let c = cmp_same_types(&l[i], &r[i]);
        if c != Ordering::Equal {
            return c;
        }
    }
    Ordering::Equal
}

pub fn cmp_same_types(l: &TableValueR, r: &TableValueR) -> Ordering {
    match (l, r) {
        (TableValueR::Null, TableValueR::Null) => Ordering::Equal,
        (TableValueR::Null, _) => Ordering::Less,
        (_, TableValueR::Null) => Ordering::Greater,
        (TableValueR::String(a), TableValueR::String(b)) => a.cmp(b),
        (TableValueR::Int(a), TableValueR::Int(b)) => a.cmp(b),
        (TableValueR::Decimal(a), TableValueR::Decimal(b)) => a.cmp(b),
        (TableValueR::Float(a), TableValueR::Float(b)) => a.cmp(b),
        (TableValueR::Bytes(a), TableValueR::Bytes(b)) => a.cmp(b),
        (TableValueR::Timestamp(a), TableValueR::Timestamp(b)) => a.cmp(b),
        (TableValueR::Boolean(a), TableValueR::Boolean(b)) => a.cmp(b),
        (a, b) => panic!("Can't compare {:?} to {:?}", a, b),
    }
}

pub fn convert_row_to_heap_allocated(r: &RowR) -> Row {
    Row::new(
        r.iter()
            .map(|c| match c {
                TableValueR::Null => TableValue::Null,
                TableValueR::String(s) => TableValue::String(s.to_string()),
                TableValueR::Int(i) => TableValue::Int(*i),
                TableValueR::Decimal(d) => TableValue::Decimal(*d),
                TableValueR::Float(f) => TableValue::Float(*f),
                TableValueR::Bytes(b) => TableValue::Bytes(b.to_vec()),
                TableValueR::Timestamp(v) => TableValue::Timestamp(v.clone()),
                TableValueR::Boolean(v) => TableValue::Boolean(*v),
            })
            .collect_vec(),
    )
}

/// `Vec<TableValueR>` with erased lifetime. Unsafe, use with caution.
struct TableValueVec {
    ptr: *mut u8,
    len: usize,
    cap: usize,
}

unsafe impl Send for TableValueVec {}
unsafe impl Sync for TableValueVec {}

impl TableValueVec {
    pub unsafe fn new(v: Vec<TableValueR>) -> TableValueVec {
        let (ptr, len, cap) = v.into_raw_parts();
        let ptr = ptr as *mut u8;
        TableValueVec { ptr, len, cap }
    }

    /// Extremely unsafe, fabricates lifetimes. Use with caution.
    pub unsafe fn as_slice<'data>(&self) -> &[TableValueR<'data>] {
        std::slice::from_raw_parts(self.ptr as *const TableValueR, self.len)
    }

    /// Extremely unsafe, fabricates lifetimes. Use with caution.
    pub unsafe fn as_mut_slice<'data>(&mut self) -> &mut [TableValueR<'data>] {
        std::slice::from_raw_parts_mut(self.ptr as *mut TableValueR, self.len)
    }

    /// Extremely unsafe, fabricates lifetimes. Use with caution.
    pub unsafe fn on_mut_vec<'data, F>(&mut self, op: F)
    where
        F: FnOnce(&mut Vec<TableValueR<'data>>),
    {
        let ptr = self.ptr as *mut TableValueR<'data>;
        let mut vec = ManuallyDrop::new(Vec::from_raw_parts(ptr, self.len, self.cap));

        op(&mut vec);

        let (ptr, len, cap) = ManuallyDrop::into_inner(vec).into_raw_parts();
        self.ptr = ptr as *mut u8;
        self.len = len;
        self.cap = cap;
    }
}

impl Drop for TableValueVec {
    fn drop(&'a mut self) {
        unsafe {
            let ptr = self.ptr as *mut TableValueR<'a>;
            // Reconstruct the vector to clean up the used memory.
            Vec::from_raw_parts(ptr, self.len, self.cap);
        }
    }
}

#[macro_export]
macro_rules! match_column_type {
    ($t: expr, $matcher: ident) => {{
        use arrow::array::*;
        let t = $t;
        match t {
            ColumnType::String => $matcher!(String, StringBuilder, String),
            ColumnType::Int => $matcher!(Int, Int64Builder, Int),
            ColumnType::Bytes => $matcher!(Bytes, BinaryBuilder, Bytes),
            ColumnType::HyperLogLog(_) => $matcher!(HyperLogLog, BinaryBuilder, Bytes),
            ColumnType::Timestamp => $matcher!(Timestamp, TimestampMicrosecondBuilder, Timestamp),
            ColumnType::Boolean => $matcher!(Boolean, BooleanBuilder, Boolean),
            ColumnType::Decimal { .. } => match t.target_scale() {
                0 => $matcher!(Decimal, Int64Decimal0Builder, Decimal, 0),
                1 => $matcher!(Decimal, Int64Decimal1Builder, Decimal, 1),
                2 => $matcher!(Decimal, Int64Decimal2Builder, Decimal, 2),
                3 => $matcher!(Decimal, Int64Decimal3Builder, Decimal, 3),
                4 => $matcher!(Decimal, Int64Decimal4Builder, Decimal, 4),
                5 => $matcher!(Decimal, Int64Decimal5Builder, Decimal, 5),
                10 => $matcher!(Decimal, Int64Decimal10Builder, Decimal, 10),
                n => panic!("unhandled target scale: {}", n),
            },
            ColumnType::Float => $matcher!(Float, Float64Builder, Float),
        }
    }};
}

pub fn create_array_builder(t: &ColumnType) -> Box<dyn ArrayBuilder> {
    macro_rules! create_builder {
        ($type: tt, $builder: tt $(,$arg: tt)*) => {
            Box::new($builder::new(0))
        };
    }
    match_column_type!(t, create_builder)
}

pub fn create_array_builders(cs: &[Column]) -> Vec<Box<dyn ArrayBuilder>> {
    cs.iter()
        .map(|c| create_array_builder(c.get_column_type()))
        .collect_vec()
}

pub fn append_row(bs: &mut [Box<dyn ArrayBuilder>], cs: &[Column], r: &Row) {
    assert_eq!(bs.len(), r.len());
    assert_eq!(cs.len(), r.len());
    for i in 0..r.len() {
        append_value(bs[i].as_mut(), cs[i].get_column_type(), &r.values()[i]);
    }
}

pub fn append_value(b: &mut dyn ArrayBuilder, c: &ColumnType, v: &TableValue) {
    let is_null = matches!(v, TableValue::Null);
    macro_rules! convert_value {
        (Decimal, $v: expr) => {{
            $v.raw_value()
        }};
        (Float, $v: expr) => {{
            $v.0
        }};
        (Timestamp, $v: expr) => {{
            $v.get_time_stamp() / 1000
        }}; // Nanoseconds to microseconds.
        (String, $v: expr) => {{
            $v.as_str()
        }};
        (Bytes, $v: expr) => {{
            $v.as_slice()
        }};
        ($tv_enum: tt, $v: expr) => {{
            *$v
        }};
    }
    macro_rules! append {
        ($type: tt, $builder: tt, $tv_enum: tt $(, $arg:tt)*) => {{
            let b = b.as_any_mut().downcast_mut::<$builder>().unwrap();
            if is_null {
                b.append_null().unwrap();
                return;
            }
            let v = match v {
                TableValue::$tv_enum(v) => convert_value!($tv_enum, v),
                other => panic!("unexpected value {:?} for type {:?}", other, c),
            };
            b.append_value(v).unwrap();
        }};
    }
    match_column_type!(c, append)
}

pub fn rows_to_columns(cols: &[Column], rows: &[Row]) -> Vec<ArrayRef> {
    let mut builders = create_array_builders(&cols);
    for r in rows {
        append_row(&mut builders, &cols, r);
    }
    builders.into_iter().map(|mut b| b.finish()).collect_vec()
}

pub async fn to_stream(r: RecordBatch) -> SendableRecordBatchStream {
    let schema = r.schema();
    MemoryExec::try_new(&[vec![r]], schema, None)
        .unwrap()
        .execute(0)
        .await
        .unwrap()
}

pub fn concat_record_batches(rs: &[RecordBatch]) -> RecordBatch {
    assert_ne!(rs.len(), 0);
    RecordBatch::concat(&rs[0].schema(), rs).unwrap()
}

#[macro_export]
macro_rules! assert_eq_columns {
    ($l: expr, $r: expr) => {{
        use crate::table::data::display_rows;
        use pretty_assertions::Comparison;

        let l = $l;
        let r = $r;
        assert_eq!(
            l,
            r,
            "{}",
            Comparison::new(
                &format!("{}", display_rows(l))
                    .split("\n")
                    .collect::<Vec<_>>(),
                &format!("{}", display_rows(r))
                    .split("\n")
                    .collect::<Vec<_>>()
            )
        );
    }};
}

pub fn display_rows(cols: &'a [ArrayRef]) -> impl fmt::Display + 'a {
    pub struct D<'a>(&'a [ArrayRef]);
    impl fmt::Display for D<'_> {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let cols = self.0;
            if cols.len() == 0 {
                return write!(f, "[]");
            }
            write!(f, "[")?;
            for i in 0..cols[0].len() {
                if i != 0 {
                    write!(f, "\n,")?
                }
                write!(f, "[")?;
                for j in 0..cols.len() {
                    if j != 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", TableValue::from_array(cols[j].as_ref(), i))?;
                }
                write!(f, "]")?;
            }
            write!(f, "]")
        }
    }
    D(cols)
}

pub fn eq_columns(l: &[ArrayRef], r: &[ArrayRef]) -> bool {
    if l.len() != r.len() {
        return false;
    }
    for i in 0..l.len() {
        if !eq_arrays(l[i].as_ref(), r[i].as_ref()) {
            return false;
        }
    }
    true
}

pub fn eq_arrays(l: &dyn Array, r: &dyn Array) -> bool {
    let t = l.data_type();
    if r.data_type() != t {
        return false;
    }

    macro_rules! arrays_eq {
        ($a: expr, $array: tt, $($rest:tt)*) => {{
            let l = l.as_any().downcast_ref::<$array>().unwrap();
            let r = r.as_any().downcast_ref::<$array>().unwrap();
            if l.len() != r.len() {
                return false;
            }
            for i in 0..l.len() {
                if l.is_valid(i) != r.is_valid(i) {
                    return false;
                }
                if !l.is_valid(i) {
                    continue;
                }
                if l.value(i) != r.value(i) {
                    return false;
                }
            }
            true
        }};
    }
    cube_match_array!(l, arrays_eq)
}
