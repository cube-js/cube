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
use crate::table::{Row, TableValue, TimestampValue};
use crate::util::decimal::Decimal;
use crate::util::ordfloat::OrdF64;
use bumpalo::Bump;
use itertools::Itertools;
use std::cmp::Ordering;
use std::mem::ManuallyDrop;
use std::ops::Index;
use std::sync::Arc;

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
