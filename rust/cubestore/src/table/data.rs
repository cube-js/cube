use crate::metastore::{Column, ColumnType};
use crate::table::{Row, TableValue, TimestampValue};
use crate::util::decimal::Decimal;
use crate::util::ordfloat::OrdF64;
use arrow::array::{Array, ArrayBuilder, ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use std::cmp::Ordering;

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

/// Use for comparing min row marker inside partitions.
pub fn cmp_min_rows(key_size: usize, l: Option<&Row>, r: Option<&Row>) -> Ordering {
    match (l, r) {
        (None, None) => Ordering::Equal,
        (None, _) => Ordering::Less,
        (_, None) => Ordering::Greater,
        (Some(a), Some(b)) => cmp_row_key_heap(key_size, &a.values(), &b.values()),
    }
}

pub fn cmp_row_key_heap(sort_key_size: usize, l: &[TableValue], r: &[TableValue]) -> Ordering {
    for i in 0..sort_key_size {
        let c = cmp_same_types(
            &TableValueR::from_heap_allocated(&l[i]),
            &TableValueR::from_heap_allocated(&r[i]),
        );
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
