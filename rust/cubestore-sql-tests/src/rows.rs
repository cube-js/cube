//! Helpers to match on returned rows in tests.
use cubestore::table::{TableValue, TimestampValue};
use cubestore::util::decimal::Decimal;

pub const NULL: () = ();

pub fn rows(i: &[impl ToRow]) -> Vec<Vec<TableValue>> {
    i.iter().map(ToRow::to_row).collect()
}

pub trait ToRow {
    fn to_row(&self) -> Vec<TableValue>;
}

macro_rules! impl_to_row {
    ( $($ts:ident),* ) => {
        impl <$( $ts: ToValue ),*> ToRow for ($( $ts ),*) {
            fn to_row(&self) -> Vec<TableValue> {
                let ( $( $ts ),* ) = self;
                vec![ $( $ts.to_val() ),* ]
            }
        }
    }
}

impl_to_row!(T);
impl_to_row!(T1, T2);
impl_to_row!(T1, T2, T3);
impl_to_row!(T1, T2, T3, T4);

pub trait ToValue {
    fn to_val(&self) -> TableValue;
}

impl ToValue for () {
    fn to_val(&self) -> TableValue {
        TableValue::Null
    }
}

impl<T: ToValue> ToValue for Option<T> {
    fn to_val(&self) -> TableValue {
        match self {
            None => TableValue::Null,
            Some(v) => v.to_val(),
        }
    }
}

impl ToValue for &str {
    fn to_val(&self) -> TableValue {
        TableValue::String(self.to_string())
    }
}

impl ToValue for i64 {
    fn to_val(&self) -> TableValue {
        TableValue::Int(*self)
    }
}

impl ToValue for Decimal {
    fn to_val(&self) -> TableValue {
        TableValue::Decimal(*self)
    }
}

impl ToValue for f64 {
    fn to_val(&self) -> TableValue {
        TableValue::Float(self.clone().into())
    }
}

impl ToValue for &[u8] {
    fn to_val(&self) -> TableValue {
        TableValue::Bytes(self.to_vec())
    }
}

impl ToValue for TimestampValue {
    fn to_val(&self) -> TableValue {
        TableValue::Timestamp(*self)
    }
}

impl ToValue for bool {
    fn to_val(&self) -> TableValue {
        TableValue::Boolean(*self)
    }
}
