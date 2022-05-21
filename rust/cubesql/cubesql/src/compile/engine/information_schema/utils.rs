pub use datafusion::arrow::{
    array::{Int32Builder as OidBuilder, StringBuilder as YesNoBuilder},
    datatypes::Int32Type as OidType,
};
pub use pg_srv::Oid;

pub type Xid = Oid;
pub type XidBuilder = OidBuilder;

use datafusion::arrow::{
    array::{
        BooleanArray, BooleanBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder,
        StringArray, StringBuilder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    datatypes::DataType,
};

type YesNoArray = StringArray;

pub fn new_string_array_with_placeholder(size: usize, default: Option<&str>) -> StringArray {
    let mut builder = StringBuilder::new(size);

    for _ in 0..size {
        builder.append_option(default).unwrap();
    }

    builder.finish()
}

pub fn new_int64_array_with_placeholder(size: usize, default: Option<i64>) -> Int64Array {
    let mut builder = Int64Builder::new(size);

    for _ in 0..size {
        builder.append_option(default).unwrap();
    }

    builder.finish()
}

pub fn new_uint64_array_with_placeholder(size: usize, default: Option<u64>) -> UInt64Array {
    let mut builder = UInt64Builder::new(size);

    for _ in 0..size {
        builder.append_option(default).unwrap();
    }

    builder.finish()
}

pub fn new_int32_array_with_placeholder(size: usize, default: Option<i32>) -> Int32Array {
    let mut builder = Int32Builder::new(size);

    for _ in 0..size {
        builder.append_option(default).unwrap();
    }

    builder.finish()
}

pub fn new_uint32_array_with_placeholder(size: usize, default: Option<u32>) -> UInt32Array {
    let mut builder = UInt32Builder::new(size);

    for _ in 0..size {
        builder.append_option(default).unwrap();
    }

    builder.finish()
}

pub fn new_boolean_array_with_placeholder(size: usize, default: Option<bool>) -> BooleanArray {
    let mut builder = BooleanBuilder::new(size);

    for _ in 0..size {
        builder.append_option(default).unwrap();
    }

    builder.finish()
}

pub fn new_yes_no_array_with_placeholder(size: usize, default: Option<bool>) -> YesNoArray {
    new_string_array_with_placeholder(size, default.map(|d| yes_no(d)))
}

pub fn yes_no(value: bool) -> &'static str {
    match value {
        true => "YES",
        false => "NO",
    }
}

pub enum ExtDataType {
    YesNo,
    Oid,
    Xid,
}

impl Into<DataType> for ExtDataType {
    fn into(self) -> DataType {
        match self {
            Self::YesNo => DataType::Utf8,
            Self::Oid | Self::Xid => DataType::Int32,
        }
    }
}
