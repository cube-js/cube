use datafusion::arrow::array::{
    BooleanArray, BooleanBuilder, Int64Array, Int64Builder, StringArray, StringBuilder,
    UInt32Array, UInt32Builder,
};

pub fn new_string_array_with_placeholder(size: usize, default: Option<String>) -> StringArray {
    let mut builder = StringBuilder::new(size);

    if let Some(d) = default {
        for _ in 0..size {
            builder.append_value(d.as_str()).unwrap();
        }
    } else {
        for _ in 0..size {
            builder.append_null().unwrap();
        }
    };

    builder.finish()
}

pub fn new_int64_array_with_placeholder(size: usize, default: i64) -> Int64Array {
    let mut builder = Int64Builder::new(size);

    for _ in 0..size {
        builder.append_value(default).unwrap();
    }

    builder.finish()
}

pub fn new_uint32_array_with_placeholder(size: usize, default: u32) -> UInt32Array {
    let mut builder = UInt32Builder::new(size);

    for _ in 0..size {
        builder.append_value(default).unwrap();
    }

    builder.finish()
}

pub fn new_boolean_array_with_placeholder(size: usize, default: bool) -> BooleanArray {
    let mut builder = BooleanBuilder::new(size);

    for _ in 0..size {
        builder.append_value(default).unwrap();
    }

    builder.finish()
}
