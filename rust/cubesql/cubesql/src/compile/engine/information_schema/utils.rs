use datafusion::arrow::array::{
    BooleanArray, BooleanBuilder, Int64Array, Int64Builder, StringArray, StringBuilder,
    UInt32Array, UInt32Builder,
};

pub fn new_string_array_with_placeholder(size: usize, default: Option<String>) -> StringArray {
    let mut builder = StringBuilder::with_capacity(size, size);

    if let Some(d) = default {
        for _ in 0..size {
            builder.append_value(d.as_str());
        }
    } else {
        for _ in 0..size {
            builder.append_null();
        }
    };

    builder.finish()
}

pub fn new_int64_array_with_placeholder(size: usize, default: i64) -> Int64Array {
    let mut builder = Int64Builder::with_capacity(size);

    for _ in 0..size {
        builder.append_value(default);
    }

    builder.finish()
}

pub fn new_uint32_array_with_placeholder(size: usize, default: u32) -> UInt32Array {
    let mut builder = UInt32Builder::with_capacity(size);

    for _ in 0..size {
        builder.append_value(default);
    }

    builder.finish()
}

pub fn new_boolean_array_with_placeholder(size: usize, default: bool) -> BooleanArray {
    let mut builder = BooleanBuilder::with_capacity(size);

    for _ in 0..size {
        builder.append_value(default);
    }

    builder.finish()
}
