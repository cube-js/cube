//! A time value outside the range of Arrow's nanosecond timestamps (roughly 1677-09-21 to
//! 2262-04-11) is legal in many sources, e.g. SQL Server `datetime2` starts at 0001-01-01.
//! Converting such a value must not silently become NULL: `SELECT MIN(d)` would then return
//! NULL for a column without any NULLs. See https://github.com/cube-js/cube/issues/11407

use std::sync::Arc;

use cubesql::compile::engine::df::scan::{
    transform_response, DataType, JsonColumnarValueObject, MemberField, Schema,
};
use datafusion::arrow::{
    array::{Array, TimestampNanosecondArray},
    datatypes::{Field, TimeUnit},
};

#[test]
fn out_of_range_timestamp_is_not_silently_null() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "lo",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let member_fields = vec![MemberField::regular("OutOfRange.d".to_string())];
    let chunk = r#"{"members": ["OutOfRange.d"], "columns": [["0202-01-01T00:00:00.000"]]}"#;
    let mut value_object = serde_json::from_str::<JsonColumnarValueObject>(chunk).unwrap();

    match transform_response(&mut value_object, schema, &member_fields) {
        // Surfacing the out-of-range value as an error is an acceptable fix
        Err(_) => {}
        Ok(batch) => {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            assert!(
                !column.is_null(0),
                "non-null source value 0202-01-01T00:00:00.000 was converted to NULL"
            );
        }
    }
}
