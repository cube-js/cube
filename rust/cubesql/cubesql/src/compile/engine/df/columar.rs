use datafusion::{
    arrow::{
        array::{self, *},
        datatypes::DataType,
    },
    error::{DataFusionError, Result},
};
use std::sync::Arc;

macro_rules! if_then_else {
    ($BUILDER_TYPE:ty, $ARRAY_TYPE:ty, $BOOLS:expr, $TRUE:expr, $FALSE:expr) => {{
        let true_values = $TRUE
            .as_ref()
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .expect("true_values downcast failed");

        let false_values = $FALSE
            .as_ref()
            .as_any()
            .downcast_ref::<$ARRAY_TYPE>()
            .expect("false_values downcast failed");

        let mut builder = <$BUILDER_TYPE>::new($BOOLS.len());
        for i in 0..$BOOLS.len() {
            if $BOOLS.is_null(i) {
                if false_values.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(false_values.value(i))?;
                }
            } else if $BOOLS.value(i) {
                if true_values.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(true_values.value(i))?;
                }
            } else {
                if false_values.is_null(i) {
                    builder.append_null()?;
                } else {
                    builder.append_value(false_values.value(i))?;
                }
            }
        }
        Ok(Arc::new(builder.finish()))
    }};
}

pub fn if_then_else(
    bools: &BooleanArray,
    true_values: ArrayRef,
    false_values: ArrayRef,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::UInt8 => if_then_else!(
            array::UInt8Builder,
            array::UInt8Array,
            bools,
            true_values,
            false_values
        ),
        DataType::UInt16 => if_then_else!(
            array::UInt16Builder,
            array::UInt16Array,
            bools,
            true_values,
            false_values
        ),
        DataType::UInt32 => if_then_else!(
            array::UInt32Builder,
            array::UInt32Array,
            bools,
            true_values,
            false_values
        ),
        DataType::UInt64 => if_then_else!(
            array::UInt64Builder,
            array::UInt64Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int8 => if_then_else!(
            array::Int8Builder,
            array::Int8Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int16 => if_then_else!(
            array::Int16Builder,
            array::Int16Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int32 => if_then_else!(
            array::Int32Builder,
            array::Int32Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Int64 => if_then_else!(
            array::Int64Builder,
            array::Int64Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Float32 => if_then_else!(
            array::Float32Builder,
            array::Float32Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Float64 => if_then_else!(
            array::Float64Builder,
            array::Float64Array,
            bools,
            true_values,
            false_values
        ),
        DataType::Utf8 => if_then_else!(
            array::StringBuilder,
            array::StringArray,
            bools,
            true_values,
            false_values
        ),
        DataType::Boolean => if_then_else!(
            array::BooleanBuilder,
            array::BooleanArray,
            bools,
            true_values,
            false_values
        ),
        other => Err(DataFusionError::Execution(format!(
            "CASE does not support '{:?}'",
            other
        ))),
    }
}
