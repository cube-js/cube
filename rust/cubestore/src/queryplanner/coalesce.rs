use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::cube_match_array;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::sync::Arc;

/// Currently supported types by the coalesce function.
/// In the order on of applied coercions.
pub static SUPPORTED_COALESCE_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Int64Decimal(0),
    DataType::Int64Decimal(1),
    DataType::Int64Decimal(2),
    DataType::Int64Decimal(3),
    DataType::Int64Decimal(4),
    DataType::Int64Decimal(5),
    DataType::Int64Decimal(10),
    DataType::Timestamp(TimeUnit::Second, None),
    DataType::Timestamp(TimeUnit::Millisecond, None),
    DataType::Timestamp(TimeUnit::Microsecond, None),
    DataType::Timestamp(TimeUnit::Nanosecond, None),
    DataType::Date32,
    DataType::Date64,
    DataType::Interval(IntervalUnit::YearMonth),
    DataType::Interval(IntervalUnit::DayTime),
    DataType::Float32,
    DataType::Float64,
    DataType::Binary,
    DataType::LargeBinary,
    DataType::Utf8,
    DataType::LargeUtf8,
];

pub fn coalesce(values: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    if values.is_empty() {
        return Err(DataFusionError::Execution(
            "empty inputs to coalesce".to_string(),
        ));
    }
    // Find first array that has null values. Other cases are trivial.
    let mut i = 0;
    while i < values.len() {
        match &values[i] {
            ColumnarValue::Array(a) => {
                if a.null_count() == 0 {
                    return Ok(ColumnarValue::Array(a.clone()));
                }
                if a.null_count() != a.len() {
                    return Ok(ColumnarValue::Array(do_coalesce(a, &values[i + 1..])?));
                }
            }
            ColumnarValue::Scalar(s) => {
                if !s.is_null() {
                    return Ok(ColumnarValue::Scalar(s.clone()));
                }
            }
        }
        i += 1;
    }
    // All elements were null.
    return Ok(values.last().unwrap().clone());
}

fn do_coalesce(start: &ArrayRef, rest: &[ColumnarValue]) -> Result<ArrayRef, DataFusionError> {
    macro_rules! match_scalar {
        ($v: pat, Int64Decimal) => {
            ScalarValue::Int64Decimal($v, _)
        };
        ($v: pat, $variant: ident) => {
            ScalarValue::$variant($v)
        };
    }
    macro_rules! apply_coalesce {
        ($start: expr, $arr: ty, $builder_ty: ty, $scalar_enum: ident $($rest: tt)*) => {{
            let start = match $start.as_any().downcast_ref::<$arr>() {
                Some(a) => a,
                None => {
                    return Err(DataFusionError::Internal(
                        "failed to downcast array".to_string(),
                    ))
                }
            };
            let mut b = <$builder_ty>::new(start.len());
            for i in 0..start.len() {
                if !start.is_null(i) {
                    b.append_value(start.value(i))?;
                    continue;
                }
                let mut found = false;
                for o in rest {
                    match o {
                        ColumnarValue::Array(o) => {
                            let o = match o.as_any().downcast_ref::<$arr>() {
                                Some(o) => o,
                                None => {
                                    return Err(DataFusionError::Internal(
                                        "expected array of the same type".to_string(),
                                    ))
                                }
                            };
                            if !o.is_null(i) {
                                b.append_value(o.value(i))?;
                                found = true;
                                break;
                            }
                        }
                        ColumnarValue::Scalar(s) => match s {
                            match_scalar!(Some(v), $scalar_enum) => {
                                b.append_value(v.clone())?;
                                found = true;
                                break;
                            }
                            match_scalar!(None, $scalar_enum) => {}
                            _ => {
                                return Err(DataFusionError::Internal(
                                    "expected scalar of the same type".to_string(),
                                ))
                            }
                        },
                    }
                }
                if !found {
                    // All values were null.
                    b.append_null()?;
                }
            }
            Ok(Arc::new(b.finish()))
        }};
    }
    cube_match_array!(start, apply_coalesce)
}
