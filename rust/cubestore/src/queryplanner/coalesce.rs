use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
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

// TODO: move to datafusion, use to simplify more code.
macro_rules! match_array {
    ($array: expr, $matcher: ident) => {{
        use arrow::array::*;
        use arrow::datatypes::*;
        let a = $array;
        match a.data_type() {
            DataType::Null => panic!("null type is not supported"),
            DataType::Boolean => ($matcher!(a, BooleanArray, BooleanBuilder, Boolean)),
            DataType::Int8 => ($matcher!(a, Int8Array, PrimitiveBuilder<Int8Type>, Int8)),
            DataType::Int16 => ($matcher!(a, Int16Array, PrimitiveBuilder<Int16Type>, Int16)),
            DataType::Int32 => ($matcher!(a, Int32Array, PrimitiveBuilder<Int32Type>, Int32)),
            DataType::Int64 => ($matcher!(a, Int64Array, PrimitiveBuilder<Int64Type>, Int64)),
            DataType::UInt8 => ($matcher!(a, UInt8Array, PrimitiveBuilder<UInt8Type>, UInt8)),
            DataType::UInt16 => ($matcher!(a, UInt16Array, PrimitiveBuilder<UInt16Type>, UInt16)),
            DataType::UInt32 => ($matcher!(a, UInt32Array, PrimitiveBuilder<UInt32Type>, UInt32)),
            DataType::UInt64 => ($matcher!(a, UInt64Array, PrimitiveBuilder<UInt64Type>, UInt64)),
            DataType::Float16 => panic!("float 16 is not supported"),
            DataType::Float32 => {
                ($matcher!(a, Float32Array, PrimitiveBuilder<Float32Type>, Float32))
            }
            DataType::Float64 => {
                ($matcher!(a, Float64Array, PrimitiveBuilder<Float64Type>, Float64))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                ($matcher!(
                    a,
                    TimestampSecondArray,
                    TimestampSecondBuilder,
                    TimestampSecond
                ))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                ($matcher!(
                    a,
                    TimestampMillisecondArray,
                    TimestampMillisecondBuilder,
                    TimestampMillisecond
                ))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                ($matcher!(
                    a,
                    TimestampMicrosecondArray,
                    TimestampMicrosecondBuilder,
                    TimestampMicrosecond
                ))
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                ($matcher!(
                    a,
                    TimestampNanosecondArray,
                    TimestampNanosecondBuilder,
                    TimestampNanosecond
                ))
            }
            DataType::Date32 => ($matcher!(a, Date32Array, PrimitiveBuilder<Date32Type>, Date32)),
            DataType::Date64 => ($matcher!(a, Date64Array, PrimitiveBuilder<Date64Type>, Date64)),
            DataType::Time32(_) => panic!("time32 not supported"),
            DataType::Time64(_) => panic!("time64 not supported"),
            DataType::Duration(_) => panic!("duration not supported"),
            DataType::FixedSizeBinary(_) => panic!("fixed size binary not supported"),
            DataType::Interval(IntervalUnit::YearMonth) => {
                ($matcher!(
                    a,
                    IntervalYearMonthArray,
                    PrimitiveBuilder<IntervalYearMonthType>,
                    IntervalYearMonth
                ))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                ($matcher!(
                    a,
                    IntervalDayTimeArray,
                    PrimitiveBuilder<IntervalDayTimeType>,
                    IntervalDayTime
                ))
            }
            DataType::Binary => ($matcher!(a, BinaryArray, BinaryBuilder, Binary)),
            DataType::LargeBinary => {
                ($matcher!(a, LargeBinaryArray, LargeBinaryBuilder, LargeBinary))
            }
            DataType::Utf8 => ($matcher!(a, StringArray, StringBuilder, Utf8)),
            DataType::LargeUtf8 => ($matcher!(a, LargeStringArray, LargeStringBuilder, Utf8)),
            DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
                panic!("list not supported")
            }
            DataType::Struct(_) | DataType::Union(_) => panic!("struct and union not supported"),
            DataType::Dictionary(_, _) => panic!("dictionary not supported"),
            DataType::Decimal(_, _) => panic!("decimal not supported"),
            DataType::Int64Decimal(0) => {
                ($matcher!(a, Int64Decimal0Array, Int64Decimal0Builder, Int64Decimal, 0))
            }
            DataType::Int64Decimal(1) => {
                ($matcher!(a, Int64Decimal1Array, Int64Decimal1Builder, Int64Decimal, 1))
            }
            DataType::Int64Decimal(2) => {
                ($matcher!(a, Int64Decimal2Array, Int64Decimal2Builder, Int64Decimal, 2))
            }
            DataType::Int64Decimal(3) => {
                ($matcher!(a, Int64Decimal3Array, Int64Decimal3Builder, Int64Decimal, 3))
            }
            DataType::Int64Decimal(4) => {
                ($matcher!(a, Int64Decimal4Array, Int64Decimal4Builder, Int64Decimal, 4))
            }
            DataType::Int64Decimal(5) => {
                ($matcher!(a, Int64Decimal5Array, Int64Decimal5Builder, Int64Decimal, 5))
            }
            DataType::Int64Decimal(10) => {
                ($matcher!(
                    a,
                    Int64Decimal10Array,
                    Int64Decimal10Builder,
                    Int64Decimal,
                    10
                ))
            }
            DataType::Int64Decimal(_) => panic!("unsupported scale for decimal"),
        }
    }};
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
    match_array!(start, apply_coalesce)
}
