use std::any::type_name;
use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, BooleanArray, BooleanBuilder, GenericStringArray,
            IntervalDayTimeBuilder, PrimitiveArray, StringBuilder, UInt32Builder,
        },
        compute::cast,
        datatypes::{
            DataType, Int32Type, Int64Type, IntervalDayTimeType, IntervalUnit, TimeUnit,
            TimestampNanosecondType, UInt64Type,
        },
    },
    error::DataFusionError,
    logical_plan::create_udf,
    physical_plan::{
        functions::{make_scalar_function, ReturnTypeFunction, Signature, Volatility},
        udf::ScalarUDF,
    },
};

use crate::{
    compile::engine::df::{
        coerce::{if_coercion, least_coercion},
        columar::if_then_else,
    },
    sql::SessionState,
};

pub fn create_version_udf() -> ScalarUDF {
    let version = make_scalar_function(|_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        builder.append_value("8.0.25").unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "version",
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        version,
    )
}

pub fn create_db_udf(name: String, state: Arc<SessionState>) -> ScalarUDF {
    let db_state = state.database().unwrap_or("db".to_string());

    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        builder.append_value(db_state.clone()).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        name.as_str(),
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        version,
    )
}

pub fn create_user_udf(state: Arc<SessionState>) -> ScalarUDF {
    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        if let Some(user) = &state.user() {
            builder.append_value(user.clone() + "@127.0.0.1").unwrap();
        } else {
            builder.append_null()?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "user",
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        version,
    )
}

pub fn create_current_user_udf(state: Arc<SessionState>) -> ScalarUDF {
    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        if let Some(user) = &state.user() {
            builder.append_value(user.clone() + "@%").unwrap();
        } else {
            builder.append_null()?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "current_user",
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        version,
    )
}

pub fn create_connection_id_udf(state: Arc<SessionState>) -> ScalarUDF {
    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = UInt32Builder::new(1);
        builder.append_value(state.connection_id).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "connection_id",
        vec![],
        Arc::new(DataType::UInt32),
        Volatility::Immutable,
        version,
    )
}

#[allow(unused_macros)]
macro_rules! downcast_boolean_arr {
    ($ARG:expr) => {{
        $ARG.as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast to {}",
                    type_name::<BooleanArray>()
                ))
            })?
    }};
}

#[allow(unused_macros)]
macro_rules! downcast_primitive_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<PrimitiveArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<PrimitiveArray<$T>>()
                ))
            })?
    }};
}

#[allow(unused_macros)]
macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<GenericStringArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} to {}",
                    $NAME,
                    type_name::<GenericStringArray<$T>>()
                ))
            })?
    }};
}

// Returns the position of the first occurrence of substring substr in string str.
// This is the same as the two-argument form of LOCATE(), except that the order of
// the arguments is reversed.
pub fn create_instr_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let string_arr = downcast_string_arg!(args[0], "str", i32);
        let substring_arr = downcast_string_arg!(args[1], "substr", i32);

        let result = string_arr
            .iter()
            .zip(substring_arr.iter())
            .map(|(string, substring)| match (string, substring) {
                (Some(string), Some(substring)) => {
                    if let Some(idx) = string.to_string().find(substring) {
                        Some((idx as i32) + 1)
                    } else {
                        Some(0)
                    }
                }
                _ => Some(0),
            })
            .collect::<PrimitiveArray<Int32Type>>();

        Ok(Arc::new(result) as ArrayRef)
    });

    create_udf(
        "instr",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        fun,
    )
}

// LOCATE( substring, string, [start_position ] )
// This is the same as INSTR(), except that the order of
// the arguments is reversed.
pub fn create_locate_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let substring_arr = downcast_string_arg!(args[0], "substring", i32);
        let string_arr = downcast_string_arg!(args[1], "string", i32);

        let result = string_arr
            .iter()
            .zip(substring_arr.iter())
            .map(|(string, substring)| match (string, substring) {
                (Some(string), Some(substring)) => {
                    if let Some(idx) = string.to_string().find(substring) {
                        Some((idx as i32) + 1)
                    } else {
                        Some(0)
                    }
                }
                _ => Some(0),
            })
            .collect::<PrimitiveArray<Int32Type>>();

        Ok(Arc::new(result) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "locate",
        &Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_ucase_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let string_arr = downcast_string_arg!(args[0], "str", i32);
        let result = string_arr
            .iter()
            .map(|string| string.map(|string| string.to_ascii_uppercase()))
            .collect::<GenericStringArray<i32>>();

        Ok(Arc::new(result) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "ucase",
        &Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_isnull_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let mut builder = BooleanBuilder::new(1);
        builder.append_value(args[0].is_null(0))?;

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "isnull",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_if_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        let condition = &args[0];
        let left = &args[1];
        let right = &args[2];

        let return_type = if_coercion(left.data_type(), right.data_type()).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Positive and negative results must be the same type, actual: [{}, {}]",
                left.data_type(),
                right.data_type(),
            ))
        })?;

        let cond_array = match condition.data_type() {
            // // Arrow doesnt support UTF8 -> Boolean cast
            DataType::Utf8 => {
                let cond_array = downcast_string_arg!(condition, "condition", i32);
                let mut result = BooleanBuilder::new(cond_array.len());

                for i in 0..cond_array.len() {
                    if condition.is_null(i) {
                        result.append_value(false)?;
                    } else {
                        result.append_value(true)?;
                    }
                }

                Arc::new(result.finish()) as ArrayRef
            }
            _ => cast(&condition, &DataType::Boolean)?,
        };

        let left = cast(&left, &return_type)?;
        let right = cast(&right, &return_type)?;

        let result = if_then_else(
            &cond_array.as_any().downcast_ref::<BooleanArray>().unwrap(),
            left,
            right,
            &return_type,
        )?;

        Ok(result)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() == 3);

        let base_type = if_coercion(&types[1], &types[2]).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Positive and negative results must be the same type, actual: [{}, {}]",
                &types[1], &types[2],
            ))
        })?;

        Ok(Arc::new(base_type))
    });

    ScalarUDF::new(
        "if",
        &Signature::any(3, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

// LEAST() function in MySQL is used to find smallest values from given arguments respectively. If any given value is NULL, it return NULLs. Otherwise it returns the smallest value.
pub fn create_least_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let left = &args[0];
        let right = &args[1];

        let base_type = least_coercion(&left.data_type(), &right.data_type()).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Unable to coercion types, actual: [{}, {}]",
                &left.data_type(),
                &right.data_type(),
            ))
        })?;

        let result = if left.is_null(0) {
            cast(&left, &base_type)?
        } else if right.is_null(0) {
            cast(&right, &base_type)?
        } else {
            let l = cast(&left, &base_type)?;
            let r = cast(&right, &base_type)?;

            let is_less = match &left.data_type() {
                DataType::UInt64 => {
                    let l = downcast_primitive_arg!(l, "left", UInt64Type);
                    let r = downcast_primitive_arg!(r, "right", UInt64Type);

                    l.value(0) < r.value(0)
                }
                DataType::Int64 => {
                    let l = downcast_primitive_arg!(l, "left", Int64Type);
                    let r = downcast_primitive_arg!(r, "right", Int64Type);

                    l.value(0) < r.value(0)
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "unsupported type in least function, actual: {}",
                        left.data_type()
                    )));
                }
            };

            if is_less {
                l
            } else {
                r
            }
        };

        Ok(result)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() == 2);

        if types[0] == DataType::Null || types[1] == DataType::Null {
            return Ok(Arc::new(DataType::Null));
        }

        let base_type = least_coercion(&types[0], &types[1]).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Unable to coercion types, actual: [{}, {}]",
                &types[0], &types[1],
            ))
        })?;

        Ok(Arc::new(base_type))
    });

    ScalarUDF::new(
        "least",
        &Signature::any(2, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

// CONVERT_TZ() converts a datetime value dt from the time zone given by from_tz to the time zone given by to_tz and returns the resulting value.
pub fn create_convert_tz_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        let input_dt = &args[0];
        let from_tz = &args[1];
        let to_tz = &args[2];

        let (_, input_tz) = match input_dt.data_type() {
            DataType::Timestamp(unit, tz) => (unit, tz),
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "dt argument must be a Timestamp, actual: {}",
                    from_tz.data_type()
                )));
            }
        };

        if from_tz.data_type() == &DataType::UInt8 {
            return Err(DataFusionError::Execution(format!(
                "from_tz argument must be a Utf8, actual: {}",
                from_tz.data_type()
            )));
        };

        if to_tz.data_type() == &DataType::UInt8 {
            return Err(DataFusionError::Execution(format!(
                "to_tz argument must be a Utf8, actual: {}",
                to_tz.data_type()
            )));
        };

        let from_tz = downcast_string_arg!(&from_tz, "from_tz", i32);
        let to_tz = downcast_string_arg!(&to_tz, "to_tz", i32);

        if from_tz.value(0) != "SYSTEM" || to_tz.value(0) != "+00:00" {
            return Err(DataFusionError::NotImplemented(format!(
                "convert_tz is not implemented, it's stub"
            )));
        }

        if input_tz.is_some() {
            return Err(DataFusionError::NotImplemented(format!(
                "convert_tz is not implemented, it's stub"
            )));
        };

        Ok(input_dt.clone())
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() == 3);

        Ok(Arc::new(types[0].clone()))
    });

    ScalarUDF::new(
        "convert_tz",
        &Signature::any(3, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_timediff_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let left_dt = &args[0];
        let right_dt = &args[1];

        let left_date = match left_dt.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = downcast_primitive_arg!(left_dt, "left_dt", TimestampNanosecondType);
                let ts = arr.value(0);

                // NaiveDateTime::from_timestamp(ts, 0)
                ts
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "left_dt argument must be a Timestamp, actual: {}",
                    left_dt.data_type()
                )));
            }
        };

        let right_date = match right_dt.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = downcast_primitive_arg!(right_dt, "right_dt", TimestampNanosecondType);
                arr.value(0)
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "right_dt argument must be a Timestamp, actual: {}",
                    right_dt.data_type()
                )));
            }
        };

        let diff = right_date - left_date;
        if diff != 0 {
            return Err(DataFusionError::NotImplemented(format!(
                "timediff is not implemented, it's stub"
            )));
        }

        let mut interal_arr = IntervalDayTimeBuilder::new(1);
        interal_arr.append_value(diff)?;

        Ok(Arc::new(interal_arr.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Interval(IntervalUnit::DayTime))));

    ScalarUDF::new(
        "timediff",
        &Signature::any(2, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_time_format_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let input_dt = &args[0];
        let format = &args[1];

        let input_date = match input_dt.data_type() {
            DataType::Interval(IntervalUnit::DayTime) => {
                let arr = downcast_primitive_arg!(input_dt, "left_dt", IntervalDayTimeType);
                arr.value(0)
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "left_dt argument must be a Timestamp, actual: {}",
                    input_dt.data_type()
                )));
            }
        };

        let format = match format.data_type() {
            DataType::Utf8 => {
                let arr = downcast_string_arg!(format, "format", i32);
                arr.value(0)
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "format argument must be a Timestamp, actual: {}",
                    format.data_type()
                )));
            }
        };

        if format != "%H:%i" {
            return Err(DataFusionError::Plan(format!(
                "unsupported format, actual: {}",
                format
            )));
        }

        if input_date != 0 {
            return Err(DataFusionError::NotImplemented(format!(
                "time_format is not implemented, it's stub"
            )));
        }

        let mut result = StringBuilder::new(1);
        result.append_value("00:00".to_string())?;

        Ok(Arc::new(result.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "time_format",
        &Signature::any(2, Volatility::Immutable),
        &return_type,
        &fun,
    )
}
