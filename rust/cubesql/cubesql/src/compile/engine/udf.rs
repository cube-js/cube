use std::{any::type_name, collections::HashMap, sync::Arc, thread};

use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime};
use datafusion::{
    arrow::{
        array::{
            new_null_array, Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder,
            Float64Array, GenericStringArray, Int32Builder, Int64Array, Int64Builder,
            IntervalDayTimeBuilder, ListArray, ListBuilder, PrimitiveArray, PrimitiveBuilder,
            StringArray, StringBuilder, StructBuilder, TimestampMicrosecondArray,
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
            UInt32Builder,
        },
        compute::{cast, concat},
        datatypes::{
            DataType, Date32Type, Field, Float64Type, Int32Type, Int64Type, IntervalDayTimeType,
            IntervalUnit, IntervalYearMonthType, TimeUnit, TimestampNanosecondType, UInt32Type,
            UInt64Type,
        },
    },
    error::{DataFusionError, Result},
    logical_plan::{create_udaf, create_udf},
    physical_plan::{
        functions::{
            datetime_expressions::date_trunc, make_scalar_function, make_table_function, Signature,
            TypeSignature, Volatility,
        },
        udaf::AggregateUDF,
        udf::ScalarUDF,
        udtf::TableUDF,
        ColumnarValue,
    },
    scalar::ScalarValue,
};
use itertools::izip;
use pg_srv::{PgType, PgTypeId};
use regex::Regex;

use crate::{
    compile::engine::df::{
        coerce::{if_coercion, least_coercion},
        columar::if_then_else,
    },
    sql::SessionState,
};

pub type ReturnTypeFunction = Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;

pub fn create_version_udf(v: String) -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        builder.append_value(v.to_string()).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "version",
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_db_udf(name: String, state: Arc<SessionState>) -> ScalarUDF {
    let db_state = state.database().unwrap_or("db".to_string());

    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        builder.append_value(db_state.clone()).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        name.as_str(),
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

// It's the same as current_user UDF, but with another host
pub fn create_user_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
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
        fun,
    )
}

pub fn create_current_user_udf(state: Arc<SessionState>, name: &str, with_host: bool) -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        if let Some(user) = &state.user() {
            if with_host {
                builder.append_value(user.clone() + "@%").unwrap();
            } else {
                builder.append_value(user.clone()).unwrap();
            }
        } else {
            builder.append_null()?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        name,
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_session_user_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        if let Some(user) = &state.user() {
            builder.append_value(user.clone()).unwrap();
        } else {
            builder.append_null()?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "session_user",
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_connection_id_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = UInt32Builder::new(1);
        builder.append_value(state.connection_id).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "connection_id",
        vec![],
        Arc::new(DataType::UInt32),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_backend_pid_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = UInt32Builder::new(1);
        builder.append_value(state.connection_id).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "pg_backend_pid",
        vec![],
        Arc::new(DataType::UInt32),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_current_schema_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);

        builder.append_value("public").unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "current_schema",
        vec![],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

macro_rules! downcast_boolean_arr {
    ($ARG:expr, $NAME:expr) => {{
        $ARG.as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<BooleanArray>()
                ))
            })?
    }};
}

macro_rules! downcast_primitive_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<PrimitiveArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<$T>()
                ))
            })?
    }};
}

macro_rules! downcast_string_arg {
    ($ARG:expr, $NAME:expr, $T:ident) => {{
        $ARG.as_any()
            .downcast_ref::<GenericStringArray<$T>>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast {} from {} to {}",
                    $NAME,
                    $ARG.data_type(),
                    type_name::<GenericStringArray<$T>>()
                ))
            })?
    }};
}

macro_rules! downcast_list_arg {
    ($ARG:expr, $NAME:expr) => {{
        $ARG.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "could not cast {} to ArrayList, actual: {}",
                $NAME,
                $ARG.data_type()
            ))
        })?
    }};
}

type OidType = UInt32Type;

// TODO: Combine with downcast
fn cast_oid_arg(argument: &ArrayRef, name: &str) -> Result<ArrayRef> {
    match argument.data_type() {
        DataType::Int32 | DataType::Int64 => {
            cast(&argument, &DataType::UInt32).map_err(|err| err.into())
        }
        // We use UInt32 for OID
        DataType::UInt32 => Ok(argument.clone()),
        dt => Err(DataFusionError::Internal(format!(
            "Argument {} must be a valid numeric type accepted for oid, actual {}",
            name, dt,
        ))),
    }
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

        let len = args[0].len();
        let mut builder = BooleanBuilder::new(len);
        for i in 0..len {
            builder.append_value(args[0].is_null(i))?;
        }

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

        if let Some(tz) = input_tz {
            if tz != &"UTC" {
                return Err(DataFusionError::NotImplemented(format!(
                    "convert_tz does not non UTC timezone as input, actual {}",
                    tz
                )));
            };
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

// https://docs.aws.amazon.com/redshift/latest/dg/r_DATEDIFF_function.html
pub fn create_datediff_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        return Err(DataFusionError::NotImplemented(format!(
            "datediff is not implemented, it's stub"
        )));
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "datediff",
        &Signature::any(3, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

// https://docs.aws.amazon.com/redshift/latest/dg/r_DATEADD_function.html
pub fn create_dateadd_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        return Err(DataFusionError::NotImplemented(format!(
            "dateadd is not implemented, it's stub"
        )));
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "dateadd",
        &Signature::any(3, Volatility::Immutable),
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

pub fn create_date_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let mut args = args
            .into_iter()
            .map(|i| -> Result<ColumnarValue> {
                if let Some(strings) = i.as_any().downcast_ref::<StringArray>() {
                    let mut builder = TimestampNanosecondArray::builder(strings.len());
                    for i in 0..strings.len() {
                        builder.append_value(
                            NaiveDateTime::parse_from_str(strings.value(i), "%Y-%m-%d %H:%M:%S%.f")
                                .map_err(|e| DataFusionError::Execution(e.to_string()))?
                                .timestamp_nanos(),
                        )?;
                    }
                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                } else {
                    assert!(i
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .is_some());
                    Ok(ColumnarValue::Array(i.clone()))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        args.insert(
            0,
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("day".to_string()))),
        );

        let res = date_trunc(args.as_slice())?;
        match res {
            ColumnarValue::Array(a) => Ok(a),
            ColumnarValue::Scalar(_) => Err(DataFusionError::Internal(
                "Date trunc returned scalar value for array input".to_string(),
            )),
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));

    ScalarUDF::new(
        "date",
        &Signature::uniform(
            1,
            vec![
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                DataType::Utf8,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_makedate_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    ScalarUDF::new(
        "makedate",
        &Signature::exact(
            vec![DataType::Int64, DataType::Int64],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_year_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "year",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_quarter_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "quarter",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_dayofweek_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "dayofweek",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_dayofmonth_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "dayofmonth",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_dayofyear_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "dayofyear",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_hour_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "hour",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_minute_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "minute",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_second_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| todo!("Not implemented"));

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "second",
        &Signature::exact(
            vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

macro_rules! date_math_udf {
    ($ARGS: expr, $FIRST_ARG_TYPE: ident, $SECOND_ARG_TYPE: ident, $FUN: ident, $IS_ADD: expr) => {{
        let timestamps = downcast_primitive_arg!(&$ARGS[0], "datetime", $FIRST_ARG_TYPE);
        let intervals = downcast_primitive_arg!(&$ARGS[1], "interval", $SECOND_ARG_TYPE);
        let mut builder = TimestampNanosecondArray::builder(timestamps.len());
        for i in 0..timestamps.len() {
            if timestamps.is_null(i) {
                builder.append_null()?;
            } else {
                let timestamp = timestamps.value_as_datetime(i).unwrap();
                let interval = intervals.value(i).into();
                builder.append_value($FUN(timestamp, interval, $IS_ADD)?.timestamp_nanos())?;
            }
        }
        return Ok(Arc::new(builder.finish()));
    }};
}

pub fn create_date_add_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| match &args[1].data_type() {
        DataType::Interval(IntervalUnit::DayTime) => {
            date_math_udf!(
                args,
                TimestampNanosecondType,
                IntervalDayTimeType,
                date_addsub_day_time,
                true
            )
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            date_math_udf!(
                args,
                TimestampNanosecondType,
                IntervalYearMonthType,
                date_addsub_year_month,
                true
            )
        }
        _ => Err(DataFusionError::Execution(format!(
            "unsupported interval type"
        ))),
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));

    ScalarUDF::new(
        "date_add",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::DayTime),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                    DataType::Interval(IntervalUnit::DayTime),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_date_sub_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        match (&args[0].data_type(), &args[1].data_type()) {
            (DataType::Timestamp(..), DataType::Interval(IntervalUnit::DayTime)) => {
                date_math_udf!(
                    args,
                    TimestampNanosecondType,
                    IntervalDayTimeType,
                    date_addsub_day_time,
                    false
                )
            }
            (DataType::Timestamp(..), DataType::Interval(IntervalUnit::YearMonth)) => {
                date_math_udf!(
                    args,
                    TimestampNanosecondType,
                    IntervalYearMonthType,
                    date_addsub_year_month,
                    false
                )
            }
            (DataType::Date32, DataType::Interval(IntervalUnit::DayTime)) => {
                date_math_udf!(
                    args,
                    Date32Type,
                    IntervalDayTimeType,
                    date_addsub_day_time,
                    false
                )
            }
            (DataType::Date32, DataType::Interval(IntervalUnit::YearMonth)) => {
                date_math_udf!(
                    args,
                    Date32Type,
                    IntervalYearMonthType,
                    date_addsub_year_month,
                    false
                )
            }
            _ => Err(DataFusionError::Execution(format!(
                "unsupported interval type"
            ))),
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));

    ScalarUDF::new(
        "date_sub",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::DayTime),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                    DataType::Interval(IntervalUnit::DayTime),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Date32,
                    DataType::Interval(IntervalUnit::DayTime),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Date32,
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

fn date_addsub_year_month(t: NaiveDateTime, i: i32, is_add: bool) -> Result<NaiveDateTime> {
    let i = match is_add {
        true => i,
        false => -i,
    };

    let mut year = t.year();
    // Note month is numbered 0..11 in this function.
    let mut month = t.month() as i32 - 1;

    year += i / 12;
    month += i % 12;

    if month < 0 {
        year -= 1;
        month += 12;
    }
    debug_assert!(0 <= month);
    year += month / 12;
    month = month % 12;

    match change_ym(t, year, 1 + month as u32) {
        Some(t) => return Ok(t),
        None => {
            return Err(DataFusionError::Execution(format!(
                "Failed to set date to ({}-{})",
                year,
                1 + month
            )))
        }
    };
}

fn date_addsub_day_time(t: NaiveDateTime, interval: i64, is_add: bool) -> Result<NaiveDateTime> {
    let i = match is_add {
        true => interval,
        false => -interval,
    };

    let days: i64 = i.signum() * (i.abs() >> 32);
    let millis: i64 = i.signum() * ((i.abs() << 32) >> 32);
    return Ok(t + Duration::days(days) + Duration::milliseconds(millis));
}

fn change_ym(t: NaiveDateTime, y: i32, m: u32) -> Option<NaiveDateTime> {
    debug_assert!(1 <= m && m <= 12);
    let mut d = t.day();
    d = d.min(last_day_of_month(y, m));
    t.with_day(1)?.with_year(y)?.with_month(m)?.with_day(d)
}

fn last_day_of_month(y: i32, m: u32) -> u32 {
    debug_assert!(1 <= m && m <= 12);
    if m == 12 {
        return 31;
    }
    NaiveDate::from_ymd(y, m + 1, 1).pred().day()
}

fn postgres_datetime_format_to_iso(format: String) -> String {
    format
        .replace("%i", "%M")
        .replace("%s", "%S")
        .replace(".%f", "%.f")
        .replace("YYYY", "%Y")
        .replace("yyyy", "%Y")
        .replace("DD", "%d")
        .replace("dd", "%d")
        .replace("HH24", "%H")
        .replace("MI", "%M")
        .replace("mi", "%M")
        .replace("SS", "%S")
        .replace("ss", "%S")
        .replace(".US", "%.f")
        .replace("MM", "%m")
        .replace(".MS", "%.3f")
}

pub fn create_str_to_date_udf() -> ScalarUDF {
    let fun: Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync> =
        Arc::new(move |args: &[ColumnarValue]| {
            let timestamp = match &args[0] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => value,
                _ => {
                    todo!()
                }
            };

            let format = match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) => value,
                ColumnarValue::Scalar(value) => {
                    return Err(DataFusionError::Execution(format!(
                        "Expected string but got {:?} as a format param",
                        value
                    )))
                }
                ColumnarValue::Array(_) => {
                    return Err(DataFusionError::Execution(
                        "Array is not supported for format param in str_to_date".to_string(),
                    ))
                }
            };

            let format = postgres_datetime_format_to_iso(format.clone());

            let res = NaiveDateTime::parse_from_str(timestamp, &format).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Error evaluating str_to_date('{}', '{}'): {}",
                    timestamp,
                    format,
                    e.to_string()
                ))
            })?;

            Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(res.timestamp_nanos()),
                None,
            )))
        });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Millisecond, None))));

    ScalarUDF::new(
        "str_to_date",
        &Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_current_timestamp_udf(name: &str) -> ScalarUDF {
    let fun: Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync> =
        Arc::new(move |_| panic!("Should be rewritten with UtcTimestamp function"));

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));

    ScalarUDF::new(
        name,
        &Signature::exact(vec![], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

macro_rules! parse_timestamp_arr {
    ($ARR:expr, $ARR_TYPE: ident, $FN_NAME: ident) => {{
        let arr = $ARR.as_any().downcast_ref::<$ARR_TYPE>();
        if arr.is_some() {
            let mut result = Vec::new();
            let arr = arr.unwrap();
            for i in 0..arr.len() {
                result.push(Duration::$FN_NAME(arr.value(i)));
            }

            Some(result)
        } else {
            None
        }
    }};
}

pub fn create_to_char_udf() -> ScalarUDF {
    let fun: Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync> =
        make_scalar_function(move |args: &[ArrayRef]| {
            let arr = &args[0];
            let (durations, timezone) = match arr.data_type() {
                DataType::Timestamp(TimeUnit::Nanosecond, str) => (
                    parse_timestamp_arr!(arr, TimestampNanosecondArray, nanoseconds),
                    str.clone().unwrap_or_default(),
                ),
                DataType::Timestamp(TimeUnit::Millisecond, str) => (
                    parse_timestamp_arr!(arr, TimestampMillisecondArray, milliseconds),
                    str.clone().unwrap_or_default(),
                ),
                DataType::Timestamp(TimeUnit::Microsecond, str) => (
                    parse_timestamp_arr!(arr, TimestampMicrosecondArray, microseconds),
                    str.clone().unwrap_or_default(),
                ),
                DataType::Timestamp(TimeUnit::Second, str) => (
                    parse_timestamp_arr!(arr, TimestampSecondArray, seconds),
                    str.clone().unwrap_or_default(),
                ),
                _ => (None, "".to_string()),
            };

            if durations.is_none() {
                return Err(DataFusionError::Execution(
                    "unsupported datetime format for to_char".to_string(),
                ));
            }

            let durations = durations.unwrap();
            let formats = downcast_string_arg!(&args[1], "format_str", i32);

            let mut builder = StringBuilder::new(durations.len());

            for (i, duration) in durations.iter().enumerate() {
                let format = formats.value(i);
                let replaced_format =
                    postgres_datetime_format_to_iso(format.to_string()).replace("TZ", &timezone);

                let secs = duration.num_seconds();
                let nanosecs = duration.num_nanoseconds().unwrap_or(0) - secs * 1_000_000_000;
                let timestamp = NaiveDateTime::from_timestamp(secs, nanosecs as u32);

                builder
                    .append_value(timestamp.format(&replaced_format).to_string())
                    .unwrap();
            }

            Ok(Arc::new(builder.finish()) as ArrayRef)
        });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "to_char",
        &Signature::any(2, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_current_schemas_udf() -> ScalarUDF {
    let current_schemas = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let primitive_builder = StringBuilder::new(2);
        let mut builder = ListBuilder::new(primitive_builder);

        let including_implicit = downcast_boolean_arr!(&args[0], "implicit");
        for i in 0..including_implicit.len() {
            if including_implicit.value(i) {
                builder.values().append_value("pg_catalog").unwrap();
            }
            builder.values().append_value("public").unwrap();
            builder.append(true).unwrap();
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "current_schemas",
        vec![DataType::Boolean],
        Arc::new(DataType::List(Box::new(Field::new(
            "item",
            DataType::Utf8,
            true,
        )))),
        Volatility::Immutable,
        current_schemas,
    )
}

pub fn create_format_type_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let tmp = cast_oid_arg(&args[0], "oid")?;
        let oids = downcast_primitive_arg!(tmp, "oid", OidType);
        // TODO: See pg_attribute.atttypmod
        let typemods = downcast_primitive_arg!(args[1], "typemod", Int64Type);

        let result = oids
            .iter()
            .zip(typemods.iter())
            .map(|args| match args {
                (Some(oid), typemod) => Some(match PgTypeId::from_oid(oid) {
                    Some(type_id) => {
                        let typemod_str = || match type_id {
                            PgTypeId::BPCHAR | PgTypeId::VARCHAR => match typemod {
                                Some(typemod) if typemod >= 5 => format!("({})", typemod - 4),
                                _ => "".to_string(),
                            },
                            PgTypeId::NUMERIC => match typemod {
                                Some(typemod) if typemod >= 4 => format!("(0,{})", typemod - 4),
                                Some(typemod) if typemod >= 0 => {
                                    format!("(65535,{})", 65532 + typemod)
                                }
                                _ => "".to_string(),
                            },
                            _ => match typemod {
                                Some(typemod) if typemod >= 0 => format!("({})", typemod),
                                _ => "".to_string(),
                            },
                        };

                        match type_id {
                            PgTypeId::UNSPECIFIED => "-".to_string(),
                            PgTypeId::BOOL => "boolean".to_string(),
                            PgTypeId::BYTEA => format!("bytea{}", typemod_str()),
                            PgTypeId::NAME => format!("name{}", typemod_str()),
                            PgTypeId::INT8 => "bigint".to_string(),
                            PgTypeId::INT2 => "smallint".to_string(),
                            PgTypeId::INT4 => "integer".to_string(),
                            PgTypeId::TEXT => format!("text{}", typemod_str()),
                            PgTypeId::OID => format!("oid{}", typemod_str()),
                            PgTypeId::TID => format!("tid{}", typemod_str()),
                            PgTypeId::PGCLASS => format!("pg_class{}", typemod_str()),
                            PgTypeId::FLOAT4 => "real".to_string(),
                            PgTypeId::FLOAT8 => "double precision".to_string(),
                            PgTypeId::MONEY => format!("money{}", typemod_str()),
                            PgTypeId::INET => format!("inet{}", typemod_str()),
                            PgTypeId::ARRAYBOOL => "boolean[]".to_string(),
                            PgTypeId::ARRAYBYTEA => format!("bytea{}[]", typemod_str()),
                            PgTypeId::ARRAYINT2 => "smallint[]".to_string(),
                            PgTypeId::ARRAYINT4 => "integer[]".to_string(),
                            PgTypeId::ARRAYTEXT => format!("text{}[]", typemod_str()),
                            PgTypeId::ARRAYINT8 => "bigint[]".to_string(),
                            PgTypeId::ARRAYFLOAT4 => "real[]".to_string(),
                            PgTypeId::ARRAYFLOAT8 => "double precision[]".to_string(),
                            PgTypeId::ACLITEM => format!("aclitem{}", typemod_str()),
                            PgTypeId::ARRAYACLITEM => format!("aclitem{}[]", typemod_str()),
                            PgTypeId::BPCHAR => match typemod {
                                Some(typemod) if typemod < 0 => "bpchar".to_string(),
                                _ => format!("character{}", typemod_str()),
                            },
                            PgTypeId::VARCHAR => format!("character varying{}", typemod_str()),
                            PgTypeId::DATE => format!("date{}", typemod_str()),
                            PgTypeId::TIME => format!("time{} without time zone", typemod_str()),
                            PgTypeId::TIMESTAMP => {
                                format!("timestamp{} without time zone", typemod_str())
                            }
                            PgTypeId::TIMESTAMPTZ => {
                                format!("timestamp{} with time zone", typemod_str())
                            }
                            PgTypeId::INTERVAL => match typemod {
                                Some(typemod) if typemod >= 0 => "-".to_string(),
                                _ => "interval".to_string(),
                            },
                            PgTypeId::TIMETZ => format!("time{} with time zone", typemod_str()),
                            PgTypeId::NUMERIC => format!("numeric{}", typemod_str()),
                            PgTypeId::RECORD => format!("record{}", typemod_str()),
                            PgTypeId::ANYARRAY => format!("anyarray{}", typemod_str()),
                            PgTypeId::ANYELEMENT => format!("anyelement{}", typemod_str()),
                            PgTypeId::PGLSN => format!("pg_lsn{}", typemod_str()),
                            PgTypeId::ANYENUM => format!("anyenum{}", typemod_str()),
                            PgTypeId::ANYRANGE => format!("anyrange{}", typemod_str()),
                            PgTypeId::INT4RANGE => format!("int4range{}", typemod_str()),
                            PgTypeId::NUMRANGE => format!("numrange{}", typemod_str()),
                            PgTypeId::TSRANGE => format!("tsrange{}", typemod_str()),
                            PgTypeId::TSTZRANGE => format!("tstzrange{}", typemod_str()),
                            PgTypeId::DATERANGE => format!("daterange{}", typemod_str()),
                            PgTypeId::INT8RANGE => format!("int8range{}", typemod_str()),
                            PgTypeId::INT4MULTIRANGE => format!("int4multirange{}", typemod_str()),
                            PgTypeId::NUMMULTIRANGE => format!("nummultirange{}", typemod_str()),
                            PgTypeId::TSMULTIRANGE => format!("tsmultirange{}", typemod_str()),
                            PgTypeId::DATEMULTIRANGE => format!("datemultirange{}", typemod_str()),
                            PgTypeId::INT8MULTIRANGE => format!("int8multirange{}", typemod_str()),
                            PgTypeId::CHARACTERDATA => {
                                format!("information_schema.character_data{}", typemod_str())
                            }
                            PgTypeId::PGCONSTRAINT => format!("pg_constraint{}", typemod_str()),
                            PgTypeId::PGNAMESPACE => {
                                format!("pg_namespace{}", typemod_str())
                            }
                            PgTypeId::SQLIDENTIFIER => {
                                format!("information_schema.sql_identifier{}", typemod_str())
                            }
                        }
                    }
                    _ => "???".to_string(),
                }),
                _ => None,
            })
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "format_type",
        &Signature::any(2, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_pg_datetime_precision_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let typids = downcast_primitive_arg!(args[0], "typid", Int64Type);
        let typmods = downcast_primitive_arg!(args[1], "typmod", Int64Type);
        let mut builder = Int64Builder::new(typids.len());
        for i in 0..typids.len() {
            let typid = typids.value(i);
            let typmod = typmods.value(i);

            // https://github.com/postgres/postgres/blob/REL_14_2/src/backend/catalog/information_schema.sql#L155
            let precision = match typid {
                1082 => Some(0),
                1083 | 1114 | 1184 | 1266 => {
                    if typmod < 0 {
                        Some(6)
                    } else {
                        Some(typmod)
                    }
                }
                1186 => {
                    if typmod < 0 || ((typmod & 65535) == 65535) {
                        Some(6)
                    } else {
                        Some(typmod & 65535)
                    }
                }
                _ => None,
            };

            if precision.is_some() {
                builder.append_value(precision.unwrap()).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }

        Ok(Arc::new(builder.finish()))
    });

    create_udf(
        "information_schema._pg_datetime_precision",
        vec![DataType::Int64, DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_numeric_precision_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let typids = downcast_primitive_arg!(args[0], "typid", Int64Type);
        let typmods = downcast_primitive_arg!(args[1], "typmod", Int64Type);
        let mut builder = Int64Builder::new(typids.len());
        for i in 0..typids.len() {
            let typid = typids.value(i);
            let typmod = typmods.value(i);

            // https://github.com/postgres/postgres/blob/REL_14_2/src/backend/catalog/information_schema.sql#L109
            let precision = match typid {
                20 => Some(64),
                21 => Some(16),
                23 => Some(32),
                700 => Some(24),
                701 => Some(53),
                1700 => match typmod {
                    -1 => None,
                    _ => Some(((typmod - 4) >> 16) & 65535),
                },
                _ => None,
            };

            if precision.is_some() {
                builder.append_value(precision.unwrap()).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }

        Ok(Arc::new(builder.finish()))
    });

    create_udf(
        "information_schema._pg_numeric_precision",
        vec![DataType::Int64, DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_truetypid_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let atttypids = downcast_primitive_arg!(args[0], "atttypid", UInt32Type);
        let typtypes = downcast_string_arg!(args[1], "typtype", i32);
        let typbasetypes = downcast_primitive_arg!(args[2], "typbasetype", UInt32Type);

        let result = izip!(atttypids, typtypes, typbasetypes)
            .map(|(atttypid, typtype, typbasetype)| match typtype {
                Some("d") => typbasetype,
                _ => atttypid,
            })
            .collect::<PrimitiveArray<UInt32Type>>();

        Ok(Arc::new(result))
    });

    create_udf(
        "information_schema._pg_truetypid",
        vec![DataType::UInt32, DataType::Utf8, DataType::UInt32],
        Arc::new(DataType::UInt32),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_truetypmod_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        // TODO: See pg_attribute.atttypmod
        let atttypmods = downcast_primitive_arg!(args[0], "atttypmod", Int64Type);
        let typtypes = downcast_string_arg!(args[1], "typtype", i32);
        // TODO: See pg_attribute.atttypmod
        let typtypmods = downcast_primitive_arg!(args[2], "typtypmod", Int64Type);

        let result = izip!(atttypmods, typtypes, typtypmods)
            .map(|(atttypmod, typtype, typtypmod)| match typtype {
                Some("d") => typtypmod,
                _ => atttypmod,
            })
            .collect::<PrimitiveArray<Int64Type>>();

        Ok(Arc::new(result))
    });

    create_udf(
        "information_schema._pg_truetypmod",
        vec![DataType::Int64, DataType::Utf8, DataType::Int64],
        // TODO: See pg_attribute.atttypmod
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_numeric_scale_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let typids = downcast_primitive_arg!(args[0], "typid", Int64Type);
        let typmods = downcast_primitive_arg!(args[1], "typmod", Int64Type);
        let mut builder = Int64Builder::new(typids.len());
        for i in 0..typids.len() {
            let typid = typids.value(i);
            let typmod = typmods.value(i);

            // https://github.com/postgres/postgres/blob/REL_14_2/src/backend/catalog/information_schema.sql#L140
            let scale = match typid {
                20 | 21 | 23 => Some(0),
                1700 => match typmod {
                    -1 => None,
                    _ => Some((typmod - 4) & 65535),
                },
                _ => None,
            };

            if scale.is_some() {
                builder.append_value(scale.unwrap()).unwrap();
            } else {
                builder.append_null().unwrap();
            }
        }

        Ok(Arc::new(builder.finish()))
    });

    create_udf(
        "information_schema._pg_numeric_scale",
        vec![DataType::Int64, DataType::Int64],
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_get_userbyid_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let role_oids = downcast_primitive_arg!(args[0], "role_oid", OidType);

        let result = role_oids
            .iter()
            .map(|oid| match oid {
                Some(10) => Some(state.user().unwrap_or("postgres".to_string())),
                Some(oid) => Some(format!("unknown (OID={})", oid)),
                _ => None,
            })
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    create_udf(
        "pg_get_userbyid",
        vec![DataType::UInt32],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_pg_get_expr_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let inputs = downcast_string_arg!(args[0], "input", i32);

        let result = inputs
            .iter()
            .map::<Option<String>, _>(|_| None)
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "pg_get_expr",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Boolean]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_pg_table_is_visible_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let oids_arr = downcast_primitive_arg!(args[0], "oid", OidType);

        let result = oids_arr
            .iter()
            .map(|oid| match oid {
                Some(_oid) => Some(true),
                _ => Some(false),
            })
            .collect::<BooleanArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "pg_table_is_visible",
        &Signature::one_of(
            vec![TypeSignature::Exact(vec![DataType::UInt32])],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_pg_sleep_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let secs_arr = downcast_primitive_arg!(args[0], "secs", Int64Type);

        if !secs_arr.is_null(0) {
            thread::sleep(core::time::Duration::new(secs_arr.value(0) as u64, 0));
        }

        let mut result = StringBuilder::new(1);
        result.append_null()?;

        Ok(Arc::new(result.finish()))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "pg_sleep",
        &Signature::exact(vec![DataType::Int64], Volatility::Volatile),
        &return_type,
        &fun,
    )
}

pub fn create_pg_type_is_visible_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let oids_arr = downcast_primitive_arg!(args[0], "oid", OidType);

        let result = oids_arr
            .iter()
            .map(|oid| match oid {
                Some(oid) => {
                    if oid >= 18000 {
                        return Some(true);
                    }

                    match PgTypeId::from_oid(oid)?.to_type().typnamespace {
                        11 | 2200 => Some(true),
                        _ => Some(false),
                    }
                }
                None => None,
            })
            .collect::<BooleanArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "pg_type_is_visible",
        &Signature::exact(vec![DataType::UInt32], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_pg_get_constraintdef_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let oids_arr = downcast_primitive_arg!(args[0], "oid", OidType);
        let result = oids_arr
            .iter()
            .map(|oid| match oid {
                Some(_oid) => Some("PRIMARY KEY (oid)".to_string()),
                _ => None,
            })
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "pg_get_constraintdef",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Boolean]),
                TypeSignature::Exact(vec![DataType::UInt32]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_measure_udaf() -> AggregateUDF {
    create_udaf(
        "measure",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|| todo!("Not implemented")),
        Arc::new(vec![DataType::Float64]),
    )
}

macro_rules! generate_series_udtf {
    ($ARGS:expr, $TYPE: ident, $PRIMITIVE_TYPE: ident) => {{
        let mut section_sizes: Vec<usize> = Vec::new();
        let l_arr = &$ARGS[0].as_any().downcast_ref::<PrimitiveArray<$TYPE>>();
        if l_arr.is_some() {
            let l_arr = l_arr.unwrap();
            let r_arr = downcast_primitive_arg!($ARGS[1], "right", $TYPE);
            let step_arr = PrimitiveArray::<$TYPE>::from_value(1 as $PRIMITIVE_TYPE, 1);
            let step_arr = if $ARGS.len() > 2 {
                downcast_primitive_arg!($ARGS[2], "step", $TYPE)
            } else {
                &step_arr
            };

            let mut builder = PrimitiveBuilder::<$TYPE>::new(1);
            for (i, (start, end)) in l_arr.iter().zip(r_arr.iter()).enumerate() {
                let step = if step_arr.len() > i {
                    step_arr.value(i)
                } else {
                    step_arr.value(0)
                };

                let start = start.unwrap();
                let end = end.unwrap();
                let mut section_size: i64 = 0;
                if start <= end && step > 0 as $PRIMITIVE_TYPE {
                    let mut current = start;
                    loop {
                        if current > end {
                            break;
                        }
                        builder.append_value(current).unwrap();

                        section_size += 1;
                        current += step;
                    }
                }
                section_sizes.push(section_size as usize);
            }

            return Ok((Arc::new(builder.finish()) as ArrayRef, section_sizes));
        }
    }};
}

pub fn create_generate_series_udtf() -> TableUDF {
    let fun = make_table_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2 || args.len() == 3);

        if args[0].as_any().downcast_ref::<Int64Array>().is_some() {
            generate_series_udtf!(args, Int64Type, i64)
        } else if args[0].as_any().downcast_ref::<Float64Array>().is_some() {
            generate_series_udtf!(args, Float64Type, f64)
        }

        Err(DataFusionError::Execution(format!("Unsupported type")))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |tp| {
        if tp.len() > 0 {
            Ok(Arc::new(tp[0].clone()))
        } else {
            Ok(Arc::new(DataType::Int64))
        }
    });

    TableUDF::new(
        "generate_series",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Int64, DataType::Int64, DataType::Int64]),
                TypeSignature::Exact(vec![DataType::Float64, DataType::Float64]),
                TypeSignature::Exact(vec![
                    DataType::Float64,
                    DataType::Float64,
                    DataType::Float64,
                ]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_unnest_udtf() -> TableUDF {
    let fun = make_table_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        match args[0].data_type() {
            DataType::List(field) => {
                let mut result = new_null_array(field.data_type(), 0);
                let rows = args[0].as_any().downcast_ref::<ListArray>().unwrap();

                let mut section_sizes: Vec<usize> = Vec::new();

                for row in rows.iter() {
                    match row {
                        None => {
                            result = concat(&[&result, &new_null_array(field.data_type(), 1)])?;

                            section_sizes.push(1);
                        }
                        Some(column_array) => {
                            result = concat(&[&result, &column_array])?;

                            section_sizes.push(column_array.len());
                        }
                    }
                }

                Ok((result, section_sizes))
            }
            dt => Err(DataFusionError::Execution(format!(
                "Unsupported argument type, argument must be a List of any type, actual: {:?}",
                dt
            ))),
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |tp| {
        if tp.len() == 1 {
            match &tp[0] {
                DataType::List(field) => Ok(Arc::new(field.data_type().clone())),
                dt => Err(DataFusionError::Execution(format!(
                    "Unsupported argument type, argument must be a List of any type, actual: {:?}",
                    dt
                ))),
            }
        } else {
            Err(DataFusionError::Execution(format!(
                "Unable to get return type for UNNEST function with arguments: {:?}",
                tp
            )))
        }
    });

    TableUDF::new(
        "unnest",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

#[allow(unused_macros)]
macro_rules! impl_array_list_fn_iter {
    ($INPUT:expr, $INPUT_DT:ty, $FN:ident) => {{
        let mut builder = PrimitiveBuilder::<$INPUT_DT>::new($INPUT.len());

        for i in 0..$INPUT.len() {
            let current_row = $INPUT.value(i);

            if $INPUT.is_null(i) {
                builder.append_null()?;
            } else {
                let arr = current_row
                    .as_any()
                    .downcast_ref::<PrimitiveArray<$INPUT_DT>>()
                    .unwrap();
                if let Some(Some(v)) = arr.into_iter().$FN() {
                    builder.append_value(v)?;
                } else {
                    builder.append_null()?;
                }
            }
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

fn create_array_lower_upper_fun(upper: bool) -> ScalarFunctionImplementation {
    make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() >= 1);

        match args[0].data_type() {
            DataType::List(_) => {}
            other => {
                return Err(DataFusionError::Execution(format!(
                    "anyarray argument must be a List of numeric values, actual: {}",
                    other
                )));
            }
        };

        let input_arr = downcast_list_arg!(args[0], "anyarray");
        let dims = if args.len() == 2 {
            Some(downcast_primitive_arg!(args[1], "dim", Int64Type))
        } else {
            None
        };

        let mut builder = Int32Builder::new(input_arr.len());

        for (idx, element) in input_arr.iter().enumerate() {
            let element_dim = if let Some(d) = dims {
                if d.is_null(idx) {
                    -1
                } else {
                    d.value(idx)
                }
            } else {
                1
            };

            if element_dim > 1 {
                return Err(DataFusionError::NotImplemented(format!(
                    "argument dim > 1 is not supported right now, actual: {}",
                    element_dim
                )));
            } else if element_dim < 1 {
                builder.append_null()?;
            } else {
                match element {
                    None => builder.append_null()?,
                    Some(arr) => {
                        if arr.len() == 0 {
                            builder.append_null()?
                        } else if upper {
                            builder.append_value(arr.len() as i32)?
                        } else {
                            // PostgreSQL allows to define array with n-based arrays,
                            // e.g. '[-7:-5]={1,2,3}'::int[], but it's not possible in the DF
                            builder.append_value(1)?
                        }
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    })
}

/// returns lower bound of the requested array dimension
/// array_lower ( anyarray, integer ) → integer
pub fn create_array_lower_udf() -> ScalarUDF {
    let fun = create_array_lower_upper_fun(false);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "array_lower",
        &Signature::one_of(
            vec![
                // array anyarray
                TypeSignature::Any(1),
                // array anyarray, bound integer
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

/// Returns the OID of the current session's temporary schema, or zero if it has none (because it has not created any temporary tables).
pub fn create_pg_my_temp_schema() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = Int64Builder::new(1);
        builder.append_value(0).unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "pg_my_temp_schema",
        &Signature::any(0, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

/// Returns true if the given OID is the OID of another session's temporary schema.
/// pg_is_other_temp_schema ( oid ) → boolean
pub fn create_pg_is_other_temp_schema() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let oids = downcast_primitive_arg!(args[0], "oid", Int64Type);
        let result = oids.iter().map(|_| Some(false)).collect::<BooleanArray>();

        Ok(Arc::new(result) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "pg_is_other_temp_schema",
        &Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

/// returns upper bound of the requested array dimension
/// array_lower ( anyarray, integer ) → integer
pub fn create_array_upper_udf() -> ScalarUDF {
    let fun = create_array_lower_upper_fun(true);

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "array_upper",
        &Signature::one_of(
            vec![
                // array anyarray
                TypeSignature::Any(1),
                // array anyarray, bound integer
                TypeSignature::Any(2),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

// generate_subscripts is a convenience function that generates the set of valid subscripts for the specified dimension of the given array.
pub fn create_generate_subscripts_udtf() -> TableUDF {
    let fun = make_table_function(move |args: &[ArrayRef]| {
        assert!(args.len() <= 3);

        let input_arr = downcast_list_arg!(args[0], "anyarray");
        let step_arr = downcast_primitive_arg!(args[1], "dim", Int64Type);
        let reverse_arr = if args.len() == 3 {
            Some(downcast_boolean_arr!(args[2], "reverse"))
        } else {
            None
        };

        let mut result = Int64Builder::new(0);
        let mut section_sizes: Vec<usize> = Vec::new();

        for i in 0..input_arr.len() {
            let current_row = input_arr.value(i);

            let (mut current_step, reverse) = if reverse_arr.is_some() {
                if reverse_arr.unwrap().value(i) {
                    (current_row.len() as i64, true)
                } else {
                    (1_i64, false)
                }
            } else {
                (1_i64, false)
            };

            for _ in 0..current_row.len() {
                result.append_value(current_step)?;

                if reverse {
                    current_step -= step_arr.value(i);
                } else {
                    current_step += step_arr.value(i);
                }
            }

            section_sizes.push(current_row.len());
        }

        Ok((Arc::new(result.finish()) as ArrayRef, section_sizes))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_tp| Ok(Arc::new(DataType::Int64)));

    TableUDF::new(
        "generate_subscripts",
        &Signature::one_of(
            vec![
                // array anyarray, dim integer
                TypeSignature::Any(2),
                // array anyarray, dim integer, reverse boolean
                TypeSignature::Any(3),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_pg_expandarray_udtf() -> TableUDF {
    let fields = || {
        vec![
            Field::new("x", DataType::Int64, true),
            Field::new("n", DataType::Int64, false),
        ]
    };

    let fun = make_table_function(move |args: &[ArrayRef]| {
        let arr = &args[0].as_any().downcast_ref::<ListArray>();
        if arr.is_none() {
            return Err(DataFusionError::Execution(format!("Unsupported type")));
        }
        let arr = arr.unwrap();

        let mut value_builder = Int64Builder::new(1);
        let mut index_builder = Int64Builder::new(1);

        let mut section_sizes: Vec<usize> = Vec::new();
        let mut total_count: usize = 0;

        for i in 0..arr.len() {
            let values = arr.value(i);
            let values_arr = values.as_any().downcast_ref::<Int64Array>();
            if values_arr.is_none() {
                return Err(DataFusionError::Execution(format!("Unsupported type")));
            }
            let values_arr = values_arr.unwrap();

            for j in 0..values_arr.len() {
                value_builder.append_value(values_arr.value(j)).unwrap();
                index_builder.append_value((j + 1) as i64).unwrap();
            }

            section_sizes.push(values_arr.len());
            total_count += values_arr.len()
        }

        let field_builders = vec![
            Box::new(value_builder) as Box<dyn ArrayBuilder>,
            Box::new(index_builder) as Box<dyn ArrayBuilder>,
        ];

        let mut builder = StructBuilder::new(fields(), field_builders);
        for _ in 0..total_count {
            builder.append(true).unwrap();
        }

        Ok((Arc::new(builder.finish()) as ArrayRef, section_sizes))
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Struct(fields()))));

    TableUDF::new(
        "information_schema._pg_expandarray",
        &Signature::exact(
            vec![DataType::List(Box::new(Field::new(
                "item",
                DataType::Int64,
                true,
            )))],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_has_schema_privilege_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let (users, schemas, privileges) = if args.len() == 3 {
            (
                Some(downcast_string_arg!(args[0], "user", i32)),
                downcast_string_arg!(args[1], "schema", i32),
                downcast_string_arg!(args[2], "privilege", i32),
            )
        } else {
            (
                None,
                downcast_string_arg!(args[0], "schema", i32),
                downcast_string_arg!(args[1], "privilege", i32),
            )
        };

        let result = izip!(schemas, privileges)
            .enumerate()
            .map(|(i, args)| {
                Ok(match args {
                    (Some(schema), Some(privilege)) => {
                        match (users, state.user()) {
                            (Some(users), Some(session_user)) => {
                                let user = users.value(i);
                                if user != session_user {
                                    return Err(DataFusionError::Execution(format!(
                                        "role \"{}\" does not exist",
                                        user
                                    )));
                                }
                            }
                            _ => (),
                        }

                        match schema {
                            "public" | "pg_catalog" | "information_schema" => (),
                            _ => {
                                return Err(DataFusionError::Execution(format!(
                                    "schema \"{}\" does not exist",
                                    schema
                                )))
                            }
                        };

                        match privilege {
                            "CREATE" => Some(false),
                            "USAGE" => Some(true),
                            _ => {
                                return Err(DataFusionError::Execution(format!(
                                    "unrecognized privilege type: \"{}\"",
                                    privilege
                                )))
                            }
                        }
                    }
                    _ => None,
                })
            })
            .collect::<Result<BooleanArray>>();

        Ok(Arc::new(result?))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "has_schema_privilege",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_pg_total_relation_size_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let relids = downcast_primitive_arg!(args[0], "relid", OidType);

        // 8192 is the lowest size for a table that has at least one column
        // TODO: check if the requested table actually exists
        let result = relids
            .iter()
            .map(|relid| relid.map(|_| 8192))
            .collect::<PrimitiveArray<Int64Type>>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "pg_total_relation_size",
        &Signature::exact(vec![DataType::UInt32], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

// Additional function which is used under the hood for CAST(x as Regclass) where
// x is a dynamic expression which doesnt allow us to use CastReplacer
pub fn create_cube_regclass_cast_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        match args[0].data_type() {
            DataType::Utf8 => {
                let string_arr = downcast_string_arg!(args[0], "expr", i32);
                let mut builder = Int64Builder::new(args[0].len());

                for value in string_arr {
                    match value {
                        None => builder.append_null()?,
                        Some(as_str) => {
                            match PgType::get_all().iter().find(|e| e.typname == as_str) {
                                None => {
                                    return Err(DataFusionError::Execution(format!(
                                        "Unable to cast expression to Regclass: Unknown type: {}",
                                        as_str
                                    )))
                                }
                                Some(ty) => {
                                    builder.append_value(ty.oid as i64)?;
                                }
                            }
                        }
                    }
                }

                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
            DataType::Int32 | DataType::UInt32 => {
                cast(&args[0], &DataType::Int64).map_err(|err| err.into())
            }
            DataType::Int64 => Ok(args[0].clone()),
            _ => Err(DataFusionError::Execution(format!(
                "CAST with Regclass doesn't support this type: {}",
                args[0].data_type()
            ))),
        }
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));

    ScalarUDF::new(
        "__cube_regclass_cast",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_pg_get_serial_sequence_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let tbl_nm_arr = downcast_string_arg!(args[0], "table", i32);

        // TODO: Checks that table/field was defined in the schema
        let res = tbl_nm_arr
            .iter()
            .map(|_| Option::<String>::None)
            .collect::<StringArray>();

        Ok(Arc::new(res) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "pg_get_serial_sequence",
        &Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_json_build_object_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_args: &[ArrayRef]| {
        // TODO: Implement
        return Err(DataFusionError::NotImplemented(format!(
            "json_build_object is not implemented, it's stub"
        )));
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "json_build_object",
        &Signature::variadic(
            vec![
                DataType::Utf8,
                DataType::Boolean,
                DataType::Int64,
                DataType::UInt32,
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

/// https://docs.aws.amazon.com/redshift/latest/dg/REGEXP_SUBSTR.html
pub fn create_regexp_substr_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let source_arr = downcast_string_arg!(args[0], "source_string", i32);
        let pattern_arr = downcast_string_arg!(args[1], "pattern", i32);
        let position_arr = if args.len() > 2 {
            Some(downcast_primitive_arg!(args[2], "position", Int64Type))
        } else {
            None
        };

        if args.len() > 3 {
            return Err(DataFusionError::NotImplemented(
                "regexp_substr does not support occurrence and parameters (flags)".to_string(),
            ));
        }

        let mut patterns: HashMap<String, Regex> = HashMap::new();
        let mut builder = StringBuilder::new(source_arr.len());

        for ((idx, source), pattern) in source_arr.iter().enumerate().zip(pattern_arr.iter()) {
            match (source, pattern) {
                (None, _) => builder.append_null()?,
                (_, None) => builder.append_null()?,
                (Some(s), Some(p)) => {
                    let input = if let Some(position) = position_arr {
                        if position.is_null(idx) {
                            builder.append_null()?;

                            continue;
                        } else {
                            let pos = position.value(idx);
                            if pos <= 1 {
                                s
                            } else if (pos as usize) > s.len() {
                                builder.append_value(&"")?;

                                continue;
                            } else {
                                &s[((pos as usize) - 1)..]
                            }
                        }
                    } else {
                        s
                    };

                    let re_pattern = if let Some(re) = patterns.get(p) {
                        re.clone()
                    } else {
                        let re = Regex::new(p).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Regular expression did not compile: {:?}",
                                e
                            ))
                        })?;
                        patterns.insert(p.to_string(), re.clone());

                        re
                    };

                    match re_pattern.captures(input) {
                        Some(caps) => {
                            if let Some(m) = caps.get(0) {
                                builder.append_value(m.as_str())?;
                            } else {
                                builder.append_value("")?
                            }
                        }
                        None => builder.append_value("")?,
                    }
                }
            };
        }

        Ok(Arc::new(builder.finish()))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "regexp_substr",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Int64]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                ]),
                TypeSignature::Exact(vec![
                    DataType::Utf8,
                    DataType::Utf8,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Utf8,
                ]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}
