use std::{
    any::type_name,
    sync::{Arc, LazyLock},
    thread,
};

use chrono::{Datelike, Days, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime};
use datafusion::{
    arrow::{
        array::{
            new_null_array, Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder,
            Date32Array, Float64Array, Float64Builder, GenericStringArray, Int32Builder,
            Int64Array, Int64Builder, IntervalDayTimeBuilder, IntervalMonthDayNanoArray, ListArray,
            ListBuilder, PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder,
            StructBuilder, TimestampMicrosecondArray, TimestampMillisecondArray,
            TimestampNanosecondArray, TimestampSecondArray, UInt32Builder, UInt64Builder,
        },
        compute::{cast, concat},
        datatypes::{
            ArrowPrimitiveType, DataType, Date32Type, Field, Float64Type, Int32Type, Int64Type,
            IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType,
            TimeUnit, TimestampNanosecondType, UInt32Type,
        },
    },
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    logical_plan::create_udf,
    physical_plan::{
        functions::{
            datetime_expressions::date_trunc, make_scalar_function, make_table_function, Signature,
            TypeSignature, Volatility,
        },
        udaf::AggregateUDF,
        udf::ScalarUDF,
        udtf::TableUDF,
        Accumulator, ColumnarValue,
    },
    scalar::ScalarValue,
};
use itertools::izip;
use pg_srv::{PgType, PgTypeId};
use regex::Regex;
use sha1_smol::Sha1;

use crate::{
    compile::engine::{
        df::{coerce::common_type_coercion, columar::if_then_else},
        information_schema::postgres::{PG_NAMESPACE_CATALOG_OID, PG_NAMESPACE_PUBLIC_OID},
        udf::utils::*,
    },
    sql::SessionState,
};

type IntervalDayTime = <IntervalDayTimeType as ArrowPrimitiveType>::Native;
type IntervalMonthDayNano = <IntervalMonthDayNanoType as ArrowPrimitiveType>::Native;

pub type ReturnTypeFunction = Arc<dyn Fn(&[DataType]) -> Result<Arc<DataType>> + Send + Sync>;
pub type ScalarFunctionImplementation =
    Arc<dyn Fn(&[ColumnarValue]) -> Result<ColumnarValue> + Send + Sync>;
pub type StateTypeFunction = Arc<dyn Fn(&DataType) -> Result<Arc<Vec<DataType>>> + Send + Sync>;
pub type AccumulatorFunctionImplementation =
    Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync>;

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
        // TODO: rm Utf8 && LargeUtf8 when Prepared Statements support integer types
        DataType::Int32 | DataType::Int64 | DataType::Utf8 | DataType::LargeUtf8 => {
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
        let result = match args.len() {
            1 => {
                let len = args[0].len();
                let mut builder = BooleanBuilder::new(len);
                for i in 0..len {
                    builder.append_value(args[0].is_null(i))?;
                }

                Arc::new(builder.finish()) as ArrayRef
            }
            2 => {
                let expr = match args[0].data_type() {
                    DataType::Utf8 => Arc::clone(&args[0]),
                    DataType::Null => cast(&args[0], &DataType::Utf8)?,
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "isnull with 2 arguments supports only (Utf8, Utf8), actual: ({}, {})",
                            args[0].data_type(),
                            args[1].data_type(),
                        )))
                    }
                };
                let replacement = match args[1].data_type() {
                    DataType::Utf8 => Arc::clone(&args[1]),
                    DataType::Null => cast(&args[1], &DataType::Utf8)?,
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "isnull with 2 arguments supports only (Utf8, Utf8), actual: ({}, {})",
                            args[0].data_type(),
                            args[1].data_type(),
                        )))
                    }
                };

                let exprs = downcast_string_arg!(expr, "expr", i32);
                let replacements = downcast_string_arg!(replacement, "replacement", i32);

                let result = exprs
                    .iter()
                    .zip(replacements.iter())
                    .map(|(expr, replacement)| if expr.is_some() { expr } else { replacement })
                    .collect::<StringArray>();

                Arc::new(result)
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "isnull accepts 1 or 2 arguments, actual: {}",
                    args.len(),
                )))
            }
        };
        Ok(result)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| match types.len() {
        1 => Ok(Arc::new(DataType::Boolean)),
        2 => Ok(Arc::new(types[0].clone())),
        _ => Err(DataFusionError::Internal(format!(
            "isnull accepts 1 or 2 arguments, actual: {}",
            types.len(),
        ))),
    });

    ScalarUDF::new(
        "isnull",
        &Signature::one_of(
            vec![TypeSignature::Any(1), TypeSignature::Any(2)],
            Volatility::Immutable,
        ),
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

        let return_type =
            common_type_coercion(left.data_type(), right.data_type()).ok_or_else(|| {
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

        let base_type = common_type_coercion(&types[1], &types[2]).ok_or_else(|| {
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

/// The LEAST function selects the smallest value from a list of any number of expressions.
/// The expressions must all be convertible to a common data type, which will be the type of
/// the result. The SQL standard requires greatest and least to return null in case one argument
/// is null. However, in Postgres NULL values in the list are ignored.
/// We follow Postgres: The result will be NULL only if all the expressions evaluate to NULL.
pub fn create_least_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() > 0);

        let mut base_type = DataType::Null;
        let array_len = args[0].len();

        for arg in args {
            base_type = common_type_coercion(&base_type, arg.data_type()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to coercion types, actual: [{}, {}]",
                    &base_type,
                    arg.data_type(),
                ))
            })?;
        }

        // Creating a new builder with the base_type
        let mut builder: Box<dyn ArrayBuilder> = match base_type {
            DataType::UInt64 => Box::new(UInt64Builder::new(array_len)),
            DataType::Int64 => Box::new(Int64Builder::new(array_len)),
            DataType::Float64 => Box::new(Float64Builder::new(array_len)),
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported type in greatest function, actual: {}",
                    base_type
                )));
            }
        };

        // Iterating over strings
        for i in 0..array_len {
            let mut min_value: Option<ScalarValue> = None;

            // Iterating over columns
            for arg in args {
                if arg.is_null(i) {
                    continue;
                }

                let scalar_value = ScalarValue::try_from_array(arg, i)?;

                min_value = match min_value {
                    Some(current_min) => {
                        if scalar_value < current_min {
                            Some(scalar_value)
                        } else {
                            Some(current_min)
                        }
                    }
                    None => Some(scalar_value),
                };
            }

            match min_value {
                Some(ScalarValue::UInt64(Some(v))) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .unwrap();
                    builder.append_value(v)?;
                }
                Some(ScalarValue::Int64(Some(v))) => {
                    let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                    builder.append_value(v)?;
                }
                Some(ScalarValue::Float64(Some(v))) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    builder.append_value(v)?;
                }
                _ => match base_type {
                    DataType::UInt64 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<UInt64Builder>()
                            .unwrap();
                        builder.append_null()?;
                    }
                    DataType::Int64 => {
                        let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                        builder.append_null()?;
                    }
                    DataType::Float64 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap();
                        builder.append_null()?;
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "unsupported type in greatest function, actual: {}",
                            base_type
                        )));
                    }
                },
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() > 0);

        let mut ti = types.iter();
        let mut base_type = ti.next().unwrap().clone();

        for t in ti {
            base_type = common_type_coercion(&base_type, t).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to coercion types, actual: [{}, {}]",
                    &base_type, t,
                ))
            })?;
        }

        Ok(Arc::new(base_type))
    });

    ScalarUDF::new(
        "least",
        &Signature::variadic(
            vec![DataType::Int64, DataType::UInt64, DataType::Float64],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

/// The GREATEST function selects the largest value from a list of any number of expressions.
/// The expressions must all be convertible to a common data type, which will be the type of
/// the result. The SQL standard requires greatest and least to return null in case one argument
/// is null. However, in Postgres NULL values in the list are ignored.
/// We follow Postgres: The result will be NULL only if all the expressions evaluate to NULL.
pub fn create_greatest_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() > 0);

        let mut base_type = DataType::Null;
        let array_len = args[0].len();

        for arg in args {
            base_type = common_type_coercion(&base_type, arg.data_type()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to coercion types, actual: [{}, {}]",
                    &base_type,
                    arg.data_type(),
                ))
            })?;
        }

        // Creating a new builder with the base_type
        let mut builder: Box<dyn ArrayBuilder> = match base_type {
            DataType::UInt64 => Box::new(UInt64Builder::new(array_len)),
            DataType::Int64 => Box::new(Int64Builder::new(array_len)),
            DataType::Float64 => Box::new(Float64Builder::new(array_len)),
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "unsupported type in greatest function, actual: {}",
                    base_type
                )));
            }
        };

        // Iterating over strings
        for i in 0..array_len {
            let mut max_value: Option<ScalarValue> = None;

            // Iterating over columns
            for arg in args {
                if arg.is_null(i) {
                    continue;
                }

                let scalar_value = ScalarValue::try_from_array(arg, i)?;

                max_value = match max_value {
                    Some(current_max) => {
                        if scalar_value > current_max {
                            Some(scalar_value)
                        } else {
                            Some(current_max)
                        }
                    }
                    None => Some(scalar_value),
                };
            }

            match max_value {
                Some(ScalarValue::UInt64(Some(v))) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .unwrap();
                    builder.append_value(v)?;
                }
                Some(ScalarValue::Int64(Some(v))) => {
                    let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                    builder.append_value(v)?;
                }
                Some(ScalarValue::Float64(Some(v))) => {
                    let builder = builder
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap();
                    builder.append_value(v)?;
                }
                _ => match base_type {
                    DataType::UInt64 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<UInt64Builder>()
                            .unwrap();
                        builder.append_null()?;
                    }
                    DataType::Int64 => {
                        let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                        builder.append_null()?;
                    }
                    DataType::Float64 => {
                        let builder = builder
                            .as_any_mut()
                            .downcast_mut::<Float64Builder>()
                            .unwrap();
                        builder.append_null()?;
                    }
                    _ => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "unsupported type in greatest function, actual: {}",
                            base_type
                        )));
                    }
                },
            }
        }

        let array = builder.finish();
        Ok(Arc::new(array) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() > 0);

        let mut ti = types.iter();
        let mut base_type = ti.next().unwrap().clone();

        for t in ti {
            base_type = common_type_coercion(&base_type, t).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Unable to coercion types, actual: [{}, {}]",
                    &base_type, t,
                ))
            })?;
        }

        Ok(Arc::new(base_type))
    });

    ScalarUDF::new(
        "greatest",
        &Signature::variadic(
            vec![DataType::Int64, DataType::UInt64, DataType::Float64],
            Volatility::Immutable,
        ),
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
            if tz != "UTC" {
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

pub fn create_ends_with_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let string_array = downcast_string_arg!(args[0], "string", i32);
        let prefix_array = downcast_string_arg!(args[1], "prefix", i32);

        let result = string_array
            .iter()
            .zip(prefix_array.iter())
            .map(|(string, prefix)| match (string, prefix) {
                (Some(string), Some(prefix)) => Some(string.ends_with(prefix)),
                _ => None,
            })
            .collect::<BooleanArray>();

        Ok(Arc::new(result) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "ends_with",
        &Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_to_date_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2 || args.len() == 3);

        return Err(DataFusionError::NotImplemented(format!(
            "to_date is not implemented, it's stub"
        )));
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Date32)));

    ScalarUDF::new(
        "to_date",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Boolean]),
            ],
            Volatility::Immutable,
        ),
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
            .iter()
            .map(|i| -> Result<ColumnarValue> {
                if let Some(strings) = i.as_any().downcast_ref::<StringArray>() {
                    let mut builder = TimestampNanosecondArray::builder(strings.len());
                    for i in 0..strings.len() {
                        builder.append_value(
                            NaiveDateTime::parse_from_str(strings.value(i), "%Y-%m-%d %H:%M:%S%.f")
                                .map_err(|e| DataFusionError::Execution(e.to_string()))?
                                .and_utc()
                                .timestamp_nanos_opt()
                                .unwrap(),
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
            if timestamps.is_null(i) || intervals.is_null(i) {
                builder.append_null()?;
            } else {
                let timestamp = timestamps.value_as_datetime(i).unwrap();
                let interval = intervals.value(i).into();
                builder.append_value(
                    $FUN(timestamp, interval, $IS_ADD)?
                        .and_utc()
                        .timestamp_nanos_opt()
                        .unwrap(),
                )?;
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
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            date_math_udf!(
                args,
                TimestampNanosecondType,
                IntervalMonthDayNanoType,
                date_addsub_month_day_nano,
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
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::MonthDayNano),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string())),
                    DataType::Interval(IntervalUnit::MonthDayNano),
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
    month %= 12;

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

fn date_addsub_month_day_nano(
    t: NaiveDateTime,
    i: IntervalMonthDayNano,
    is_add: bool,
) -> Result<NaiveDateTime> {
    let (month, day, nano) = IntervalMonthDayNanoType::to_parts(i);

    let result = if month > 0 && is_add || month < 0 && !is_add {
        t.checked_add_months(Months::new(month as u32))
    } else {
        t.checked_sub_months(Months::new(month.unsigned_abs()))
    };

    let result = if day > 0 && is_add || day < 0 && !is_add {
        result.and_then(|t| t.checked_add_days(Days::new(day as u64)))
    } else {
        result.and_then(|t| t.checked_sub_days(Days::new(day.unsigned_abs() as u64)))
    };

    let result = result.and_then(|t| {
        t.checked_add_signed(Duration::nanoseconds(nano * (if !is_add { -1 } else { 1 })))
    });
    result.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Failed to add interval: {} month {} day {} nano",
            month, day, nano
        ))
    })
}

fn date_addsub_day_time(
    t: NaiveDateTime,
    interval: IntervalDayTime,
    is_add: bool,
) -> Result<NaiveDateTime> {
    let (days, millis) = IntervalDayTimeType::to_parts(interval);

    let result = if days > 0 && is_add || days < 0 && !is_add {
        t.checked_add_days(Days::new(days as u64))
    } else {
        t.checked_sub_days(Days::new(days.unsigned_abs() as u64))
    };

    let result = result.and_then(|t| {
        t.checked_add_signed(Duration::milliseconds(
            millis as i64 * (if !is_add { -1 } else { 1 }),
        ))
    });
    result.ok_or_else(|| {
        DataFusionError::Execution(format!(
            "Failed to add interval: {} day {} ms",
            days, millis
        ))
    })
}

fn change_ym(t: NaiveDateTime, y: i32, m: u32) -> Option<NaiveDateTime> {
    debug_assert!((1..=12).contains(&m));
    let mut d = t.day();
    d = d.min(last_day_of_month(y, m));
    t.with_day(1)?.with_year(y)?.with_month(m)?.with_day(d)
}

fn last_day_of_month(y: i32, m: u32) -> u32 {
    debug_assert!((1..=12).contains(&m));
    if m == 12 {
        return 31;
    }
    NaiveDate::from_ymd_opt(y, m + 1, 1)
        .unwrap_or_else(|| panic!("Invalid year month: {}-{}", y, m))
        .pred_opt()
        .unwrap_or_else(|| panic!("Invalid year month: {}-{}", y, m))
        .day()
}

fn postgres_datetime_format_to_iso(format: String) -> String {
    format
        // Workaround for FM modifier
        .replace("FMDay", "%A")
        .replace("FMMonth", "%B")
        .replace("Day", "%A")
        .replace("Month", "%B")
        .replace("%i", "%M")
        .replace("%s", "%S")
        .replace(".%f", "%.f")
        .replace("YYYY", "%Y")
        .replace("yyyy", "%Y")
        // NOTE: "%q" is not a part of chrono
        .replace("Q", "%q")
        .replace("Mon", "%b")
        .replace("DD", "%d")
        .replace("dd", "%d")
        .replace("HH24", "%H")
        .replace("HH12", "%I")
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
                    "Error evaluating str_to_date('{timestamp}', '{format}'): {e}"
                ))
            })?;

            Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                Some(res.and_utc().timestamp_nanos_opt().unwrap()),
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
                result.push(chrono::Duration::$FN_NAME(arr.value(i)));
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
                dt => {
                    return Err(DataFusionError::Execution(format!(
                        "unsupported date type for to_char, actual: {}",
                        dt
                    )))
                }
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
                if duration.is_zero() {
                    builder.append_null().unwrap();
                } else {
                    let format = formats.value(i);
                    let format = postgres_datetime_format_to_iso(format.to_string())
                        .replace("TZ", &timezone);

                    let secs = duration.num_seconds();
                    let nanosecs = duration.num_nanoseconds().unwrap_or(0) - secs * 1_000_000_000;
                    let timestamp = ::chrono::DateTime::from_timestamp(secs, nanosecs as u32)
                        .map(|dt| dt.naive_utc())
                        .unwrap_or_else(|| panic!("Invalid secs {} nanosecs {}", secs, nanosecs));

                    // chrono's strftime is missing quarter format, as such a workaround is required
                    let quarter = &format!("{}", timestamp.date().month0() / 3 + 1);
                    let format = format.replace("%q", quarter);

                    builder
                        .append_value(timestamp.format(&format).to_string())
                        .unwrap();
                }
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
        let typemods = cast(&args[1], &DataType::Int64)?;
        let typemods = downcast_primitive_arg!(typemods, "typemod", Int64Type);

        let result = oids
            .iter()
            .zip(typemods.iter())
            .map(|args| match args {
                (Some(oid), typemod) => Some(match PgTypeId::from_oid(oid) {
                    Some(type_id) => {
                        let typemod_str = || match type_id {
                            PgTypeId::BPCHAR
                            | PgTypeId::VARCHAR
                            | PgTypeId::ARRAYBPCHAR
                            | PgTypeId::ARRAYVARCHAR => match typemod {
                                Some(typemod) if typemod >= 5 => format!("({})", typemod - 4),
                                _ => "".to_string(),
                            },
                            PgTypeId::NUMERIC | PgTypeId::ARRAYNUMERIC => match typemod {
                                Some(typemod) if typemod >= 4 => format!("(0,{})", typemod - 4),
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
                            PgTypeId::ARRAYPGCLASS => format!("pg_class{}[]", typemod_str()),
                            PgTypeId::FLOAT4 => "real".to_string(),
                            PgTypeId::FLOAT8 => "double precision".to_string(),
                            PgTypeId::MONEY => format!("money{}", typemod_str()),
                            PgTypeId::ARRAYMONEY => format!("money{}[]", typemod_str()),
                            PgTypeId::INET => format!("inet{}", typemod_str()),
                            PgTypeId::ARRAYBOOL => "boolean[]".to_string(),
                            PgTypeId::ARRAYBYTEA => format!("bytea{}[]", typemod_str()),
                            PgTypeId::ARRAYNAME => format!("name{}[]", typemod_str()),
                            PgTypeId::ARRAYINT2 => "smallint[]".to_string(),
                            PgTypeId::ARRAYINT4 => "integer[]".to_string(),
                            PgTypeId::ARRAYTEXT => format!("text{}[]", typemod_str()),
                            PgTypeId::ARRAYTID => format!("tid{}[]", typemod_str()),
                            PgTypeId::ARRAYBPCHAR => format!("character{}[]", typemod_str()),
                            PgTypeId::ARRAYVARCHAR => {
                                format!("character varying{}[]", typemod_str())
                            }
                            PgTypeId::ARRAYINT8 => "bigint[]".to_string(),
                            PgTypeId::ARRAYFLOAT4 => "real[]".to_string(),
                            PgTypeId::ARRAYFLOAT8 => "double precision[]".to_string(),
                            PgTypeId::ARRAYOID => format!("oid{}[]", typemod_str()),
                            PgTypeId::ACLITEM => format!("aclitem{}", typemod_str()),
                            PgTypeId::ARRAYACLITEM => format!("aclitem{}[]", typemod_str()),
                            PgTypeId::ARRAYINET => format!("inet{}[]", typemod_str()),
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
                            PgTypeId::ARRAYTIMESTAMP => {
                                format!("timestamp{} without time zone[]", typemod_str())
                            }
                            PgTypeId::ARRAYDATE => format!("date{}[]", typemod_str()),
                            PgTypeId::ARRAYTIME => {
                                format!("time{} without time zone[]", typemod_str())
                            }
                            PgTypeId::TIMESTAMPTZ => {
                                format!("timestamp{} with time zone", typemod_str())
                            }
                            PgTypeId::ARRAYTIMESTAMPTZ => {
                                format!("timestamp{} with time zone[]", typemod_str())
                            }
                            PgTypeId::INTERVAL => match typemod {
                                Some(typemod) if typemod >= 0 => "-".to_string(),
                                _ => "interval".to_string(),
                            },
                            PgTypeId::ARRAYINTERVAL => match typemod {
                                Some(typemod) if typemod >= 0 => "-".to_string(),
                                _ => "interval[]".to_string(),
                            },
                            PgTypeId::ARRAYNUMERIC => format!("numeric{}[]", typemod_str()),
                            PgTypeId::TIMETZ => format!("time{} with time zone", typemod_str()),
                            PgTypeId::ARRAYTIMETZ => {
                                format!("time{} with time zone[]", typemod_str())
                            }
                            PgTypeId::NUMERIC => format!("numeric{}", typemod_str()),
                            PgTypeId::RECORD => format!("record{}", typemod_str()),
                            PgTypeId::ANYARRAY => format!("anyarray{}", typemod_str()),
                            PgTypeId::ANYELEMENT => format!("anyelement{}", typemod_str()),
                            PgTypeId::ARRAYRECORD => format!("record{}[]", typemod_str()),
                            PgTypeId::PGLSN => format!("pg_lsn{}", typemod_str()),
                            PgTypeId::ARRAYPGLSN => format!("pg_lsn{}[]", typemod_str()),
                            PgTypeId::ANYENUM => format!("anyenum{}", typemod_str()),
                            PgTypeId::ANYRANGE => format!("anyrange{}", typemod_str()),
                            PgTypeId::INT4RANGE => format!("int4range{}", typemod_str()),
                            PgTypeId::ARRAYINT4RANGE => format!("int4range{}[]", typemod_str()),
                            PgTypeId::NUMRANGE => format!("numrange{}", typemod_str()),
                            PgTypeId::ARRAYNUMRANGE => format!("numrange{}[]", typemod_str()),
                            PgTypeId::TSRANGE => format!("tsrange{}", typemod_str()),
                            PgTypeId::ARRAYTSRANGE => format!("tsrange{}[]", typemod_str()),
                            PgTypeId::TSTZRANGE => format!("tstzrange{}", typemod_str()),
                            PgTypeId::ARRAYTSTZRANGE => format!("tstzrange{}[]", typemod_str()),
                            PgTypeId::DATERANGE => format!("daterange{}", typemod_str()),
                            PgTypeId::ARRAYDATERANGE => format!("daterange{}[]", typemod_str()),
                            PgTypeId::INT8RANGE => format!("int8range{}", typemod_str()),
                            PgTypeId::ARRAYINT8RANGE => format!("int8range{}[]", typemod_str()),
                            PgTypeId::INT4MULTIRANGE => format!("int4multirange{}", typemod_str()),
                            PgTypeId::NUMMULTIRANGE => format!("nummultirange{}", typemod_str()),
                            PgTypeId::TSMULTIRANGE => format!("tsmultirange{}", typemod_str()),
                            PgTypeId::DATEMULTIRANGE => format!("datemultirange{}", typemod_str()),
                            PgTypeId::INT8MULTIRANGE => format!("int8multirange{}", typemod_str()),
                            PgTypeId::ARRAYINT4MULTIRANGE => {
                                format!("int4multirange{}[]", typemod_str())
                            }
                            PgTypeId::ARRAYNUMMULTIRANGE => {
                                format!("nummultirange{}[]", typemod_str())
                            }
                            PgTypeId::ARRAYTSMULTIRANGE => {
                                format!("tsmultirange{}[]", typemod_str())
                            }
                            PgTypeId::ARRAYDATEMULTIRANGE => {
                                format!("datemultirange{}[]", typemod_str())
                            }
                            PgTypeId::ARRAYINT8MULTIRANGE => {
                                format!("int8multirange{}[]", typemod_str())
                            }
                            PgTypeId::ARRAYPGCONSTRAINT => {
                                format!("pg_constraint{}[]", typemod_str())
                            }
                            PgTypeId::PGCONSTRAINT => format!("pg_constraint{}", typemod_str()),
                            PgTypeId::ARRAYPGNAMESPACE => {
                                format!("pg_namespace{}[]", typemod_str())
                            }
                            PgTypeId::PGNAMESPACE => format!("pg_namespace{}", typemod_str()),
                            PgTypeId::CHARACTERDATA => {
                                format!("information_schema.character_data{}", typemod_str())
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
                        PG_NAMESPACE_CATALOG_OID | PG_NAMESPACE_PUBLIC_OID => Some(true),
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
            .map(|oid| oid.map(|_oid| "PRIMARY KEY (oid)".to_string()))
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

pub const MEASURE_UDAF_NAME: &str = "measure";

pub fn create_measure_udaf() -> AggregateUDF {
    let signature = Signature::any(1, Volatility::Immutable);

    // MEASURE(cube.measure) should have same type as just cube.measure
    let return_type: ReturnTypeFunction = Arc::new(move |inputs| {
        if inputs.len() != 1 {
            Err(DataFusionError::Internal(format!(
                "Unexpected argument types for MEASURE: {inputs:?}"
            )))
        } else {
            Ok(Arc::new(inputs[0].clone()))
        }
    });

    let accumulator: AccumulatorFunctionImplementation = Arc::new(|| todo!("Not implemented"));

    let state_type = Arc::new(vec![DataType::Float64]);
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));

    AggregateUDF::new(
        MEASURE_UDAF_NAME,
        &signature,
        &return_type,
        &accumulator,
        &state_type,
    )
}

pub const PATCH_MEASURE_UDAF_NAME: &str = "__patch_measure";

// TODO add sanity check on incoming query to disallow it in input
pub fn create_patch_measure_udaf() -> AggregateUDF {
    // TODO actually signature should look like (any, text, boolean)
    let signature = Signature::any(3, Volatility::Immutable);

    // __PATCH_MEASURE(cube.measure, type, filter) should have same type as just cube.measure
    let return_type: ReturnTypeFunction = Arc::new(move |inputs| {
        if inputs.len() != 3 {
            Err(DataFusionError::Internal(format!(
                "Unexpected argument types for {PATCH_MEASURE_UDAF_NAME}: {inputs:?}"
            )))
        } else {
            Ok(Arc::new(inputs[0].clone()))
        }
    });

    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|| todo!("Internal, should not execute"));

    let state_type = Arc::new(vec![DataType::Float64]);
    let state_type: StateTypeFunction = Arc::new(move |_| Ok(state_type.clone()));

    AggregateUDF::new(
        PATCH_MEASURE_UDAF_NAME,
        &signature,
        &return_type,
        &accumulator,
        &state_type,
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

macro_rules! generate_series_helper_date32 {
    ($CURRENT:ident, $STEP:ident, $PRIMITIVE_TYPE: ident) => {
        let current_dt = ::chrono::DateTime::from_timestamp(($CURRENT as i64) * 86400, 0)
            .map(|dt| dt.naive_utc())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Cannot convert date to NaiveDateTime: {}",
                    $CURRENT
                ))
            })?;
        let res = date_addsub_month_day_nano(current_dt, $STEP, true)?;
        $CURRENT = (res.and_utc().timestamp() / 86400) as $PRIMITIVE_TYPE;
    };
}

macro_rules! generate_series_helper_timestamp {
    ($CURRENT:ident, $STEP:ident, $PRIMITIVE_TYPE: ident) => {
        let current_dt = ::chrono::DateTime::from_timestamp(
            ($CURRENT as i64) / 1_000_000_000,
            ($CURRENT % 1_000_000_000) as u32,
        )
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Cannot convert timestamp to NaiveDateTime: {}",
                $CURRENT
            ))
        })?;
        let res = date_addsub_month_day_nano(current_dt, $STEP, true)?;
        $CURRENT = res.and_utc().timestamp_nanos_opt().unwrap() as $PRIMITIVE_TYPE;
    };
}

macro_rules! generate_series_non_primitive_udtf {
    ($ARGS:expr, $TYPE: ident, $PRIMITIVE_TYPE: ident, $HANDLE_MACRO:ident) => {{
        let mut section_sizes: Vec<usize> = Vec::new();
        let l_arr = &$ARGS[0].as_any().downcast_ref::<PrimitiveArray<$TYPE>>();

        if l_arr.is_some() {
            let l_arr = l_arr.unwrap();
            let r_arr = downcast_primitive_arg!($ARGS[1], "right", $TYPE);
            let step_arr = IntervalMonthDayNanoArray::from_value(
                IntervalMonthDayNanoType::make_value(0, 1, 0), // 1 day as default
                1,
            );
            let step_arr = if $ARGS.len() > 2 {
                downcast_primitive_arg!($ARGS[2], "step", IntervalMonthDayNanoType)
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

                if let (Some(start), Some(end)) = (start, end) {
                    let mut section_size: i64 = 0;
                    if start <= end && step > 0 {
                        let mut current = start;
                        loop {
                            if current > end {
                                break;
                            }
                            builder.append_value(current).unwrap();

                            section_size += 1;
                            $HANDLE_MACRO!(current, step, $PRIMITIVE_TYPE);
                        }
                    }
                    section_sizes.push(section_size as usize);
                }
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
        } else if args[0].as_any().downcast_ref::<Date32Array>().is_some() {
            generate_series_non_primitive_udtf!(
                args,
                Date32Type,
                i32,
                generate_series_helper_date32
            )
        } else if args[0]
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .is_some()
        {
            generate_series_non_primitive_udtf!(
                args,
                TimestampNanosecondType,
                i64,
                generate_series_helper_timestamp
            )
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
                TypeSignature::Exact(vec![DataType::Date32, DataType::Date32]),
                TypeSignature::Exact(vec![
                    DataType::Date32,
                    DataType::Date32,
                    DataType::Interval(IntervalUnit::MonthDayNano),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Date32,
                    DataType::Date32,
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Date32,
                    DataType::Date32,
                    DataType::Interval(IntervalUnit::DayTime),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::MonthDayNano),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::YearMonth),
                ]),
                TypeSignature::Exact(vec![
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    DataType::Interval(IntervalUnit::DayTime),
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
            let values = match values.data_type() {
                DataType::Int64 => values,
                DataType::Int16 => cast(&values, &DataType::Int64)?,
                _ => {
                    return Err(DataFusionError::Internal("information_schema._pg_expandarray only supports inputs of type List<Int16> or List<Int64>".to_string()))
                }
            };
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
        // FIXME: it is likely more correct to analyze List type and match type of `x`.
        // For now, in order to avoid unexpected breakages, this is left as `Int64`.
        Arc::new(move |_| Ok(Arc::new(DataType::Struct(fields()))));

    TableUDF::new(
        "information_schema._pg_expandarray",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Int64,
                    true,
                )))]),
                TypeSignature::Exact(vec![DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Int16,
                    true,
                )))]),
            ],
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

                        let requested = if privilege.contains(",") {
                            privilege
                                .split(",")
                                .map(|v| v.trim().to_lowercase())
                                .collect()
                        } else {
                            vec![privilege.to_lowercase()]
                        };

                        let mut result = true;

                        for request in requested {
                            match request.as_str() {
                                "create" => {
                                    result = false;
                                }
                                "usage" => {}
                                _ => {
                                    return Err(DataFusionError::Execution(format!(
                                        "unrecognized privilege type: \"{}\"",
                                        privilege
                                    )))
                                }
                            }
                        }

                        Some(result)
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

pub fn create_has_table_privilege_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let (users, tables, privileges) = if args.len() == 3 {
            (
                Some(downcast_string_arg!(args[0], "user", i32)),
                downcast_string_arg!(args[1], "table", i32),
                downcast_string_arg!(args[2], "privilege", i32),
            )
        } else {
            (
                None,
                downcast_string_arg!(args[0], "table", i32),
                downcast_string_arg!(args[1], "privilege", i32),
            )
        };

        let result = izip!(tables, privileges)
            .enumerate()
            .map(|(i, args)| {
                Ok(match args {
                    (Some(_table), Some(privilege)) => {
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

                        // TODO: check if table exists

                        match privilege.to_lowercase().as_str() {
                            "select" => Some(true),
                            "update" | "insert" | "delete" => Some(false),
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
        "has_table_privilege",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
            ],
            Volatility::Stable,
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
                                    // If the type name contains a dot, it's a schema-qualified name
                                    // and we should return the approprate RegClass to be converted to OID
                                    // For now, we'll return 0 so metabase can sync without failing
                                    // TODO actually read `pg_type`
                                    if as_str.contains('.') {
                                        builder.append_value(0)?;
                                    } else {
                                        return Err(DataFusionError::Execution(format!(
                                            "Unable to cast expression to Regclass: Unknown type: {}",
                                            as_str
                                        )));
                                    }
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

// Return a NOOP for this so metabase can sync without failing
// See https://www.postgresql.org/docs/17/functions-info.html#FUNCTIONS-INFO-COMMENT here
// TODO: Implement this
pub fn create_col_description_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        // Ensure the output array has the same length as the input
        let input_length = args[0].len();
        let mut builder = StringBuilder::new(input_length);

        for _ in 0..input_length {
            builder.append_null()?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "col_description",
        // Correct signature for col_description should be `(oid, integer) → text`
        // We model oid as UInt32, so [DataType::UInt32, DataType::Int32] is a proper arguments
        // However, it seems that coercion rules in DF differs from PostgreSQL at the moment
        // And metabase uses col_description(CAST(CAST(... AS regclass) AS oid), cardinal_number)
        // And we model regclass as Int64, and cardinal_number as UInt32
        // Which is why second signature is necessary
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Int32]),
                // TODO remove this signature in favor of proper model/coercion
                TypeSignature::Exact(vec![DataType::Int64, DataType::UInt32]),
            ],
            Volatility::Stable,
        ),
        &return_type,
        &fun,
    )
}

// See https://www.postgresql.org/docs/17/functions-string.html#FUNCTIONS-STRING-FORMAT
pub fn create_format_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        // Ensure at least one argument is provided
        if args.is_empty() {
            return Err(DataFusionError::Execution(
                "format() requires at least one argument".to_string(),
            ));
        }

        // Ensure the first argument is a Utf8 (string)
        if args[0].data_type() != &DataType::Utf8 {
            return Err(DataFusionError::Execution(
                "format() first argument must be a string".to_string(),
            ));
        }

        let format_strings = downcast_string_arg!(&args[0], "format_str", i32);
        let mut builder = StringBuilder::new(format_strings.len());

        for i in 0..format_strings.len() {
            if format_strings.is_null(i) {
                builder.append_null()?;
                continue;
            }

            let format_str = format_strings.value(i);
            let mut result = String::new();
            let mut format_chars = format_str.chars().peekable();
            let mut arg_index = 1; // Start from first argument after format string

            while let Some(c) = format_chars.next() {
                if c != '%' {
                    result.push(c);
                    continue;
                }

                match format_chars.next() {
                    Some('I') => {
                        // Handle %I - SQL identifier
                        if arg_index >= args.len() {
                            return Err(DataFusionError::Execution(
                                "Not enough arguments for format string".to_string(),
                            ));
                        }

                        let arg = &args[arg_index];
                        let value = match arg.data_type() {
                            DataType::Utf8 => {
                                let str_arr = downcast_string_arg!(arg, "arg", i32);
                                if str_arr.is_null(i) {
                                    return Err(DataFusionError::Execution(
                                        "NULL values cannot be formatted as identifiers"
                                            .to_string(),
                                    ));
                                }
                                str_arr.value(i).to_string()
                            }
                            _ => {
                                // For other types, try to convert to string
                                let str_arr = cast(&arg, &DataType::Utf8)?;
                                let str_arr =
                                    str_arr.as_any().downcast_ref::<StringArray>().unwrap();
                                if str_arr.is_null(i) {
                                    return Err(DataFusionError::Execution(
                                        "NULL values cannot be formatted as identifiers"
                                            .to_string(),
                                    ));
                                }
                                str_arr.value(i).to_string()
                            }
                        };

                        // Quote any identifier for now
                        // That's a safety-first approach: it would quote too much, but every edge-case would be covered
                        // Like `1` or `1a` or `select`
                        // TODO Quote identifier only if necessary
                        let needs_quoting = true;

                        if needs_quoting {
                            result.push('"');
                            result.push_str(&value.replace('"', "\"\""));
                            result.push('"');
                        } else {
                            result.push_str(&value);
                        }
                        arg_index += 1;
                    }
                    Some('%') => {
                        // %% is escaped to single %
                        result.push('%');
                    }
                    Some('s') => {
                        // Handle %s - regular string
                        if arg_index >= args.len() {
                            return Err(DataFusionError::Execution(
                                "Not enough arguments for format string".to_string(),
                            ));
                        }

                        let arg = &args[arg_index];
                        let value = match arg.data_type() {
                            DataType::Utf8 => {
                                let str_arr = downcast_string_arg!(arg, "arg", i32);
                                if str_arr.is_null(i) {
                                    // A null value is treated as an empty string
                                    String::new()
                                } else {
                                    str_arr.value(i).to_string()
                                }
                            }
                            _ => {
                                // For other types, try to convert to string
                                let str_arr = cast(&arg, &DataType::Utf8)?;
                                let str_arr =
                                    str_arr.as_any().downcast_ref::<StringArray>().unwrap();
                                if str_arr.is_null(i) {
                                    // A null value is treated as an empty string
                                    String::new()
                                } else {
                                    str_arr.value(i).to_string()
                                }
                            }
                        };

                        result.push_str(&value);
                        arg_index += 1;
                    }
                    Some(c) => {
                        return Err(DataFusionError::Execution(format!(
                            "Unsupported format specifier %{}",
                            c
                        )));
                    }
                    None => {
                        return Err(DataFusionError::Execution(
                            "Invalid format string - ends with %".to_string(),
                        ));
                    }
                }
            }

            builder.append_value(result)?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "format",
        // Actually, format should be variadic with types (Utf8, any*)
        // But ATM DataFusion does not support those signatures
        // And this would work through implicit casting to Utf8
        // TODO migrate to proper custom signature once it's supported by DF
        &Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
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

pub fn create_position_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        return Err(DataFusionError::NotImplemented(format!(
            "POSITION is not implemented, it's a stub"
        )));
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "position",
        &Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_date_to_timestamp_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        match args[0].data_type() {
            DataType::Date32 => {
                let date_arr = downcast_primitive_arg!(args[0], "date", Date32Type);

                let result = date_arr
                    .iter()
                    .map(|date| {
                        date.map(|date| {
                            let nanoseconds_in_day = 86_400_000_000_000_i64;
                            let timestamp = date as i64 * nanoseconds_in_day;
                            timestamp
                        })
                    })
                    .collect::<PrimitiveArray<TimestampNanosecondType>>();

                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::Utf8 => {
                let date_arr = downcast_string_arg!(args[0], "date", i32);

                let result = date_arr
                    .iter()
                    .map(|date| match date {
                        Some(date) => {
                            let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
                                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                            let time = NaiveTime::from_hms_opt(0, 0, 0).ok_or(
                                DataFusionError::Execution(
                                    "Cannot initalize default zero NaiveTime".to_string(),
                                ),
                            )?;
                            Ok(Some(
                                NaiveDateTime::new(date, time)
                                    .and_utc()
                                    .timestamp_nanos_opt()
                                    .unwrap(),
                            ))
                        }
                        None => Ok(None),
                    })
                    .collect::<Result<PrimitiveArray<TimestampNanosecondType>>>()?;

                Ok(Arc::new(result) as ArrayRef)
            }
            _ => Err(DataFusionError::Execution(format!(
                "DATE_TO_TIMESTAMP doesn't support this type: {}",
                args[0].data_type(),
            ))),
        }
    });

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));

    ScalarUDF::new(
        "date_to_timestamp",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Date32]),
                TypeSignature::Exact(vec![DataType::Utf8]),
            ],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_sha1_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let strings = downcast_string_arg!(args[0], "str", i32);

        let result = strings
            .iter()
            .map(|string| {
                string.map(|string| {
                    let mut hasher = Sha1::new();
                    hasher.update(string.as_bytes());
                    hasher.digest().to_string()
                })
            })
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "sha1",
        &Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_current_setting_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let setting_names = downcast_string_arg!(args[0], "str", i32);

        let result = setting_names
            .iter()
            .map(|setting_name| {
                if let Some(setting_name) = setting_name {
                    Ok(Some(match setting_name.to_ascii_lowercase().as_str() {
                        "max_index_keys" => "32".to_string(), // Taken from PostgreSQL
                        "search_path" => "\"$user\", public".to_string(), // Taken from PostgreSQL
                        "server_version_num" => "140002".to_string(), // Matches 14.2
                        setting_name => Err(DataFusionError::Execution(format!(
                            "unrecognized configuration parameter \"{}\"",
                            setting_name
                        )))?,
                    }))
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<StringArray>>()?;

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "current_setting",
        &Signature::exact(vec![DataType::Utf8], Volatility::Stable),
        &return_type,
        &fun,
    )
}

pub fn create_quote_ident_udf() -> ScalarUDF {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-z_][a-z0-9_]*$").unwrap());

    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let idents = downcast_string_arg!(args[0], "str", i32);

        let result = idents
            .iter()
            .map(|ident| {
                ident.map(|ident| {
                    if RE.is_match(ident) {
                        return ident.to_string();
                    }
                    format!("\"{}\"", ident.replace("\"", "\"\""))
                })
            })
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "quote_ident",
        &Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_pg_encoding_to_char_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let encoding_ids = downcast_primitive_arg!(args[0], "encoding_id", Int32Type);

        let result = encoding_ids
            .iter()
            .map(|oid| match oid {
                Some(0) => Some("SQL_ASCII".to_string()),
                Some(6) => Some("UTF8".to_string()),
                Some(_) => Some("".to_string()),
                _ => None,
            })
            .collect::<StringArray>();

        Ok(Arc::new(result))
    });

    create_udf(
        "pg_encoding_to_char",
        vec![DataType::Int32],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_array_to_string_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 2);

        let input_arr = downcast_list_arg!(args[0], "strs");
        let join_strs = downcast_string_arg!(args[1], "join_str", i32);

        let mut builder = StringBuilder::new(input_arr.len());

        for i in 0..input_arr.len() {
            if input_arr.is_null(i) || join_strs.is_null(i) {
                builder.append_null()?;
                continue;
            }

            let array = input_arr.value(i);
            let join_str = join_strs.value(i);
            let strings = downcast_string_arg!(array, "str", i32);
            let joined_string =
                itertools::Itertools::intersperse(strings.iter().flatten(), join_str)
                    .collect::<String>();
            builder.append_value(joined_string)?;
        }

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "array_to_string",
        vec![
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            DataType::Utf8,
        ],
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        fun,
    )
}

pub fn create_to_regtype_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 1);

        let regtype_arr = downcast_string_arg!(args[0], "regtype", i32);

        let pg_types = PgType::get_all();

        let result = regtype_arr
            .iter()
            .map(|regtype| match regtype {
                Some(regtype) => pg_types
                    .iter()
                    .find(|typ| typ.typname == regtype || typ.regtype == regtype)
                    .map(|typ| typ.oid as i32),
                None => None,
            })
            .collect::<PrimitiveArray<Int32Type>>();

        Ok(Arc::new(result) as ArrayRef)
    });

    // TODO: `to_regtype` should return regtype but we use `oid` since it's used for comparison with oids
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int32)));

    ScalarUDF::new(
        "to_regtype",
        &Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn create_pg_get_indexdef_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        assert!(args.len() == 3);

        let index_oids = downcast_primitive_arg!(args[0], "index_oid", UInt32Type);
        let column_nos = downcast_primitive_arg!(args[1], "column_no", Int64Type);
        let prettys = downcast_boolean_arr!(&args[2], "pretty");

        let result = izip!(index_oids, column_nos, prettys)
            .map(|_| None as Option<&str>)
            .collect::<StringArray>();

        Ok(Arc::new(result) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "pg_get_indexdef",
        &Signature::exact(
            vec![DataType::UInt32, DataType::Int64, DataType::Boolean],
            Volatility::Immutable,
        ),
        &return_type,
        &fun,
    )
}

pub fn create_udf_stub(
    name: &'static str,
    type_signature: TypeSignature,
    return_type: Option<DataType>,
    volatility: Option<Volatility>,
) -> ScalarUDF {
    let fun = make_scalar_function(move |_| {
        Err(DataFusionError::NotImplemented(format!(
            "{} is not implemented, it's a stub",
            name
        )))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |args| {
        if let Some(return_type) = &return_type {
            return Ok(Arc::new(return_type.clone()));
        }

        if args.len() > 0 {
            return Ok(Arc::new(args[0].clone()));
        }

        Ok(Arc::new(DataType::Null))
    });

    ScalarUDF::new(
        name,
        &Signature::new(type_signature, volatility.unwrap_or(Volatility::Immutable)),
        &return_type,
        &fun,
    )
}

pub fn create_udaf_stub(
    name: &'static str,
    type_signature: TypeSignature,
    return_type: Option<DataType>,
    volatility: Option<Volatility>,
) -> AggregateUDF {
    let fun: AccumulatorFunctionImplementation = Arc::new(move || {
        Err(DataFusionError::NotImplemented(format!(
            "{} is not implemented, it's a stub",
            name
        )))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |args| {
        if let Some(return_type) = &return_type {
            return Ok(Arc::new(return_type.clone()));
        }

        if args.len() > 0 {
            return Ok(Arc::new(args[0].clone()));
        }

        Ok(Arc::new(DataType::Null))
    });

    let state_type: StateTypeFunction = Arc::new(move |dt| Ok(Arc::new(vec![dt.clone()])));

    AggregateUDF::new(
        name,
        &Signature::new(type_signature, volatility.unwrap_or(Volatility::Immutable)),
        &return_type,
        &fun,
        &state_type,
    )
}

pub fn create_udtf_stub(
    name: &'static str,
    type_signature: TypeSignature,
    return_type: Option<DataType>,
    volatility: Option<Volatility>,
) -> TableUDF {
    let fun = make_table_function(move |_| {
        Err(DataFusionError::NotImplemented(format!(
            "{} is not implemented, it's a stub",
            name
        )))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |args| {
        if let Some(return_type) = &return_type {
            return Ok(Arc::new(return_type.clone()));
        }

        if args.len() > 0 {
            return Ok(Arc::new(args[0].clone()));
        }

        Ok(Arc::new(DataType::Null))
    });

    TableUDF::new(
        name,
        &Signature::new(type_signature, volatility.unwrap_or(Volatility::Immutable)),
        &return_type,
        &fun,
    )
}

pub fn create_inet_server_addr_udf() -> ScalarUDF {
    let fun = make_scalar_function(move |_: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        builder.append_value("127.0.0.1/32").unwrap();

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new(
        "inet_server_addr",
        &Signature::exact(vec![], Volatility::Immutable),
        &return_type,
        &fun,
    )
}

pub fn register_fun_stubs(mut ctx: SessionContext) -> SessionContext {
    macro_rules! register_fun_stub {
        ($FTYP:ident, $NAME:expr, argc=$ARGC:expr $(, rettyp=$RETTYP:ident)? $(, vol=$VOL:ident)?) => {
            register_fun_stub!(
                __internal,
                $FTYP,
                $NAME,
                tsig=TypeSignature::Any($ARGC)
                $(, rettyp=$RETTYP)?
                $(, vol=$VOL)?
            );
        };

        ($FTYP:ident, $NAME:expr, tsig=[$($DT:ident),+] $(, rettyp=$RETTYP:ident)? $(, vol=$VOL:ident)?) => {
            register_fun_stub!(
                __internal,
                $FTYP,
                $NAME,
                tsig=TypeSignature::Exact(vec![$(__typ!($DT)),+])
                $(, rettyp=$RETTYP)?
                $(, vol=$VOL)?
            );
        };

        ($FTYP:ident, $NAME:expr, tsigs=[$([$($DT:ident),*],)+] $(, rettyp=$RETTYP:ident)? $(, vol=$VOL:ident)?) => {
            register_fun_stub!(
                __internal,
                $FTYP,
                $NAME,
                tsig=TypeSignature::OneOf(vec![$(TypeSignature::Exact(vec![$(__typ!($DT)),*]),)+])
                $(, rettyp=$RETTYP)?
                $(, vol=$VOL)?
            );
        };

        (__internal, udf, $NAME:expr, tsig=$TSIG:expr $(, rettyp=$RETTYP:ident)? $(, vol=$VOL:ident)?) => {{
            ctx.register_udf(create_udf_stub($NAME, $TSIG, __rettyp!($($RETTYP)?), __vol!($($VOL)?)));
        }};

        (__internal, udaf, $NAME:expr, tsig=$TSIG:expr $(, rettyp=$RETTYP:ident)? $(, vol=$VOL:ident)?) => {{
            ctx.register_udaf(create_udaf_stub($NAME, $TSIG, __rettyp!($($RETTYP)?), __vol!($($VOL)?)));
        }};

        (__internal, udtf, $NAME:expr, tsig=$TSIG:expr $(, rettyp=$RETTYP:ident)? $(, vol=$VOL:ident)?) => {{
            ctx.register_udtf(create_udtf_stub($NAME, $TSIG, __rettyp!($($RETTYP)?), __vol!($($VOL)?)));
        }};
    }

    macro_rules! __rettyp {
        ($RETTYP:ident) => {
            Some(__typ!($RETTYP))
        };
        () => {
            None
        };
    }

    macro_rules! __typ {
        (List<$LTYP:ident>) => {
            DataType::List(Box::new(Field::new("item", DataType::$LTYP, true)))
        };
        (ListUtf8) => {
            __typ!(List<Utf8>)
        };
        (ListInt32) => {
            __typ!(List<Int32>)
        };
        (Interval) => {
            DataType::Interval(IntervalUnit::MonthDayNano)
        };
        (Time) => {
            DataType::Time64(TimeUnit::Nanosecond)
        };
        (Timestamp) => {
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        };
        (TimestampTz) => {
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_string()))
        };
        (Oid) => {
            __typ!(UInt32)
        };
        (Regclass) => {
            __typ!(Utf8)
        };
        (Regnamespace) => {
            __typ!(Utf8)
        };
        (Regtype) => {
            __typ!(Utf8)
        };
        (Xid) => {
            __typ!(UInt32)
        };
        (Xid8) => {
            __typ!(UInt64)
        };
        ($TYP:ident) => {
            DataType::$TYP
        };
    }

    macro_rules! __vol {
        ($VOL:ident) => {
            Some(Volatility::$VOL)
        };
        () => {
            None
        };
    }

    // NOTE: types accepted as "numeric" in Postgres are implemented as "Float64" here
    // NOTE: lack of "rettyp" implies "type of first arg"
    register_fun_stub!(udf, "acosd", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "acosh", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(
        udf,
        "age",
        tsigs = [[Timestamp], [Timestamp, Timestamp],],
        rettyp = Interval,
        vol = Stable
    );
    register_fun_stub!(udf, "asind", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "asinh", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "atan2", tsig = [Float64, Float64], rettyp = Float64);
    register_fun_stub!(udf, "atan2d", tsig = [Float64, Float64], rettyp = Float64);
    register_fun_stub!(udf, "atand", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "atanh", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "bit_count", tsig = [Binary], rettyp = Int64);
    register_fun_stub!(
        udf,
        "brin_desummarize_range",
        tsig = [Regclass, Int64],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "brin_summarize_new_values",
        tsig = [Regclass],
        rettyp = Int32,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "brin_summarize_range",
        tsig = [Regclass, Int64],
        rettyp = Int32,
        vol = Volatile
    );
    register_fun_stub!(udf, "cbrt", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "ceiling", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(
        udf,
        "clock_timestamp",
        argc = 0,
        rettyp = TimestampTz,
        vol = Volatile
    );
    register_fun_stub!(udf, "convert", tsig = [Binary, Utf8, Utf8], rettyp = Binary);
    register_fun_stub!(udf, "convert_from", tsig = [Binary, Utf8], rettyp = Utf8);
    register_fun_stub!(udf, "convert_to", tsig = [Utf8, Utf8], rettyp = Binary);
    register_fun_stub!(udf, "cosd", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "cosh", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "cot", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "cotd", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(
        udf,
        "current_catalog",
        argc = 0,
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(udf, "current_query", argc = 0, rettyp = Utf8, vol = Stable);
    register_fun_stub!(udf, "current_role", argc = 0, rettyp = Utf8, vol = Stable);
    register_fun_stub!(
        udf,
        "current_time",
        tsigs = [[], [Int32],],
        rettyp = Time,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "date_bin",
        tsig = [Interval, Timestamp, Timestamp],
        rettyp = Timestamp
    );
    register_fun_stub!(udf, "decode", tsig = [Utf8, Utf8], rettyp = Binary);
    register_fun_stub!(udf, "degrees", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "dexp", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "div", tsig = [Float64, Float64], rettyp = Float64);
    register_fun_stub!(
        udf,
        "dlog1",
        tsigs = [[Int16], [Int32], [Int64], [Float64],]
    );
    register_fun_stub!(udf, "dlog10", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "encode", tsig = [Binary, Utf8], rettyp = Utf8);
    register_fun_stub!(udf, "factorial", tsig = [Int64], rettyp = Float64);
    register_fun_stub!(udf, "gcd", argc = 2);
    register_fun_stub!(
        udf,
        "gen_random_uuid",
        argc = 0,
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(udf, "get_bit", tsig = [Binary, Int64], rettyp = Int32);
    register_fun_stub!(udf, "get_byte", tsig = [Binary, Int32], rettyp = Int32);
    register_fun_stub!(
        udf,
        "gin_clean_pending_list",
        tsig = [Regclass],
        rettyp = Int64,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "has_column_privilege",
        tsigs = [
            [Utf8, Utf8, Utf8],
            [Oid, Utf8, Utf8],
            [Utf8, Int16, Utf8],
            [Oid, Int16, Utf8],
            [Utf8, Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8, Utf8],
            [Utf8, Utf8, Int16, Utf8],
            [Utf8, Oid, Int16, Utf8],
            [Oid, Utf8, Utf8, Utf8],
            [Oid, Oid, Utf8, Utf8],
            [Oid, Utf8, Int16, Utf8],
            [Oid, Oid, Int16, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_database_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_foreign_data_wrapper_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_function_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_language_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_parameter_privilege",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8], [Oid, Utf8, Utf8],],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_sequence_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_server_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_tablespace_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "has_type_privilege",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "inet_client_addr",
        argc = 0,
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "inet_client_port",
        argc = 0,
        rettyp = Int32,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "inet_server_port",
        argc = 0,
        rettyp = Int32,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "isfinite",
        tsigs = [[Date32], [Timestamp], [Interval],],
        rettyp = Boolean
    );
    register_fun_stub!(udf, "justify_days", tsig = [Interval], rettyp = Interval);
    register_fun_stub!(udf, "justify_hours", tsig = [Interval], rettyp = Interval);
    register_fun_stub!(
        udf,
        "justify_interval",
        tsig = [Interval],
        rettyp = Interval
    );
    register_fun_stub!(udf, "lcm", argc = 2);
    register_fun_stub!(
        udf,
        "localtime",
        tsigs = [[], [Int32],],
        rettyp = Time,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "make_date",
        tsig = [Int32, Int32, Int32],
        rettyp = Date32
    );
    register_fun_stub!(
        udf,
        "make_interval",
        tsigs = [
            [],
            [Int32],
            [Int32, Int32],
            [Int32, Int32, Int32],
            [Int32, Int32, Int32, Int32],
            [Int32, Int32, Int32, Int32, Int32],
            [Int32, Int32, Int32, Int32, Int32, Int32],
            [Int32, Int32, Int32, Int32, Int32, Int32, Float64],
        ],
        rettyp = Interval
    );
    register_fun_stub!(
        udf,
        "make_time",
        tsig = [Int32, Int32, Float64],
        rettyp = Time
    );
    register_fun_stub!(
        udf,
        "make_timestamp",
        tsig = [Int32, Int32, Int32, Int32, Int32, Float64],
        rettyp = Timestamp
    );
    register_fun_stub!(
        udf,
        "make_timestamptz",
        tsigs = [
            [Int32, Int32, Int32, Int32, Int32, Float64],
            [Int32, Int32, Int32, Int32, Int32, Float64, Utf8],
        ],
        rettyp = TimestampTz
    );
    register_fun_stub!(udf, "min_scale", tsig = [Float64], rettyp = Int32);
    register_fun_stub!(udf, "mod", argc = 2);
    register_fun_stub!(
        udf,
        "obj_description",
        tsigs = [[Oid], [Oid, Utf8],],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "parse_ident",
        tsigs = [[Utf8], [Utf8, Boolean],],
        rettyp = ListUtf8
    );
    register_fun_stub!(
        udf,
        "pg_advisory_lock",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_advisory_lock_shared",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_advisory_unlock",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(udf, "pg_advisory_unlock_all", argc = 0, rettyp = Null);
    register_fun_stub!(
        udf,
        "pg_advisory_unlock_shared",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_advisory_xact_lock",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_advisory_xact_lock_shared",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_blocking_pids",
        tsig = [Int32],
        rettyp = ListInt32,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_cancel_backend",
        tsig = [Int32],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_char_to_encoding",
        tsig = [Utf8],
        rettyp = Int32,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_client_encoding",
        argc = 0,
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_collation_actual_version",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_collation_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_column_compression",
        argc = 1,
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_column_size",
        argc = 1,
        rettyp = Int32,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_conf_load_time",
        argc = 0,
        rettyp = TimestampTz,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_conversion_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_current_logfile",
        tsigs = [[], [Utf8],],
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_current_xact_id",
        argc = 0,
        rettyp = Xid8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_current_xact_id_if_assigned",
        argc = 0,
        rettyp = Xid8,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_database_collation_actual_version",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_database_size",
        tsigs = [[Utf8], [Oid],],
        rettyp = Int64,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_describe_object",
        tsig = [Oid, Oid, Int32],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_drop_replication_slot",
        tsig = [Utf8],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_event_trigger_table_rewrite_oid",
        argc = 0,
        rettyp = Oid,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_event_trigger_table_rewrite_reason",
        argc = 0,
        rettyp = Int32,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_export_snapshot",
        argc = 0,
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_filenode_relation",
        tsig = [Oid, Oid],
        rettyp = Regclass,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_function_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_functiondef",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_function_arguments",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_function_identity_arguments",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_function_result",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_ruledef",
        tsigs = [[Oid], [Oid, Boolean],],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_statisticsobjdef",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_triggerdef",
        tsigs = [[Oid], [Oid, Boolean],],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_viewdef",
        tsigs = [[Oid], [Oid, Boolean], [Oid, Int32], [Utf8], [Utf8, Boolean],],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_get_wal_replay_pause_state",
        argc = 0,
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_has_role",
        tsigs = [
            [Utf8, Utf8],
            [Oid, Utf8],
            [Utf8, Utf8, Utf8],
            [Utf8, Oid, Utf8],
            [Oid, Utf8, Utf8],
            [Oid, Oid, Utf8],
        ],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_import_system_collations",
        tsig = [Regnamespace],
        rettyp = Int32,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_index_column_has_property",
        tsig = [Regclass, Int32, Utf8],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_index_has_property",
        tsig = [Regclass, Utf8],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_indexam_has_property",
        tsig = [Oid, Utf8],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_indexes_size",
        tsig = [Regclass],
        rettyp = Int64,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_is_in_recovery",
        argc = 0,
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_is_wal_replay_paused",
        argc = 0,
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_jit_available",
        argc = 0,
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_last_xact_replay_timestamp",
        argc = 0,
        rettyp = TimestampTz,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_log_backend_memory_contexts",
        tsig = [Int32],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_notification_queue_usage",
        argc = 0,
        rettyp = Float64,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_opclass_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_operator_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_opfamily_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_partition_root",
        tsig = [Regclass],
        rettyp = Regclass,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_postmaster_start_time",
        argc = 0,
        rettyp = TimestampTz,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_promote",
        tsigs = [[], [Boolean], [Boolean, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_read_binary_file",
        tsigs = [[Utf8], [Utf8, Int64, Int64], [Utf8, Int64, Int64, Boolean],],
        rettyp = Binary,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_read_file",
        tsigs = [[Utf8], [Utf8, Int64, Int64], [Utf8, Int64, Int64, Boolean],],
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_relation_filenode",
        tsig = [Regclass],
        rettyp = Oid,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_relation_filepath",
        tsig = [Regclass],
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_relation_size",
        tsigs = [[Regclass], [Regclass, Utf8],],
        rettyp = Int64,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_reload_conf",
        argc = 0,
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_create",
        tsig = [Utf8],
        rettyp = Oid,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_drop",
        tsig = [Utf8],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_oid",
        tsig = [Utf8],
        rettyp = Oid,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_session_is_setup",
        argc = 0,
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_session_reset",
        argc = 0,
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_session_setup",
        tsig = [Utf8],
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_replication_origin_xact_reset",
        argc = 0,
        rettyp = Null
    );
    register_fun_stub!(
        udf,
        "pg_rotate_logfile",
        argc = 0,
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_safe_snapshot_blocking_pids",
        tsig = [Int32],
        rettyp = ListInt32,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_settings_get_flags",
        tsig = [Utf8],
        rettyp = ListUtf8,
        vol = Stable
    );
    register_fun_stub!(udf, "pg_size_bytes", tsig = [Utf8], rettyp = Int64);
    register_fun_stub!(
        udf,
        "pg_size_pretty",
        tsigs = [[Int64], [Float64],],
        rettyp = Utf8
    );
    register_fun_stub!(
        udf,
        "pg_statistics_obj_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_table_size",
        tsig = [Regclass],
        rettyp = Int64,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_tablespace_location",
        tsig = [Oid],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_tablespace_size",
        tsigs = [[Utf8], [Oid],],
        rettyp = Int64,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_terminate_backend",
        tsigs = [[Int32], [Int32, Int64],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_trigger_depth",
        argc = 0,
        rettyp = Int32,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_try_advisory_lock",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_try_advisory_lock_shared",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_try_advisory_xact_lock",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_try_advisory_xact_lock_shared",
        tsigs = [[Int64], [Int32, Int32],],
        rettyp = Boolean,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_ts_config_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_ts_dict_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_ts_parser_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "pg_ts_template_is_visible",
        tsig = [Oid],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(udf, "pg_typeof", argc = 1, rettyp = Regtype, vol = Stable);
    register_fun_stub!(udf, "pg_wal_replay_pause", argc = 0, rettyp = Null);
    register_fun_stub!(udf, "pg_wal_replay_resume", argc = 0, rettyp = Null);
    register_fun_stub!(
        udf,
        "pg_xact_commit_timestamp",
        tsig = [Xid],
        rettyp = TimestampTz,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "pg_xact_status",
        tsig = [Xid8],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(udf, "pi", argc = 0, rettyp = Float64);
    register_fun_stub!(udf, "power", tsig = [Float64, Float64], rettyp = Float64);
    register_fun_stub!(udf, "quote_literal", argc = 1, rettyp = Utf8);
    register_fun_stub!(udf, "quote_nullable", argc = 1, rettyp = Utf8);
    register_fun_stub!(udf, "radians", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(
        udf,
        "regexp_count",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Int32], [Utf8, Utf8, Int32, Utf8],],
        rettyp = Int32
    );
    register_fun_stub!(
        udf,
        "regexp_like",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8],],
        rettyp = Boolean
    );
    register_fun_stub!(
        udf,
        "regexp_split_to_array",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8],],
        rettyp = ListUtf8
    );
    register_fun_stub!(
        udf,
        "row_security_active",
        tsigs = [[Utf8], [Oid],],
        rettyp = Boolean,
        vol = Stable
    );
    register_fun_stub!(udf, "scale", tsig = [Float64], rettyp = Int32);
    register_fun_stub!(
        udf,
        "set_bit",
        tsig = [Binary, Int64, Int32],
        rettyp = Binary
    );
    register_fun_stub!(
        udf,
        "set_byte",
        tsig = [Binary, Int32, Int32],
        rettyp = Binary
    );
    register_fun_stub!(
        udf,
        "set_config",
        tsig = [Utf8, Utf8, Boolean],
        rettyp = Utf8
    );
    register_fun_stub!(
        udf,
        "shobj_description",
        tsig = [Oid, Utf8],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(udf, "sign", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "sind", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "sinh", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(
        udf,
        "statement_timestamp",
        argc = 0,
        rettyp = TimestampTz,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "string_to_array",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8],],
        rettyp = ListUtf8
    );
    register_fun_stub!(udf, "tand", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "tanh", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "timeofday", argc = 0, rettyp = Utf8, vol = Volatile);
    register_fun_stub!(
        udf,
        "to_ascii",
        tsigs = [[Utf8], [Utf8, Utf8], [Utf8, Int32],],
        rettyp = Utf8
    );
    register_fun_stub!(udf, "to_hex", tsigs = [[Int32], [Int64],], rettyp = Utf8);
    register_fun_stub!(udf, "to_number", tsig = [Utf8, Utf8], rettyp = Float64);
    register_fun_stub!(
        udf,
        "transaction_timestamp",
        argc = 0,
        rettyp = TimestampTz,
        vol = Stable
    );
    register_fun_stub!(udf, "trim_scale", tsig = [Float64], rettyp = Float64);
    register_fun_stub!(udf, "txid_current", argc = 0, rettyp = Int64, vol = Stable);
    register_fun_stub!(
        udf,
        "txid_current_if_assigned",
        argc = 0,
        rettyp = Int64,
        vol = Volatile
    );
    register_fun_stub!(
        udf,
        "txid_status",
        tsig = [Int64],
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(udf, "unistr", tsig = [Utf8], rettyp = Utf8);
    register_fun_stub!(
        udf,
        "width_bucket",
        tsig = [Float64, Float64, Float64, Int32],
        rettyp = Int32
    );
    // TODO: "width_bucket" also has a two-arg variant with anyarray args

    register_fun_stub!(udaf, "any_value", argc = 1);
    register_fun_stub!(udaf, "bit_and", tsigs = [[Int16], [Int32], [Int64],]);
    register_fun_stub!(udaf, "bit_or", tsigs = [[Int16], [Int32], [Int64],]);
    register_fun_stub!(udaf, "bit_xor", tsigs = [[Int16], [Int32], [Int64],]);
    register_fun_stub!(udaf, "every", tsig = [Boolean], rettyp = Boolean);
    register_fun_stub!(udaf, "median", argc = 1);
    register_fun_stub!(
        udaf,
        "string_agg",
        tsigs = [[Utf8, Utf8], [Binary, Binary],]
    );
    register_fun_stub!(
        udaf,
        "regr_avgx",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(
        udaf,
        "regr_avgy",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(
        udaf,
        "regr_count",
        tsig = [Float64, Float64],
        rettyp = Int64
    );
    register_fun_stub!(
        udaf,
        "regr_intercept",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(udaf, "regr_r2", tsig = [Float64, Float64], rettyp = Float64);
    register_fun_stub!(
        udaf,
        "regr_slope",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(
        udaf,
        "regr_sxx",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(
        udaf,
        "regr_sxy",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(
        udaf,
        "regr_syy",
        tsig = [Float64, Float64],
        rettyp = Float64
    );
    register_fun_stub!(
        udaf,
        "variance",
        tsigs = [[Int16], [Int32], [Int64], [Float64],],
        rettyp = Float64
    );

    register_fun_stub!(
        udtf,
        "pg_listening_channels",
        argc = 0,
        rettyp = Utf8,
        vol = Stable
    );
    register_fun_stub!(
        udtf,
        "pg_ls_dir",
        tsigs = [[Utf8], [Utf8, Boolean, Boolean],],
        rettyp = Utf8,
        vol = Volatile
    );
    register_fun_stub!(
        udtf,
        "pg_partition_ancestors",
        tsig = [Regclass],
        rettyp = Regclass,
        vol = Stable
    );
    register_fun_stub!(
        udtf,
        "pg_tablespace_databases",
        tsig = [Oid],
        rettyp = Oid,
        vol = Stable
    );
    register_fun_stub!(
        udtf,
        "regexp_matches",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8],],
        rettyp = ListUtf8
    );
    register_fun_stub!(
        udtf,
        "regexp_split_to_table",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8],],
        rettyp = Utf8
    );
    register_fun_stub!(
        udtf,
        "string_to_table",
        tsigs = [[Utf8, Utf8], [Utf8, Utf8, Utf8],],
        rettyp = Utf8
    );

    register_fun_stub!(
        udf,
        "eval_current_date",
        argc = 0,
        rettyp = Date32,
        vol = Stable
    );

    register_fun_stub!(
        udf,
        "eval_now",
        argc = 0,
        rettyp = TimestampTz,
        vol = Stable
    );
    register_fun_stub!(
        udf,
        "eval_utc_timestamp",
        argc = 0,
        rettyp = Timestamp,
        vol = Stable
    );

    ctx
}
