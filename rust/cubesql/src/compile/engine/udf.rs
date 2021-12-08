use std::any::type_name;
use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanArray, BooleanBuilder, GenericStringArray, Int32Builder,
            PrimitiveArray, StringBuilder, UInt32Builder,
        },
        datatypes::{DataType, Int64Type},
    },
    error::DataFusionError,
    logical_plan::create_udf,
    physical_plan::{
        functions::{make_scalar_function, ReturnTypeFunction, Signature, Volatility},
        udf::ScalarUDF,
    },
};

use crate::compile::QueryPlannerExecutionProps;

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

pub fn create_db_udf(name: String, props: &QueryPlannerExecutionProps) -> ScalarUDF {
    // Due our requirements it's more easy to clone this variable rather then Arc
    let db_state = props.database.clone().unwrap_or("db".to_string());

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

pub fn create_user_udf(props: &QueryPlannerExecutionProps) -> ScalarUDF {
    // Due our requirements it's more easy to clone this variable rather then Arc
    let state_user = props.user.clone();

    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        if let Some(user) = &state_user {
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

pub fn create_current_user_udf(props: &QueryPlannerExecutionProps) -> ScalarUDF {
    // Due our requirements it's more easy to clone this variable rather then Arc
    let state_user = props.user.clone();

    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = StringBuilder::new(1);
        if let Some(user) = &state_user {
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

pub fn create_connection_id_udf(props: &QueryPlannerExecutionProps) -> ScalarUDF {
    // Due our requirements it's more easy to clone this variable rather then Arc
    let state_connection_id = props.connection_id;

    let version = make_scalar_function(move |_args: &[ArrayRef]| {
        let mut builder = UInt32Builder::new(1);
        builder.append_value(state_connection_id).unwrap();

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

        let arg1_arr = downcast_string_arg!(args[0], "str", i32);
        let arg2_arr = downcast_string_arg!(args[1], "substr", i32);

        let input_str = arg1_arr.value(0);
        let input_substr = arg2_arr.value(0);

        let mut builder = Int32Builder::new(1);

        if let Some(idx) = input_str.to_string().find(input_substr) {
            builder.append_value((idx as i32) + 1)?;
        } else {
            builder.append_value(0)?;
        };

        Ok(Arc::new(builder.finish()) as ArrayRef)
    });

    create_udf(
        "instr",
        vec![DataType::Utf8, DataType::Utf8],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        fun,
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

        if left.data_type() != right.data_type() {
            return Err(DataFusionError::Execution(format!(
                "positive and negative results must be the same type, actual: [{}, {}]",
                left.data_type(),
                right.data_type(),
            )));
        }

        let is_true: bool = match condition.data_type() {
            DataType::Boolean => {
                if condition.is_null(0) {
                    false
                } else {
                    let arr = downcast_boolean_arr!(condition);
                    arr.value(0)
                }
            }
            _ => false,
        };

        let result = if is_true { left.clone() } else { right.clone() };

        Ok(result)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() == 3);

        Ok(Arc::new(types[1].clone()))
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

        let result = if left.is_null(0) {
            right.clone()
        } else if right.is_null(0) {
            left.clone()
        } else {
            match &left.data_type() {
                DataType::Int64 => {
                    let l = downcast_primitive_arg!(left, "left", Int64Type);
                    let r = downcast_primitive_arg!(right, "right", Int64Type);

                    if l.value(0) < r.value(0) {
                        left.clone()
                    } else {
                        right.clone()
                    }
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "unsupported type in least function, actual: {}",
                        left.data_type()
                    )));
                }
            }
        };

        Ok(result)
    });

    let return_type: ReturnTypeFunction = Arc::new(move |types| {
        assert!(types.len() == 2);

        Ok(Arc::new(types[0].clone()))
    });

    ScalarUDF::new(
        "least",
        &Signature::uniform(
            2,
            vec![
                DataType::Int32,
                DataType::UInt32,
                DataType::UInt64,
                DataType::Int64,
            ],
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
