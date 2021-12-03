use std::any::type_name;
use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, GenericStringArray, Int32Builder, StringBuilder,
            UInt32Builder,
        },
        datatypes::DataType,
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

    let return_type: ReturnTypeFunction =
        Arc::new(move |_| Ok(Arc::new(DataType::Boolean).clone()));

    ScalarUDF::new(
        "isnull",
        &Signature::any(1, Volatility::Immutable),
        &return_type,
        &fun,
    )
}
