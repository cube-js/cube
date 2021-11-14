use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{ArrayRef, StringBuilder, UInt32Builder},
        datatypes::DataType,
    },
    logical_plan::create_udf,
    physical_plan::{
        functions::{make_scalar_function, Volatility},
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
