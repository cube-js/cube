use crate::{
    compile::engine::{
        df::scan::{ArrayRef, DataFusionError, DataType},
        udf::{common::ReturnTypeFunction, utils::downcast_string_arg},
    },
    sql::SessionState,
};
use datafusion::{
    arrow::array::BooleanArray,
    logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility},
    physical_plan::functions::make_scalar_function,
};
use itertools::izip;
use std::{any::type_name, sync::Arc};

// has_any_column_privilege ( [ user name or oid, ] table text or oid, privilege text ) → boolean
pub fn create_has_any_column_privilege_udf(state: Arc<SessionState>) -> ScalarUDF {
    let fun = make_scalar_function(move |args: &[ArrayRef]| {
        let (users, tables, privileges) = if args.len() == 3 {
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
                                "update" | "insert" | "delete" => {
                                    result = false;
                                }
                                "select" => {}
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
            .collect::<crate::compile::engine::df::scan::Result<BooleanArray>>();

        Ok(Arc::new(result?))
    });

    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Boolean)));

    ScalarUDF::new(
        "has_any_column_privilege",
        &Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::UInt32, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::UInt32, DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::UInt32, DataType::UInt32, DataType::Utf8]),
            ],
            Volatility::Stable,
        ),
        &return_type,
        &fun,
    )
}
