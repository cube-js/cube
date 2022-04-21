use datafusion::scalar::ScalarValue;
use std::collections::HashMap;

use crate::sql::database_variables::{DatabaseVariable, DatabaseVariables};

pub fn defaults() -> DatabaseVariables {
    let mut variables: DatabaseVariables = HashMap::new();

    variables.insert(
        "client_min_messages".to_string(),
        DatabaseVariable::system(
            "client_min_messages".to_string(),
            ScalarValue::Utf8(Some("NOTICE".to_string())),
            None,
        ),
    );

    variables.insert(
        "timezone".to_string(),
        DatabaseVariable::system(
            "timezone".to_string(),
            ScalarValue::Utf8(Some("GMT".to_string())),
            None,
        ),
    );

    variables.insert(
        "application_name".to_string(),
        DatabaseVariable::system(
            "application_name".to_string(),
            ScalarValue::Utf8(None),
            None,
        ),
    );

    variables.insert(
        "extra_float_digits".to_string(),
        DatabaseVariable::system(
            "extra_float_digits".to_string(),
            ScalarValue::UInt32(Some(1)),
            None,
        ),
    );

    variables
}
