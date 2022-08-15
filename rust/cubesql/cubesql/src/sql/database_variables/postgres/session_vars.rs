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

    variables.insert(
        "transaction_isolation".to_string(),
        DatabaseVariable::system(
            "transaction_isolation".to_string(),
            ScalarValue::Utf8(Some("read committed".to_string())),
            None,
        ),
    );

    variables.insert(
        "max_allowed_packet".to_string(),
        DatabaseVariable::system(
            "max_allowed_packet".to_string(),
            ScalarValue::UInt32(Some(67108864)),
            None,
        ),
    );

    variables.insert(
        "max_index_keys".to_string(),
        DatabaseVariable::system(
            "max_index_keys".to_string(),
            ScalarValue::UInt32(Some(32)),
            None,
        ),
    );

    variables.insert(
        "lc_collate".to_string(),
        DatabaseVariable::system(
            "lc_collate".to_string(),
            ScalarValue::Utf8(Some("en_US.utf8".to_string())),
            None,
        ),
    );

    variables.insert(
        "standard_conforming_strings".to_string(),
        DatabaseVariable::system(
            "standard_conforming_strings".to_string(),
            ScalarValue::Utf8(Some("on".to_string())),
            None,
        ),
    );

    variables
}
