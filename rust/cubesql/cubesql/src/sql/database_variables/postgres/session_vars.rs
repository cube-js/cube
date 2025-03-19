use datafusion::scalar::ScalarValue;

use crate::compile::{DatabaseVariable, DatabaseVariables};

pub const CUBESQL_PENALIZE_POST_PROCESSING_VAR: &str = "cubesql_penalize_post_processing";

pub fn defaults() -> DatabaseVariables {
    let variables = [
        DatabaseVariable::system(
            "client_min_messages".to_string(),
            ScalarValue::Utf8(Some("NOTICE".to_string())),
            None,
        ),
        DatabaseVariable::system(
            "timezone".to_string(),
            ScalarValue::Utf8(Some("GMT".to_string())),
            None,
        ),
        DatabaseVariable::system(
            "application_name".to_string(),
            ScalarValue::Utf8(None),
            None,
        ),
        DatabaseVariable::system(
            "extra_float_digits".to_string(),
            ScalarValue::UInt32(Some(1)),
            None,
        ),
        DatabaseVariable::system(
            "transaction_isolation".to_string(),
            ScalarValue::Utf8(Some("read committed".to_string())),
            None,
        ),
        DatabaseVariable::system(
            "max_allowed_packet".to_string(),
            ScalarValue::UInt32(Some(67108864)),
            None,
        ),
        DatabaseVariable::system(
            "max_index_keys".to_string(),
            ScalarValue::UInt32(Some(32)),
            None,
        ),
        DatabaseVariable::system(
            "lc_collate".to_string(),
            ScalarValue::Utf8(Some("en_US.utf8".to_string())),
            None,
        ),
        DatabaseVariable::system(
            "standard_conforming_strings".to_string(),
            ScalarValue::Utf8(Some("on".to_string())),
            None,
        ),
        DatabaseVariable::system(
            "max_identifier_length".to_string(),
            ScalarValue::UInt32(Some(63)),
            None,
        ),
        DatabaseVariable::system(
            "role".to_string(),
            ScalarValue::Utf8(Some("none".to_string())),
            None,
        ),
        // Custom cubesql variables
        DatabaseVariable::user_defined(
            CUBESQL_PENALIZE_POST_PROCESSING_VAR.to_string(),
            ScalarValue::Boolean(Some(false)),
            None,
        ),
    ];

    let variables = IntoIterator::into_iter(variables)
        .map(|v| (v.name.clone(), v))
        .collect::<DatabaseVariables>();

    variables
}
