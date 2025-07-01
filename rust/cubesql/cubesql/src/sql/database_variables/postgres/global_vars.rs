use datafusion::scalar::ScalarValue;

use crate::compile::{DatabaseVariable, DatabaseVariables};

pub fn defaults() -> DatabaseVariables {
    let variables = [
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
    ];

    let variables = IntoIterator::into_iter(variables)
        .map(|v| (v.name.clone(), v))
        .collect::<DatabaseVariables>();

    variables
}
