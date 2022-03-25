use std::collections::HashMap;

use datafusion::scalar::ScalarValue;

use crate::sql::database_variables::{DatabaseVariable, DatabaseVariables};

pub fn defaults() -> DatabaseVariables {
    let mut variables: DatabaseVariables = HashMap::new();

    variables.insert(
        "max_allowed_packet".to_string(),
        DatabaseVariable::system(
            "max_allowed_packet".to_string(),
            ScalarValue::UInt32(Some(67108864)),
            None,
        ),
    );

    variables
}
