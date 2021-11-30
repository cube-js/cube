use std::collections::HashMap;

use datafusion::error::Result;
use datafusion::{scalar::ScalarValue, variable::VarProvider};

pub struct SystemVar {
    variables: HashMap<String, ScalarValue>,
}

impl SystemVar {
    /// new system variable
    pub fn new() -> Self {
        let mut variables = HashMap::new();
        variables.insert(
            "@@max_allowed_packet".to_string(),
            ScalarValue::UInt32(Some(67108864)),
        );
        variables.insert(
            "@@auto_increment_increment".to_string(),
            ScalarValue::UInt32(Some(1)),
        );
        variables.insert(
            "@@version_comment".to_string(),
            ScalarValue::Utf8(Some("mysql".to_string())),
        );
        variables.insert(
            "@@sessiontransaction_isolation".to_string(),
            ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
        );
        variables.insert(
            "@@sessionauto_increment_increment".to_string(),
            ScalarValue::Int64(Some(1)),
        );
        variables.insert(
            "@@character_set_client".to_string(),
            ScalarValue::Utf8(Some("utf8mb4".to_string())),
        );
        variables.insert(
            "@@character_set_connection".to_string(),
            ScalarValue::Utf8(Some("utf8mb4".to_string())),
        );
        variables.insert(
            "@@character_set_results".to_string(),
            ScalarValue::Utf8(Some("utf8mb4".to_string())),
        );
        variables.insert(
            "@@character_set_server".to_string(),
            ScalarValue::Utf8(Some("utf8mb4".to_string())),
        );
        variables.insert(
            "@@collation_connection".to_string(),
            ScalarValue::Utf8(Some("utf8mb4_general_ci".to_string())),
        );
        variables.insert(
            "@@system_time_zone".to_string(),
            ScalarValue::Utf8(Some("UTC".to_string())),
        );
        variables.insert(
            "@@time_zone".to_string(),
            ScalarValue::Utf8(Some("SYSTEM".to_string())),
        );

        Self { variables }
    }
}

impl VarProvider for SystemVar {
    /// get system variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        let key = var_names.concat();

        if let Some(value) = self.variables.get(&key) {
            Ok(value.clone())
        } else {
            Ok(ScalarValue::Utf8(None))
        }
    }
}
