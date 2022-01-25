use std::collections::HashMap;

use datafusion::error::Result;
use datafusion::{scalar::ScalarValue, variable::VarProvider};
use log::warn;

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
            "@@system_time_zone".to_string(),
            ScalarValue::Utf8(Some("UTC".to_string())),
        );
        variables.insert(
            "@@globaltime_zone".to_string(),
            ScalarValue::Utf8(Some("SYSTEM".to_string())),
        );
        variables.insert(
            "@@time_zone".to_string(),
            ScalarValue::Utf8(Some("SYSTEM".to_string())),
        );
        // Isolation old variables
        variables.insert(
            "@@tx_isolation".to_string(),
            ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
        );
        variables.insert(
            "@@tx_read_only".to_string(),
            ScalarValue::Boolean(Some(false)),
        );
        // Isolation new variables after 8.0.3
        variables.insert(
            "@@transaction_isolation".to_string(),
            ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
        );
        variables.insert(
            "@@transaction_read_only".to_string(),
            ScalarValue::Boolean(Some(false)),
        );
        // Session
        variables.insert(
            "@@sessiontransaction_isolation".to_string(),
            ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
        );
        variables.insert(
            "@@sessionauto_increment_increment".to_string(),
            ScalarValue::Int64(Some(1)),
        );
        // character
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
            "@@collation_server".to_string(),
            ScalarValue::Utf8(Some("utf8mb4_0900_ai_ci".to_string())),
        );
        variables.insert(
            "@@init_connect".to_string(),
            ScalarValue::Utf8(Some("".to_string())),
        );
        variables.insert(
            "@@interactive_timeout".to_string(),
            ScalarValue::UInt32(Some(28800)),
        );
        variables.insert(
            "@@license".to_string(),
            ScalarValue::Utf8(Some("Apache 2".to_string())),
        );
        variables.insert(
            "@@lower_case_table_names".to_string(),
            ScalarValue::UInt32(Some(0)),
        );
        variables.insert(
            "@@net_buffer_length".to_string(),
            ScalarValue::UInt32(Some(16384)),
        );
        variables.insert(
            "@@net_write_timeout".to_string(),
            ScalarValue::UInt32(Some(600)),
        );
        variables.insert(
            "@@wait_timeout".to_string(),
            ScalarValue::UInt32(Some(28800)),
        );
        variables.insert(
            "@@sql_mode".to_string(),
            ScalarValue::Utf8(Some("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION".to_string()))
        );

        Self { variables }
    }
}

impl VarProvider for SystemVar {
    /// get system variable value
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue> {
        let key = var_names.concat().to_lowercase();

        if let Some(value) = self.variables.get(&key) {
            Ok(value.clone())
        } else {
            warn!("Unknown system variable: {}", key);

            Ok(ScalarValue::Utf8(None))
        }
    }
}
