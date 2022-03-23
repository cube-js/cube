use std::collections::HashMap;

use datafusion::scalar::ScalarValue;

use crate::sql::{database_variables::DatabaseVariable, session::DatabaseProtocol};

fn append_to_hashmap(hm: &mut HashMap<String, DatabaseVariable>, key: &str, value: ScalarValue) {
    let mut row: HashMap<String, ScalarValue> = HashMap::new();
    row.insert(
        "VARIABLE_NAME".to_string(),
        ScalarValue::Utf8(Some(key.to_string())),
    );
    row.insert("VARIABLE_VALUE".to_string(), value);

    hm.insert(
        key.to_string(),
        DatabaseVariable::new(row, DatabaseProtocol::MySQL),
    );
}

pub fn defaults() -> HashMap<String, DatabaseVariable> {
    let mut variables: HashMap<String, DatabaseVariable> = HashMap::new();

    append_to_hashmap(
        &mut variables,
        "max_allowed_packet",
        ScalarValue::UInt32(Some(67108864)),
    );
    append_to_hashmap(
        &mut variables,
        "auto_increment_increment",
        ScalarValue::UInt32(Some(1)),
    );
    append_to_hashmap(
        &mut variables,
        "version_comment",
        ScalarValue::Utf8(Some("mysql".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "system_time_zone",
        ScalarValue::Utf8(Some("UTC".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "time_zone",
        ScalarValue::Utf8(Some("SYSTEM".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "tx_isolation",
        ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "tx_read_only",
        ScalarValue::Boolean(Some(false)),
    );
    append_to_hashmap(
        &mut variables,
        "transaction_isolation",
        ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "transaction_read_only",
        ScalarValue::Boolean(Some(false)),
    );
    append_to_hashmap(
        &mut variables,
        "sessiontransaction_isolation",
        ScalarValue::Utf8(Some("REPEATABLE-READ".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "transaction_read_only",
        ScalarValue::Int64(Some(1)),
    );
    append_to_hashmap(
        &mut variables,
        "character_set_client",
        ScalarValue::Utf8(Some("utf8mb4".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "character_set_connection",
        ScalarValue::Utf8(Some("utf8mb4".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "character_set_results",
        ScalarValue::Utf8(Some("utf8mb4".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "character_set_server",
        ScalarValue::Utf8(Some("utf8mb4".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "collation_connection",
        ScalarValue::Utf8(Some("utf8mb4_general_ci".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "collation_server",
        ScalarValue::Utf8(Some("utf8mb4_0900_ai_ci".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "init_connect",
        ScalarValue::Utf8(Some("".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "interactive_timeout",
        ScalarValue::Int32(Some(28800)),
    );
    append_to_hashmap(
        &mut variables,
        "license",
        ScalarValue::Utf8(Some("Apache 2".to_string())),
    );
    append_to_hashmap(
        &mut variables,
        "lower_case_table_names",
        ScalarValue::Int32(Some(0)),
    );
    append_to_hashmap(
        &mut variables,
        "net_buffer_length",
        ScalarValue::Int32(Some(16384)),
    );
    append_to_hashmap(
        &mut variables,
        "net_write_timeout",
        ScalarValue::Int32(Some(600)),
    );
    append_to_hashmap(
        &mut variables,
        "wait_timeout",
        ScalarValue::Int32(Some(28800)),
    );
    append_to_hashmap(&mut variables, "sql_mode", ScalarValue::Utf8(Some("ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION".to_string())));

    variables
}
