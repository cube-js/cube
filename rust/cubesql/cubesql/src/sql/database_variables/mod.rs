use std::collections::HashMap;

use datafusion::{scalar::ScalarValue, variable::VarType};
use serde_json::Value;

use super::session::DatabaseProtocol;

pub mod mysql;

#[derive(Debug, Clone)]
pub struct DatabaseVariable {
    pub name: String,
    pub value: String,
    pub var_type: VarType,
    pub readonly: bool,
    pub db_params: HashMap<String, ScalarValue>,
}

impl DatabaseVariable {
    pub fn new(params: HashMap<String, ScalarValue>, protocol: DatabaseProtocol) -> Self {
        match protocol {
            DatabaseProtocol::PostgreSQL => Self {
                name: "".to_string(),
                value: "".to_string(),
                var_type: VarType::System,
                readonly: false,
                db_params: params,
            },
            DatabaseProtocol::MySQL => Self {
                name: params["VARIABLE_NAME"].to_string(),
                value: params["VARIABLE_VALUE"].to_string(),
                var_type: VarType::System,
                readonly: true,
                db_params: params,
            },
        }
    }
}

pub fn mysql_default_session_variables() -> HashMap<String, DatabaseVariable> {
    parse_mysql_str(mysql::session_vars::DEFAULT_VARS)
}

pub fn mysql_default_global_variables() -> HashMap<String, DatabaseVariable> {
    parse_mysql_str(mysql::global_vars::DEFAULT_VARS)
}

fn parse_mysql_str(str: &str) -> HashMap<String, DatabaseVariable> {
    match serde_json::from_str(&str).expect("Unable to parse") {
        Value::Array(arr) => {
            let mut result: HashMap<String, DatabaseVariable> = HashMap::new();

            arr.iter().for_each(|x| {
                let mut hm: HashMap<String, ScalarValue> = HashMap::new();
                hm.insert(
                    "VARIABLE_NAME".to_string(),
                    ScalarValue::Utf8(Some(x["VARIABLE_NAME"].as_str().unwrap().to_string())),
                );
                hm.insert(
                    "VARIABLE_VALUE".to_string(),
                    ScalarValue::Utf8(Some(x["VARIABLE_VALUE"].as_str().unwrap().to_string())),
                );
                result.insert(
                    hm["VARIABLE_NAME"].to_string(),
                    DatabaseVariable::new(hm, DatabaseProtocol::MySQL),
                );
            });

            result
        }
        _ => return HashMap::new(),
    }
}
