use std::collections::HashMap;

use datafusion::{scalar::ScalarValue, variable::VarType};

use super::session::DatabaseProtocol;

pub mod mysql;

#[derive(Debug, Clone)]
pub struct DatabaseVariable {
    pub name: String,
    pub value: ScalarValue,
    pub var_type: VarType,
    pub readonly: bool,
    pub db_params: HashMap<String, ScalarValue>,
}

impl DatabaseVariable {
    pub fn new(params: HashMap<String, ScalarValue>, protocol: DatabaseProtocol) -> Self {
        match protocol {
            DatabaseProtocol::PostgreSQL => Self {
                name: "".to_string(),
                value: ScalarValue::Utf8(Some("".to_string())),
                var_type: VarType::System,
                readonly: false,
                db_params: params,
            },
            DatabaseProtocol::MySQL => Self {
                name: params["VARIABLE_NAME"].to_string(),
                value: params["VARIABLE_VALUE"].clone(),
                var_type: VarType::System,
                readonly: true,
                db_params: params,
            },
        }
    }
}

pub fn mysql_default_session_variables() -> HashMap<String, DatabaseVariable> {
    mysql::session_vars::defaults()
}

pub fn mysql_default_global_variables() -> HashMap<String, DatabaseVariable> {
    mysql::global_vars::defaults()
}
