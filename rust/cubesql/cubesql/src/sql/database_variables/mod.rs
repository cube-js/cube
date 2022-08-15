use std::collections::HashMap;

use datafusion::{scalar::ScalarValue, variable::VarType};

pub mod mysql;
pub mod postgres;

pub type DatabaseVariablesToUpdate = Vec<DatabaseVariable>;
pub type DatabaseVariables = HashMap<String, DatabaseVariable>;

#[derive(Debug, Clone)]
pub struct DatabaseVariable {
    pub name: String,
    pub value: ScalarValue,
    pub var_type: VarType,
    pub readonly: bool,
    // Postgres schema includes a range of additional parameters
    pub additional_params: Option<HashMap<String, ScalarValue>>,
}

impl DatabaseVariable {
    pub fn system(
        name: String,
        value: ScalarValue,
        additional_params: Option<HashMap<String, ScalarValue>>,
    ) -> Self {
        Self {
            name: name,
            value: value,
            var_type: VarType::System,
            readonly: false,
            additional_params,
        }
    }

    pub fn user_defined(
        name: String,
        value: ScalarValue,
        additional_params: Option<HashMap<String, ScalarValue>>,
    ) -> Self {
        Self {
            name: name,
            value: value,
            var_type: VarType::UserDefined,
            readonly: false,
            additional_params,
        }
    }
}

pub fn mysql_default_session_variables() -> DatabaseVariables {
    mysql::session_vars::defaults()
}

pub fn mysql_default_global_variables() -> DatabaseVariables {
    mysql::global_vars::defaults()
}

pub fn postgres_default_session_variables() -> DatabaseVariables {
    postgres::session_vars::defaults()
}

pub fn postgres_default_global_variables() -> DatabaseVariables {
    postgres::global_vars::defaults()
}
