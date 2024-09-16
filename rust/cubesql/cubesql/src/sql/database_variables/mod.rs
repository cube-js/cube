use crate::compile::DatabaseVariables;

pub mod mysql;
pub mod postgres;

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
