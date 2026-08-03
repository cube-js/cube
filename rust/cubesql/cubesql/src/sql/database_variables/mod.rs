use crate::compile::DatabaseVariables;

pub mod postgres;

pub fn postgres_default_session_variables() -> DatabaseVariables {
    postgres::session_vars::defaults()
}

pub fn postgres_default_global_variables() -> DatabaseVariables {
    postgres::global_vars::defaults()
}
