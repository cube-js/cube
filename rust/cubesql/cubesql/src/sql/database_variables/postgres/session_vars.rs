use std::collections::HashMap;

use crate::sql::database_variables::DatabaseVariables;

pub fn defaults() -> DatabaseVariables {
    HashMap::new()
}
