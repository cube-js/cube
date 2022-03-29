use std::sync::Arc;

use datafusion::error::Result;
use datafusion::variable::VarType;
use datafusion::{scalar::ScalarValue, variable::VarProvider};
use log::warn;

use crate::sql::SessionState;

pub struct VariablesProvider {
    session: Arc<SessionState>,
    var_type: VarType,
}

impl VariablesProvider {
    pub fn new(session: Arc<SessionState>, var_type: VarType) -> Self {
        Self { session, var_type }
    }
}

impl VarProvider for VariablesProvider {
    /// get system variable value
    fn get_value(&self, identifier: Vec<String>) -> Result<ScalarValue> {
        let key = if identifier.len() > 1 {
            let ignore_first = identifier[0].to_ascii_lowercase() == "@@global".to_owned();

            if ignore_first {
                identifier[1..].concat()
            } else {
                identifier.concat()[2..].to_string()
            }
        } else {
            identifier.concat()[2..].to_string()
        };

        if let Some(var) = self.session.all_variables().get(&key) {
            if var.var_type == self.var_type {
                return Ok(var.value.clone());
            }
        }

        warn!("Unknown system variable: {}", key);

        Ok(ScalarValue::Utf8(None))
    }
}
