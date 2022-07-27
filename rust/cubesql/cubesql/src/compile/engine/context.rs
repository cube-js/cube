use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    error::Result,
    scalar::ScalarValue,
    variable::{VarProvider, VarType},
};
use log::warn;

use crate::sql::{session::DatabaseProtocol, ServerManager, SessionState};

pub struct VariablesProvider {
    session: Arc<SessionState>,
    server: Arc<ServerManager>,
}

impl VariablesProvider {
    pub fn new(session: Arc<SessionState>, server: Arc<ServerManager>) -> Self {
        Self { session, server }
    }

    fn get_session_value(&self, identifier: Vec<String>, var_type: VarType) -> Result<ScalarValue> {
        let key = if identifier.len() > 1 {
            let ignore_first = identifier[0].to_ascii_lowercase() == "@@session".to_owned();
            if ignore_first {
                identifier[1..].concat()
            } else {
                identifier.concat()[1..].to_string()
            }
        } else {
            identifier.concat()[1..].to_string()
        };

        if let Some(var) = self.session.get_variable(&key) {
            if var.var_type == var_type {
                return Ok(var.value.clone());
            }
        }

        warn!("Unknown session variable: {}", key);

        Ok(ScalarValue::Utf8(None))
    }

    fn get_global_value(&self, identifier: Vec<String>) -> Result<ScalarValue> {
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

        if let Some(var) = self
            .server
            .read_variables(DatabaseProtocol::MySQL)
            .get(&key)
        {
            if var.var_type == VarType::System {
                return Ok(var.value.clone());
            }
        }

        warn!("Unknown system variable: {}", key);

        Ok(ScalarValue::Utf8(None))
    }
}

impl VarProvider for VariablesProvider {
    /// get variable value
    fn get_value(&self, identifier: Vec<String>) -> Result<ScalarValue> {
        let first_word_vec: Vec<char> = identifier[0].chars().collect();
        if first_word_vec.len() < 2 {
            return Ok(ScalarValue::Utf8(None));
        }

        match (&first_word_vec[0], &first_word_vec[1]) {
            ('@', '@') => {
                if identifier.len() > 1
                    && identifier[0].to_ascii_lowercase() == "@@session".to_owned()
                {
                    return self.get_session_value(identifier, VarType::System);
                }

                return self.get_global_value(identifier);
            }
            ('@', _) => return self.get_session_value(identifier, VarType::UserDefined),
            (_, _) => return Ok(ScalarValue::Utf8(None)),
        };
    }

    fn get_type(&self, _var_names: &[String]) -> Option<DataType> {
        Some(DataType::Utf8)
    }
}
