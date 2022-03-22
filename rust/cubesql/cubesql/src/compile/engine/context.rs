use std::collections::HashMap;

use datafusion::error::Result;
use datafusion::variable::VarType;
use datafusion::{scalar::ScalarValue, variable::VarProvider};
use log::warn;

use crate::sql::database_variables::DatabaseVariable;

pub struct VariablesProvider {
    variables: HashMap<String, ScalarValue>,
}

impl VariablesProvider {
    pub fn new(variables: HashMap<String, DatabaseVariable>, var_type: VarType) -> Self {
        let mut vars: HashMap<String, ScalarValue> = HashMap::new();
        for (k, v) in variables.into_iter() {
            if var_type == v.var_type {
                vars.insert(format!("{}{}", "@@", k), ScalarValue::Utf8(Some(v.value)));
            }
        }

        Self { variables: vars }
    }
}

impl VarProvider for VariablesProvider {
    /// get system variable value
    fn get_value(&self, identifier: Vec<String>) -> Result<ScalarValue> {
        let key = if identifier.len() > 1 {
            let ignore_first = identifier[0].to_ascii_lowercase() == "@@global".to_owned();

            if ignore_first {
                "@@".to_string() + &identifier[1..].concat()
            } else {
                identifier.concat()
            }
        } else {
            identifier.concat()
        };

        if let Some(value) = self.variables.get(&key) {
            Ok(value.clone())
        } else {
            warn!("Unknown system variable: {}", key);

            Ok(ScalarValue::Utf8(None))
        }
    }
}
