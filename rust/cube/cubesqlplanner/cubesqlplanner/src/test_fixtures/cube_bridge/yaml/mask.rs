use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum YamlMask {
    SqlMask { sql: String },
    Number(f64),
    Bool(bool),
    StringVal(String),
}

impl YamlMask {
    pub fn to_sql_string(self) -> String {
        match self {
            YamlMask::SqlMask { sql } => sql,
            YamlMask::Number(n) => {
                if n == (n as i64) as f64 {
                    format!("({})", n as i64)
                } else {
                    format!("({})", n)
                }
            }
            YamlMask::Bool(b) => {
                if b {
                    "(TRUE)".to_string()
                } else {
                    "(FALSE)".to_string()
                }
            }
            YamlMask::StringVal(s) => format!("'{}'", s.replace('\'', "''")),
        }
    }
}
