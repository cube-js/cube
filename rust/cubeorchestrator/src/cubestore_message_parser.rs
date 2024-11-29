use neon::prelude::Finalize;
use cubeshared::codegen::{root_as_http_message, HttpCommand};

#[derive(Debug)]
pub enum ParseError {
    UnsupportedCommand,
    EmptyResultSet,
    NullRow,
    ColumnNameNotDefined,
    FlatBufferError,
    ErrorMessage(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnsupportedCommand => write!(f, "Unsupported command"),
            ParseError::EmptyResultSet => write!(f, "Empty resultSet"),
            ParseError::NullRow => write!(f, "Null row"),
            ParseError::ColumnNameNotDefined => write!(f, "Column name is not defined"),
            ParseError::FlatBufferError => write!(f, "FlatBuffer parsing error"),
            ParseError::ErrorMessage(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

pub struct CubeStoreResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl Finalize for CubeStoreResult {}

impl CubeStoreResult {
    pub fn new(msg_data: &[u8]) -> Result<Self, ParseError> {
        let mut result = CubeStoreResult {
            columns: vec![],
            rows: vec![],
        };

        let http_message = root_as_http_message(msg_data)
            .map_err(|_| ParseError::FlatBufferError)?;

        match http_message.command_type() {
            HttpCommand::HttpError => {
                let http_error = http_message
                    .command_as_http_error()
                    .ok_or(ParseError::FlatBufferError)?;
                let error_message = http_error.error().unwrap_or("Unknown error").to_string();
                Err(ParseError::ErrorMessage(error_message))
            }
            HttpCommand::HttpResultSet => {
                let result_set = http_message
                    .command_as_http_result_set()
                    .ok_or(ParseError::EmptyResultSet)?;

                let result_set_columns = result_set.columns().ok_or(ParseError::EmptyResultSet)?;

                if result_set_columns.iter().any(|c| c.is_empty()) {
                    return Err(ParseError::ColumnNameNotDefined);
                }

                result.columns = result_set_columns.iter().map(|val| val.to_owned()).collect();
                let result_set_rows = result_set.rows().ok_or(ParseError::EmptyResultSet)?;
                result.rows = Vec::with_capacity(result_set_rows.len());

                for row in result_set_rows.iter() {
                    let values = row.values().ok_or(ParseError::NullRow)?;
                    let row_obj: Vec<_> = values
                        .iter()
                        .map(|val| val.string_value().unwrap_or("").to_owned())
                        .collect();

                    result.rows.push(row_obj);
                }

                Ok(result)
            },
            _ => Err(ParseError::UnsupportedCommand),
        }
    }
}
