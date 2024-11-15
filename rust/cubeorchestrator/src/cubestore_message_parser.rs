use cubeshared::codegen::{root_as_http_message, HttpCommand};
use std::collections::HashMap;

#[derive(Debug)]
pub enum ParseError {
    UnsupportedCommand,
    EmptyResultSet,
    NullRow,
    ColumnValueMissed,
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
            ParseError::ColumnValueMissed => write!(f, "Column value missed"),
            ParseError::ColumnNameNotDefined => write!(f, "Column name is not defined"),
            ParseError::FlatBufferError => write!(f, "FlatBuffer parsing error"),
            ParseError::ErrorMessage(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

pub fn parse_cubestore_ws_result(
    msg_data: Vec<u8>,
) -> Result<Vec<HashMap<String, String>>, ParseError> {
    let http_message =
        root_as_http_message(msg_data.as_slice()).map_err(|_| ParseError::FlatBufferError)?;

    let command_type = http_message.command_type();

    match command_type {
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
            let columns_len = result_set_columns.len();
            let mut columns = Vec::with_capacity(columns_len);

            for column in result_set_columns.iter() {
                if column.is_empty() {
                    return Err(ParseError::ColumnNameNotDefined);
                }
                columns.push(column.to_string());
            }

            let result_set_rows = result_set.rows().ok_or(ParseError::EmptyResultSet)?;
            let mut result = Vec::with_capacity(result_set_rows.len());

            for row in result_set_rows.iter() {
                let values = row.values().ok_or(ParseError::NullRow)?;
                let mut row_obj = HashMap::with_capacity(columns_len);

                for (i, val) in values.iter().enumerate() {
                    let value = val.string_value().ok_or(ParseError::ColumnValueMissed)?;
                    row_obj.insert(columns[i].clone(), value.to_string());
                }

                result.push(row_obj);
            }

            Ok(result)
        }
        _ => Err(ParseError::UnsupportedCommand),
    }
}
