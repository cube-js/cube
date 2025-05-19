use crate::{
    query_result_transform::{DBResponsePrimitive, DBResponseValue},
    transport::JsRawData,
};
use cubeshared::codegen::{root_as_http_message, HttpCommand};
use neon::prelude::Finalize;
use std::collections::HashMap;

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

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<DBResponseValue>>,
    pub columns_pos: HashMap<String, usize>,
}

impl Finalize for QueryResult {}

impl QueryResult {
    pub fn from_cubestore_fb(msg_data: &[u8]) -> Result<Self, ParseError> {
        let mut result = QueryResult {
            columns: vec![],
            rows: vec![],
            columns_pos: HashMap::new(),
        };

        let http_message =
            root_as_http_message(msg_data).map_err(|_| ParseError::FlatBufferError)?;

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

                if let Some(result_set_columns) = result_set.columns() {
                    if result_set_columns.iter().any(|c| c.is_empty()) {
                        return Err(ParseError::ColumnNameNotDefined);
                    }

                    let (columns, columns_pos): (Vec<_>, HashMap<_, _>) = result_set_columns
                        .iter()
                        .enumerate()
                        .map(|(index, column_name)| {
                            (column_name.to_owned(), (column_name.to_owned(), index))
                        })
                        .unzip();

                    result.columns = columns;
                    result.columns_pos = columns_pos;
                }

                if let Some(result_set_rows) = result_set.rows() {
                    result.rows = Vec::with_capacity(result_set_rows.len());

                    for row in result_set_rows.iter() {
                        let values = row.values().ok_or(ParseError::NullRow)?;
                        let row_obj: Vec<_> = values
                            .iter()
                            .map(|val| match val.string_value() {
                                Some(s) => DBResponseValue::Primitive(DBResponsePrimitive::String(
                                    s.to_owned(),
                                )),
                                None => DBResponseValue::Primitive(DBResponsePrimitive::Null),
                            })
                            .collect();

                        result.rows.push(row_obj);
                    }
                }

                Ok(result)
            }
            _ => Err(ParseError::UnsupportedCommand),
        }
    }

    pub fn from_js_raw_data(js_raw_data: JsRawData) -> Result<Self, ParseError> {
        if js_raw_data.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                columns_pos: HashMap::new(),
            });
        }

        let first_row = &js_raw_data[0];
        let columns: Vec<String> = first_row.keys().cloned().collect();
        let columns_pos: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.clone(), index))
            .collect();

        let rows: Vec<Vec<DBResponseValue>> = js_raw_data
            .into_iter()
            .map(|row_map| {
                columns
                    .iter()
                    .map(|col| {
                        row_map
                            .get(col)
                            .map(|val| DBResponseValue::Primitive(val.clone()))
                            .unwrap_or(DBResponseValue::Primitive(DBResponsePrimitive::Null))
                    })
                    .collect()
            })
            .collect();

        Ok(QueryResult {
            columns,
            rows,
            columns_pos,
        })
    }
}
