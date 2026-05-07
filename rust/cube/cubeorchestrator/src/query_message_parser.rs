use crate::{
    query_result_transform::{DBResponsePrimitive, DBResponseValue},
    transport::JsRawColumnarData,
};
use cubeshared::codegen::{root_as_http_message_with_opts, HttpCommand};
use cubeshared::flatbuffers::VerifierOptions;
use indexmap::IndexMap;
use neon::prelude::Finalize;

#[derive(Debug)]
pub enum ParseError {
    UnsupportedCommand,
    EmptyResultSet,
    NullRow,
    ColumnNameNotDefined,
    FlatBufferError(String),
    ErrorMessage(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnsupportedCommand => write!(f, "Unsupported command"),
            ParseError::EmptyResultSet => write!(f, "Empty resultSet"),
            ParseError::NullRow => write!(f, "Null row"),
            ParseError::ColumnNameNotDefined => write!(f, "Column name is not defined"),
            ParseError::FlatBufferError(msg) => write!(f, "FlatBuffer parsing error: {}", msg),
            ParseError::ErrorMessage(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub members: Vec<String>,
    pub rows: Vec<Vec<DBResponseValue>>,
    pub columns_pos: IndexMap<String, usize>,
}

impl Finalize for QueryResult {}

impl QueryResult {
    pub fn from_cubestore_fb(msg_data: &[u8]) -> Result<Self, ParseError> {
        let mut result = QueryResult {
            members: vec![],
            rows: vec![],
            columns_pos: IndexMap::new(),
        };

        let opts = VerifierOptions {
            max_tables: 10_000_000,     // Support up to 10M tables
            max_apparent_size: 1 << 31, // 2GB limit for large datasets
            ..Default::default()
        };

        let http_message = root_as_http_message_with_opts(&opts, msg_data)
            .map_err(|err| ParseError::FlatBufferError(err.to_string()))?;

        match http_message.command_type() {
            HttpCommand::HttpError => {
                let http_error = http_message.command_as_http_error().ok_or_else(|| {
                    ParseError::FlatBufferError("Failed to parse HttpError command".to_string())
                })?;
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

                    let (members, columns_pos): (Vec<_>, IndexMap<_, _>) = result_set_columns
                        .iter()
                        .enumerate()
                        .map(|(index, column_name)| {
                            (column_name.to_owned(), (column_name.to_owned(), index))
                        })
                        .unzip();

                    result.members = members;
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

    pub fn from_js_raw_data(js_raw_data: JsRawColumnarData) -> Result<Self, ParseError> {
        let JsRawColumnarData { members, columns } = js_raw_data;

        if members.is_empty() {
            return Ok(QueryResult {
                members: vec![],
                rows: vec![],
                columns_pos: IndexMap::new(),
            });
        }

        let columns_pos: IndexMap<String, usize> = members
            .iter()
            .enumerate()
            .map(|(index, member)| (member.clone(), index))
            .collect();

        let row_count = columns.first().map(|c| c.len()).unwrap_or(0);
        // Transpose column-major input into the row-major shape `QueryResult`
        // expects. Rows are pre-allocated, then we drain each column into the
        // matching slot to avoid per-cell clones.
        let mut rows: Vec<Vec<DBResponseValue>> = (0..row_count)
            .map(|_| Vec::with_capacity(members.len()))
            .collect();

        for column in columns.into_iter() {
            for (row_idx, value) in column.into_iter().enumerate() {
                if let Some(row) = rows.get_mut(row_idx) {
                    row.push(DBResponseValue::Primitive(value));
                }
            }
        }

        Ok(QueryResult {
            members,
            rows,
            columns_pos,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cubeshared::codegen::{
        root_as_http_message_unchecked, HttpColumnValue, HttpColumnValueArgs, HttpCommand,
        HttpMessage, HttpMessageArgs, HttpResultSet, HttpResultSetArgs, HttpRow, HttpRowArgs,
    };
    use cubeshared::flatbuffers::FlatBufferBuilder;

    /// Helper function to create a test HttpMessage with a given number of rows and columns
    fn create_test_message(num_rows: usize, num_columns: usize) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();

        // Create column names
        let column_names: Vec<_> = (0..num_columns)
            .map(|i| builder.create_string(&format!("column_{}", i)))
            .collect();

        // Create rows with values
        let mut rows_vec = Vec::with_capacity(num_rows);
        for row_idx in 0..num_rows {
            // Create column values for this row
            let mut values_vec = Vec::with_capacity(num_columns);
            for col_idx in 0..num_columns {
                let value_str = builder.create_string(&format!("row_{}_col_{}", row_idx, col_idx));
                let col_value = HttpColumnValue::create(
                    &mut builder,
                    &HttpColumnValueArgs {
                        string_value: Some(value_str),
                    },
                );
                values_vec.push(col_value);
            }

            let values_vector = builder.create_vector(&values_vec);
            let row = HttpRow::create(
                &mut builder,
                &HttpRowArgs {
                    values: Some(values_vector),
                },
            );
            rows_vec.push(row);
        }

        // Create the result set
        let columns_vector = builder.create_vector(&column_names);
        let rows_vector = builder.create_vector(&rows_vec);

        let result_set = HttpResultSet::create(
            &mut builder,
            &HttpResultSetArgs {
                columns: Some(columns_vector),
                rows: Some(rows_vector),
            },
        );

        // Create the message
        let connection_id = builder.create_string("test_connection");
        let message = HttpMessage::create(
            &mut builder,
            &HttpMessageArgs {
                message_id: 1,
                command_type: HttpCommand::HttpResultSet,
                command: Some(result_set.as_union_value()),
                connection_id: Some(connection_id),
            },
        );

        builder.finish(message, None);
        builder.finished_data().to_vec()
    }

    #[test]
    fn test_parse_small_result_set() {
        // Small result set should work fine
        let msg_data = create_test_message(10, 5);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 5);
        assert_eq!(query_result.rows.len(), 10);
    }

    #[test]
    fn test_parse_medium_result_set() {
        // Medium result set: 1000 rows, 20 columns
        let msg_data = create_test_message(1000, 20);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 20);
        assert_eq!(query_result.rows.len(), 1000);
    }

    #[test]
    fn test_parse_large_result_set() {
        // Large result set: 10,000 rows, 30 columns
        // This should start showing verification issues
        let msg_data = create_test_message(10_000, 30);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 30);
        assert_eq!(query_result.rows.len(), 10_000);
    }

    #[test]
    fn test_parse_very_large_result_set() {
        // Very large result set: 33,000 rows, 40 columns
        let msg_data = create_test_message(33_000, 40);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 40);
        assert_eq!(query_result.rows.len(), 33_000);
    }

    #[test]
    fn test_parse_huge_result_set() {
        // Huge result set: 50,000 rows, 100 columns
        let msg_data = create_test_message(50_000, 100);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 100);
        assert_eq!(query_result.rows.len(), 50_000);
    }

    #[test]
    fn test_compare_with_unchecked_parse() {
        // Test to demonstrate that unchecked parsing would work
        let msg_data = create_test_message(33_000, 40);

        // Checked version (current implementation)
        let checked_result = QueryResult::from_cubestore_fb(&msg_data);

        // Try unchecked version to verify the data itself is valid
        let unchecked_result = unsafe {
            let http_message = root_as_http_message_unchecked(&msg_data);
            match http_message.command_type() {
                HttpCommand::HttpResultSet => {
                    let result_set = http_message.command_as_http_result_set();
                    if let Some(rs) = result_set {
                        if let Some(rows) = rs.rows() {
                            println!("Unchecked parse found {} rows", rows.len());
                            Ok(rows.len())
                        } else {
                            Err("No rows")
                        }
                    } else {
                        Err("No result set")
                    }
                }
                _ => Err("Wrong command type"),
            }
        };

        assert!(checked_result.is_ok());
        assert!(unchecked_result.is_ok());
    }

    #[test]
    fn test_parse_with_custom_verifier_options() {
        use cubeshared::codegen::root_as_http_message_with_opts;
        use cubeshared::flatbuffers::VerifierOptions;

        // Test that custom verifier options can handle large datasets
        let msg_data = create_test_message(33_000, 40);

        // Create custom verifier options with increased limits
        let opts = VerifierOptions {
            max_tables: 10_000_000,     // Support up to 10M tables
            max_apparent_size: 1 << 31, // 2GB limit
            ..Default::default()
        };

        // This should succeed with custom options
        let result = root_as_http_message_with_opts(&opts, &msg_data);

        match result {
            Ok(http_message) => match http_message.command_type() {
                HttpCommand::HttpResultSet => {
                    let result_set = http_message.command_as_http_result_set();
                    if let Some(rs) = result_set {
                        if let Some(rows) = rs.rows() {
                            assert_eq!(rows.len(), 33_000);
                        }
                    }
                }
                _ => panic!("Wrong command type"),
            },
            Err(e) => {
                panic!("Failed to parse with custom verifier options: {:?}", e);
            }
        }
    }
}
