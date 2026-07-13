use crate::{
    arrow_column::ArrowColumn,
    query_result_transform::{ColumnarArray, DBResponsePrimitive},
    transport::JsRawColumnarData,
};
use arrow::array::ArrayRef;
use arrow::ipc::reader::StreamReader;
use cubeshared::codegen::{
    root_as_http_message_with_opts, HttpCommand, HttpQueryResultData, HttpResultSet,
};
use cubeshared::flatbuffers::VerifierOptions;
use indexmap::IndexMap;
use neon::prelude::Finalize;
use std::io::Cursor;

#[derive(Debug)]
pub enum ParseError {
    UnsupportedCommand,
    EmptyResultSet,
    NullRow,
    ColumnNameNotDefined,
    ColumnIndexOutOfRange {
        idx: usize,
        data_len: usize,
    },
    InconsistentColumnLength {
        idx: usize,
        name: Option<String>,
        col_len: usize,
        expected: usize,
    },
    MembersColumnsMismatch {
        members_len: usize,
        data_len: usize,
    },
    FlatBufferError(String),
    ArrowError(String),
    UnsupportedArrowType(String),
    ErrorMessage(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnsupportedCommand => write!(f, "Unsupported command"),
            ParseError::EmptyResultSet => write!(f, "Empty resultSet"),
            ParseError::NullRow => write!(f, "Null row"),
            ParseError::ColumnNameNotDefined => write!(f, "Column name is not defined"),
            ParseError::ColumnIndexOutOfRange { idx, data_len } => write!(
                f,
                "QueryResult.data missing column at index {} (data.len() = {})",
                idx, data_len
            ),
            ParseError::InconsistentColumnLength {
                idx,
                name,
                col_len,
                expected,
            } => {
                write!(f, "QueryResult.data column")?;

                if let Some(n) = name {
                    write!(f, " {:?}", n)?;
                }

                write!(
                    f,
                    " at index {} has {} rows, expected {}",
                    idx, col_len, expected
                )
            }
            ParseError::MembersColumnsMismatch {
                members_len,
                data_len,
            } => write!(
                f,
                "QueryResult has {} members but {} data columns",
                members_len, data_len
            ),
            ParseError::FlatBufferError(msg) => write!(f, "FlatBuffer parsing error: {}", msg),
            ParseError::ArrowError(msg) => write!(f, "Arrow parsing error: {}", msg),
            ParseError::UnsupportedArrowType(ty) => {
                write!(f, "Unsupported Arrow data type: {}", ty)
            }
            ParseError::ErrorMessage(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub(crate) members: Vec<String>,
    pub(crate) columns_pos: IndexMap<String, usize>,
    pub(crate) row_count: usize,
    pub(crate) data: Vec<ColumnarArray>,
}

impl Finalize for QueryResult {}

impl QueryResult {
    pub fn empty() -> Self {
        QueryResult {
            members: vec![],
            columns_pos: IndexMap::new(),
            row_count: 0,
            data: vec![],
        }
    }

    pub fn try_new(members: Vec<String>, data: Vec<ColumnarArray>) -> Result<Self, ParseError> {
        if members.len() != data.len() {
            return Err(ParseError::MembersColumnsMismatch {
                members_len: members.len(),
                data_len: data.len(),
            });
        }

        let row_count = data.first().map(|c| c.len()).unwrap_or(0);

        for (idx, col) in data.iter().enumerate() {
            if col.len() != row_count {
                return Err(ParseError::InconsistentColumnLength {
                    idx,
                    name: members.get(idx).cloned(),
                    col_len: col.len(),
                    expected: row_count,
                });
            }
        }

        let columns_pos: IndexMap<String, usize> = members
            .iter()
            .enumerate()
            .map(|(index, member)| (member.clone(), index))
            .collect();

        Ok(QueryResult {
            members,
            columns_pos,
            row_count,
            data,
        })
    }

    #[inline]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    #[inline]
    pub fn members(&self) -> &[String] {
        &self.members
    }

    #[inline]
    pub fn column(&self, idx: usize) -> Result<&ColumnarArray, ParseError> {
        self.data.get(idx).ok_or(ParseError::ColumnIndexOutOfRange {
            idx,
            data_len: self.data.len(),
        })
    }

    pub fn from_js_raw_data(js_raw_data: JsRawColumnarData) -> Result<Self, ParseError> {
        let JsRawColumnarData { members, columns } = js_raw_data;
        QueryResult::try_new(members, columns)
    }

    pub fn from_cubestore_fb(msg_data: &[u8]) -> Result<Self, ParseError> {
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

                Self::parse_legacy(result_set)
            }
            HttpCommand::HttpQueryResult => {
                let query_result = http_message
                    .command_as_http_query_result()
                    .ok_or(ParseError::EmptyResultSet)?;

                match query_result.data_type() {
                    HttpQueryResultData::HttpQueryResultArrow => {
                        let arrow =
                            query_result
                                .data_as_http_query_result_arrow()
                                .ok_or_else(|| {
                                    ParseError::FlatBufferError(
                                        "HttpQueryResult.data is not HttpQueryResultArrow"
                                            .to_string(),
                                    )
                                })?;

                        Self::from_arrow(arrow.data().bytes())
                    }
                    // Marker for statements that complete without a result set
                    // (CREATE TABLE/INSERT, queue/cache writes). Carries no payload,
                    // so it maps to an empty result, like a zero-column legacy result set.
                    HttpQueryResultData::HttpQueryResultCompleted => Ok(QueryResult::empty()),
                    other => Err(ParseError::FlatBufferError(format!(
                        "Unsupported HttpQueryResult.data type: {:?}",
                        other
                    ))),
                }
            }
            _ => Err(ParseError::UnsupportedCommand),
        }
    }

    fn parse_legacy(result_set: HttpResultSet<'_>) -> Result<Self, ParseError> {
        let members: Vec<String> = match result_set.columns() {
            Some(result_set_columns) => {
                if result_set_columns.iter().any(|c| c.is_empty()) {
                    return Err(ParseError::ColumnNameNotDefined);
                }
                result_set_columns.iter().map(|c| c.to_owned()).collect()
            }
            None => Vec::new(),
        };

        let n_cols = members.len();
        let data: Vec<Vec<DBResponsePrimitive>> = if let Some(result_set_rows) = result_set.rows() {
            let row_count = result_set_rows.len();
            let mut data: Vec<Vec<DBResponsePrimitive>> =
                (0..n_cols).map(|_| Vec::with_capacity(row_count)).collect();

            for row in result_set_rows.iter() {
                let values = row.values().ok_or(ParseError::NullRow)?;
                for (col_idx, val) in values.iter().enumerate() {
                    if col_idx >= n_cols {
                        break;
                    }
                    let cell = match val.string_value() {
                        Some(s) => DBResponsePrimitive::String(s.to_owned()),
                        None => DBResponsePrimitive::Null,
                    };
                    data[col_idx].push(cell);
                }

                // Pad short rows with Null to keep all columns aligned.
                for col in data.iter_mut().take(n_cols).skip(values.len()) {
                    col.push(DBResponsePrimitive::Null);
                }
            }

            data
        } else {
            (0..n_cols).map(|_| Vec::new()).collect()
        };

        QueryResult::try_new(members, data.into_iter().map(ColumnarArray::from).collect())
    }

    /// Wraps the Arrow arrays as-is (one chunk per record batch) instead of
    /// materializing per-cell primitives: values are read straight from the
    /// Arrow buffers when the result is transformed/serialized. Column data
    /// types are still validated eagerly, so unsupported types fail here, at
    /// parse time.
    pub(crate) fn from_arrow(bytes: &[u8]) -> Result<Self, ParseError> {
        let reader = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|err| ParseError::ArrowError(err.to_string()))?;

        let schema = reader.schema();
        let members: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let n_cols = members.len();

        let mut columns: Vec<Vec<ArrayRef>> = (0..n_cols).map(|_| Vec::new()).collect();

        for batch in reader {
            let batch = batch.map_err(|err| ParseError::ArrowError(err.to_string()))?;
            for (idx, chunks) in columns.iter_mut().enumerate() {
                chunks.push(batch.column(idx).clone());
            }
        }

        let data: Vec<ColumnarArray> = columns
            .into_iter()
            .map(|chunks| ArrowColumn::try_new(chunks).map(ColumnarArray::Arrow))
            .collect::<Result<_, _>>()?;
        QueryResult::try_new(members, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, Decimal128Array, Decimal256Array, Float64Array, StringArray,
        TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use cubeshared::codegen::{
        root_as_http_message_unchecked, HttpColumnValue, HttpColumnValueArgs, HttpMessage,
        HttpMessageArgs, HttpQueryResult, HttpQueryResultArgs, HttpQueryResultArrow,
        HttpQueryResultArrowArgs, HttpQueryResultCompleted, HttpQueryResultCompletedArgs,
        HttpResultSetArgs, HttpRow, HttpRowArgs,
    };
    use cubeshared::flatbuffers::FlatBufferBuilder;
    use std::sync::Arc;

    /// Helper function to create a test HttpMessage with a given number of rows and columns
    fn create_test_message(num_rows: usize, num_columns: usize) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();

        let column_names: Vec<_> = (0..num_columns)
            .map(|i| builder.create_string(&format!("column_{}", i)))
            .collect();

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
    fn test_parse_small_result_set() -> Result<(), ParseError> {
        let msg_data = create_test_message(10, 5);
        let query_result = QueryResult::from_cubestore_fb(&msg_data)?;

        assert_eq!(query_result.members.len(), 5);
        assert_eq!(query_result.row_count, 10);
        assert_eq!(query_result.data.len(), 5);
        assert!(query_result.data.iter().all(|c| c.len() == 10));

        Ok(())
    }

    #[test]
    fn test_parse_medium_result_set() -> Result<(), ParseError> {
        // Medium result set: 1000 rows, 20 columns
        let msg_data = create_test_message(1000, 20);
        let query_result = QueryResult::from_cubestore_fb(&msg_data)?;

        assert_eq!(query_result.members.len(), 20);
        assert_eq!(query_result.row_count, 1000);
        assert_eq!(query_result.data.len(), 20);
        assert!(query_result.data.iter().all(|c| c.len() == 1000));

        Ok(())
    }

    #[test]
    fn test_parse_large_result_set() -> Result<(), ParseError> {
        // Large result set: 10,000 rows, 30 columns
        // This should start showing verification issues
        let msg_data = create_test_message(10_000, 30);
        let query_result = QueryResult::from_cubestore_fb(&msg_data)?;

        assert_eq!(query_result.members.len(), 30);
        assert_eq!(query_result.row_count, 10_000);
        assert_eq!(query_result.data.len(), 30);
        assert!(query_result.data.iter().all(|c| c.len() == 10_000));

        Ok(())
    }

    #[test]
    fn test_parse_very_large_result_set() -> Result<(), ParseError> {
        // Very large result set: 33,000 rows, 40 columns
        let msg_data = create_test_message(33_000, 40);
        let query_result = QueryResult::from_cubestore_fb(&msg_data)?;

        assert_eq!(query_result.members.len(), 40);
        assert_eq!(query_result.row_count, 33_000);
        assert_eq!(query_result.data.len(), 40);
        assert!(query_result.data.iter().all(|c| c.len() == 33_000));

        Ok(())
    }

    #[test]
    fn test_parse_huge_result_set() -> Result<(), ParseError> {
        // Huge result set: 50,000 rows, 100 columns
        let msg_data = create_test_message(50_000, 100);
        let query_result = QueryResult::from_cubestore_fb(&msg_data)?;

        assert_eq!(query_result.members.len(), 100);
        assert_eq!(query_result.row_count, 50_000);
        assert_eq!(query_result.data.len(), 100);
        assert!(query_result.data.iter().all(|c| c.len() == 50_000));

        Ok(())
    }

    #[test]
    fn test_compare_with_unchecked_parse() -> Result<(), ParseError> {
        // Test to demonstrate that unchecked parsing would work
        let msg_data = create_test_message(33_000, 40);

        // Checked version (current implementation)
        let checked_result = QueryResult::from_cubestore_fb(&msg_data)?;

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

        assert_eq!(checked_result.row_count, 33_000);
        assert!(unchecked_result.is_ok());

        Ok(())
    }

    fn arrow_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        {
            let schema = batch.schema();
            let mut writer = StreamWriter::try_new(&mut buf, schema.as_ref()).unwrap();
            writer.write(batch).unwrap();
            writer.finish().unwrap();
        }
        buf
    }

    #[test]
    fn test_from_arrow_basic_types() -> Result<(), ParseError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ]));

        let cities = StringArray::from(vec![Some("Berlin"), None, Some("Lisbon")]);
        let amounts = Float64Array::from(vec![Some(1.5), Some(2.0), None]);
        // 0 -> 1970-01-01T00:00:00.000, 1_000 -> 1970-01-01T00:00:01.000
        let created = TimestampMillisecondArray::from(vec![Some(0i64), None, Some(1_000)]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(cities), Arc::new(amounts), Arc::new(created)],
        )
        .unwrap();

        let bytes = arrow_ipc_bytes(&batch);
        let result = QueryResult::from_arrow(&bytes)?;

        assert_eq!(result.members, vec!["city", "amount", "created_at"]);
        assert_eq!(result.row_count, 3);
        assert_eq!(result.data.len(), 3);

        assert_eq!(
            result.data[0].to_cells().as_ref(),
            &[
                DBResponsePrimitive::String("Berlin".to_string()),
                DBResponsePrimitive::Null,
                DBResponsePrimitive::String("Lisbon".to_string()),
            ]
        );
        assert_eq!(
            result.data[1].to_cells().as_ref(),
            &[
                DBResponsePrimitive::Float64(1.5),
                DBResponsePrimitive::Float64(2.0),
                DBResponsePrimitive::Null,
            ]
        );

        // Numeric values serialize as JSON strings, matching the legacy result set.
        let amounts_json = serde_json::to_value(&result.data[1]).unwrap();
        assert_eq!(amounts_json[0], "1.5");
        assert_eq!(amounts_json[1], "2");
        assert_eq!(amounts_json[2], serde_json::Value::Null);

        // Timestamps land in the dedicated variant and serialize to the ISO format.
        match &result.data[2].to_cells()[0] {
            DBResponsePrimitive::Timestamp(_) => {}
            other => panic!("expected Timestamp, got {other:?}"),
        }
        assert_eq!(result.data[2].to_cells()[1], DBResponsePrimitive::Null);
        let json = serde_json::to_value(&result.data[2]).unwrap();
        assert_eq!(json[0], "1970-01-01T00:00:00.000");
        assert_eq!(json[1], serde_json::Value::Null);
        assert_eq!(json[2], "1970-01-01T00:00:01.000");

        Ok(())
    }

    #[test]
    fn test_from_arrow_decimal256_beyond_i128() -> Result<(), ParseError> {
        use arrow::datatypes::i256;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "d256",
            DataType::Decimal256(76, 2),
            true,
        )]));

        // A mantissa larger than i128::MAX, formatted directly (no Arrow fallback).
        let big = i256::from_string("999999999999999999999999999999999999999").unwrap();
        let d256 = Decimal256Array::from(vec![Some(big), None])
            .with_precision_and_scale(76, 2)
            .unwrap();

        let batch = RecordBatch::try_new(schema, vec![Arc::new(d256)]).unwrap();
        let bytes = arrow_ipc_bytes(&batch);
        let result = QueryResult::from_arrow(&bytes)?;

        assert_eq!(
            result.data[0].to_cells().as_ref(),
            &[
                DBResponsePrimitive::String("9999999999999999999999999999999999999.99".to_string()),
                DBResponsePrimitive::Null,
            ]
        );

        Ok(())
    }

    #[test]
    fn test_from_arrow_decimal128_leading_null() -> Result<(), ParseError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "totalSales",
            DataType::Decimal128(38, 2),
            true,
        )]));

        let d128 = Decimal128Array::from(vec![
            None,
            Some(239996i128),
            Some(224991),
            Some(215490),
            Some(197989),
        ])
        .with_precision_and_scale(38, 2)
        .unwrap();

        let batch = RecordBatch::try_new(schema, vec![Arc::new(d128)]).unwrap();
        let bytes = arrow_ipc_bytes(&batch);
        let result = QueryResult::from_arrow(&bytes)?;

        assert_eq!(result.row_count, 5);
        let json = serde_json::to_value(&result.data[0]).unwrap();
        assert_eq!(json[0], serde_json::Value::Null);
        assert_eq!(json[1], "2399.96");
        assert_eq!(json[2], "2249.91");
        assert_eq!(json[3], "2154.9");
        assert_eq!(json[4], "1979.89");

        Ok(())
    }

    #[test]
    fn test_from_arrow_unsupported_type() -> Result<(), ParseError> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "blob",
            DataType::Binary,
            false,
        )]));
        let blobs = BinaryArray::from_vec(vec![b"a".as_ref(), b"b".as_ref()]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(blobs)]).unwrap();

        let bytes = arrow_ipc_bytes(&batch);
        let err = QueryResult::from_arrow(&bytes).expect_err("should reject Binary");
        assert!(matches!(err, ParseError::UnsupportedArrowType(_)));

        Ok(())
    }

    #[test]
    fn test_from_cubestore_fb_arrow_query_result() -> Result<(), ParseError> {
        // Arrow IPC stream payload, as CubeStore would emit it.
        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["Berlin", "Lisbon"])),
                Arc::new(Float64Array::from(vec![1.5, 2.0])),
            ],
        )
        .unwrap();
        let ipc = arrow_ipc_bytes(&batch);

        // Wrap it in a FlatBuffer HttpMessage with the HttpQueryResult command.
        let mut builder = FlatBufferBuilder::new();
        let data_vec = builder.create_vector(&ipc);
        let arrow = HttpQueryResultArrow::create(
            &mut builder,
            &HttpQueryResultArrowArgs {
                data: Some(data_vec),
                is_last: true,
            },
        );
        let query_result = HttpQueryResult::create(
            &mut builder,
            &HttpQueryResultArgs {
                data_type: HttpQueryResultData::HttpQueryResultArrow,
                data: Some(arrow.as_union_value()),
            },
        );
        let connection_id = builder.create_string("test_connection");
        let message = HttpMessage::create(
            &mut builder,
            &HttpMessageArgs {
                message_id: 1,
                command_type: HttpCommand::HttpQueryResult,
                command: Some(query_result.as_union_value()),
                connection_id: Some(connection_id),
            },
        );
        builder.finish(message, None);
        let msg_data = builder.finished_data().to_vec();

        let result = QueryResult::from_cubestore_fb(&msg_data)?;
        assert_eq!(result.members, vec!["city", "amount"]);
        assert_eq!(result.row_count, 2);
        assert_eq!(
            result.data[0].to_cells().as_ref(),
            &[
                DBResponsePrimitive::String("Berlin".to_string()),
                DBResponsePrimitive::String("Lisbon".to_string()),
            ]
        );
        assert_eq!(
            result.data[1].to_cells().as_ref(),
            &[
                DBResponsePrimitive::Float64(1.5),
                DBResponsePrimitive::Float64(2.0),
            ]
        );

        Ok(())
    }

    #[test]
    fn test_from_cubestore_fb_completed_query_result() -> Result<(), ParseError> {
        let mut builder = FlatBufferBuilder::new();
        let completed =
            HttpQueryResultCompleted::create(&mut builder, &HttpQueryResultCompletedArgs {});
        let query_result = HttpQueryResult::create(
            &mut builder,
            &HttpQueryResultArgs {
                data_type: HttpQueryResultData::HttpQueryResultCompleted,
                data: Some(completed.as_union_value()),
            },
        );
        let connection_id = builder.create_string("test_connection");
        let message = HttpMessage::create(
            &mut builder,
            &HttpMessageArgs {
                message_id: 1,
                command_type: HttpCommand::HttpQueryResult,
                command: Some(query_result.as_union_value()),
                connection_id: Some(connection_id),
            },
        );
        builder.finish(message, None);
        let msg_data = builder.finished_data().to_vec();

        let result = QueryResult::from_cubestore_fb(&msg_data)?;
        assert!(result.members.is_empty());
        assert_eq!(result.row_count, 0);
        assert!(result.data.is_empty());

        Ok(())
    }

    #[test]
    fn test_parse_with_custom_verifier_options() -> Result<(), ParseError> {
        // Test that custom verifier options can handle large datasets
        let msg_data = create_test_message(33_000, 40);

        // Create custom verifier options with increased limits
        let opts = VerifierOptions {
            max_tables: 10_000_000,     // Support up to 10M tables
            max_apparent_size: 1 << 31, // 2GB limit
            ..Default::default()
        };

        // This should succeed with custom options
        let http_message = root_as_http_message_with_opts(&opts, &msg_data)
            .map_err(|err| ParseError::FlatBufferError(err.to_string()))?;

        match http_message.command_type() {
            HttpCommand::HttpResultSet => {
                let result_set = http_message.command_as_http_result_set();
                if let Some(rs) = result_set {
                    if let Some(rows) = rs.rows() {
                        assert_eq!(rows.len(), 33_000);
                    }
                }
            }
            _ => panic!("Wrong command type"),
        }

        Ok(())
    }
}
