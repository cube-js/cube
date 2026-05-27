use crate::{
    query_result_transform::{ColumnarArray, DBResponsePrimitive},
    transport::JsRawColumnarData,
};
use arrow::array::{
    Array, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array, Float16Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
    StringArray, StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::reader::StreamReader;
use cubeshared::codegen::{root_as_http_message_with_opts, HttpCommand, HttpResultSet};
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
                let arrow = query_result
                    .data_as_http_query_result_arrow()
                    .ok_or_else(|| {
                        ParseError::FlatBufferError(
                            "HttpQueryResult.data is not HttpQueryResultArrow".to_string(),
                        )
                    })?;

                Self::from_arrow(arrow.data().bytes())
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
        let data: Vec<ColumnarArray> = if let Some(result_set_rows) = result_set.rows() {
            let row_count = result_set_rows.len();
            let mut data: Vec<ColumnarArray> = (0..n_cols)
                .map(|_| ColumnarArray::with_capacity(row_count))
                .collect();

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
            (0..n_cols).map(|_| ColumnarArray::new()).collect()
        };

        QueryResult::try_new(members, data)
    }

    fn from_arrow(bytes: &[u8]) -> Result<Self, ParseError> {
        let reader = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|err| ParseError::ArrowError(err.to_string()))?;

        let schema = reader.schema();
        let members: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let n_cols = members.len();

        let mut columns: Vec<Vec<DBResponsePrimitive>> = (0..n_cols).map(|_| Vec::new()).collect();

        for batch in reader {
            let batch = batch.map_err(|err| ParseError::ArrowError(err.to_string()))?;
            for (idx, col) in columns.iter_mut().enumerate() {
                append_arrow_array(col, batch.column(idx).as_ref())?;
            }
        }

        let data: Vec<ColumnarArray> = columns.into_iter().map(ColumnarArray::from).collect();
        QueryResult::try_new(members, data)
    }
}

/// Append every element of an Arrow `array` to a column accumulator, converting
/// each value to [`DBResponsePrimitive`].
fn append_arrow_array(
    col: &mut Vec<DBResponsePrimitive>,
    array: &dyn Array,
) -> Result<(), ParseError> {
    let len = array.len();
    col.reserve(len);

    macro_rules! downcast_array_ref {
        ($ty:ty) => {
            array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                ParseError::ArrowError(format!(
                    "Failed to downcast Arrow array to {}",
                    stringify!($ty)
                ))
            })?
        };
    }

    macro_rules! push_int {
        ($ty:ty) => {{
            let a = downcast_array_ref!($ty);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    col.push(DBResponsePrimitive::Int64(a.value(i) as i64));
                }
            }
        }};
    }

    macro_rules! push_uint {
        ($ty:ty) => {{
            let a = downcast_array_ref!($ty);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    col.push(DBResponsePrimitive::UInt64(a.value(i) as u64));
                }
            }
        }};
    }

    macro_rules! push_float {
        ($ty:ty) => {{
            let a = downcast_array_ref!($ty);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    col.push(DBResponsePrimitive::Float64(a.value(i) as f64));
                }
            }
        }};
    }

    macro_rules! push_str {
        ($ty:ty) => {{
            let a = downcast_array_ref!($ty);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    col.push(DBResponsePrimitive::String(a.value(i).to_owned()));
                }
            }
        }};
    }

    macro_rules! push_datetime {
        ($ty:ty) => {{
            let a = downcast_array_ref!($ty);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    match a.value_as_datetime(i) {
                        Some(dt) => col.push(DBResponsePrimitive::Timestamp(dt)),
                        None => col.push(DBResponsePrimitive::Null),
                    }
                }
            }
        }};
    }

    macro_rules! push_decimal {
        ($ty:ty) => {{
            let a = downcast_array_ref!($ty);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    let s = a.value_as_string(i);
                    match s.parse::<f64>() {
                        Ok(n) => col.push(DBResponsePrimitive::Float64(n)),
                        Err(_) => col.push(DBResponsePrimitive::String(s)),
                    }
                }
            }
        }};
    }

    match array.data_type() {
        DataType::Null => {
            for _ in 0..len {
                col.push(DBResponsePrimitive::Null);
            }
        }
        DataType::Boolean => {
            let a = downcast_array_ref!(BooleanArray);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    col.push(DBResponsePrimitive::Boolean(a.value(i)));
                }
            }
        }
        DataType::Int8 => push_int!(Int8Array),
        DataType::Int16 => push_int!(Int16Array),
        DataType::Int32 => push_int!(Int32Array),
        DataType::Int64 => push_int!(Int64Array),
        DataType::UInt8 => push_uint!(UInt8Array),
        DataType::UInt16 => push_uint!(UInt16Array),
        DataType::UInt32 => push_uint!(UInt32Array),
        DataType::UInt64 => push_uint!(UInt64Array),
        DataType::Float32 => push_float!(Float32Array),
        DataType::Float64 => push_float!(Float64Array),
        DataType::Float16 => {
            let a = downcast_array_ref!(Float16Array);
            for i in 0..len {
                if a.is_null(i) {
                    col.push(DBResponsePrimitive::Null);
                } else {
                    col.push(DBResponsePrimitive::Float64(a.value(i).to_f64()));
                }
            }
        }
        DataType::Utf8 => push_str!(StringArray),
        DataType::LargeUtf8 => push_str!(LargeStringArray),
        DataType::Utf8View => push_str!(StringViewArray),
        DataType::Date32 => push_datetime!(Date32Array),
        DataType::Date64 => push_datetime!(Date64Array),
        DataType::Timestamp(TimeUnit::Second, _) => push_datetime!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => push_datetime!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => push_datetime!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => push_datetime!(TimestampNanosecondArray),
        DataType::Decimal128(_, _) => push_decimal!(Decimal128Array),
        DataType::Decimal256(_, _) => push_decimal!(Decimal256Array),
        other => return Err(ParseError::UnsupportedArrowType(format!("{other:?}"))),
    }

    Ok(())
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
    fn test_parse_small_result_set() {
        let msg_data = create_test_message(10, 5);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 5);
        assert_eq!(query_result.row_count, 10);
        assert_eq!(query_result.data.len(), 5);
        assert!(query_result.data.iter().all(|c| c.len() == 10));
    }

    #[test]
    fn test_parse_medium_result_set() {
        // Medium result set: 1000 rows, 20 columns
        let msg_data = create_test_message(1000, 20);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 20);
        assert_eq!(query_result.row_count, 1000);
        assert_eq!(query_result.data.len(), 20);
        assert!(query_result.data.iter().all(|c| c.len() == 1000));
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
        assert_eq!(query_result.row_count, 10_000);
        assert_eq!(query_result.data.len(), 30);
        assert!(query_result.data.iter().all(|c| c.len() == 10_000));
    }

    #[test]
    fn test_parse_very_large_result_set() {
        // Very large result set: 33,000 rows, 40 columns
        let msg_data = create_test_message(33_000, 40);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 40);
        assert_eq!(query_result.row_count, 33_000);
        assert_eq!(query_result.data.len(), 40);
        assert!(query_result.data.iter().all(|c| c.len() == 33_000));
    }

    #[test]
    fn test_parse_huge_result_set() {
        // Huge result set: 50,000 rows, 100 columns
        let msg_data = create_test_message(50_000, 100);
        let result = QueryResult::from_cubestore_fb(&msg_data);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.members.len(), 100);
        assert_eq!(query_result.row_count, 50_000);
        assert_eq!(query_result.data.len(), 100);
        assert!(query_result.data.iter().all(|c| c.len() == 50_000));
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

    fn arrow_ipc_bytes(batch: &arrow::record_batch::RecordBatch) -> Vec<u8> {
        use arrow::ipc::writer::StreamWriter;
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
    fn test_from_arrow_basic_types() {
        use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

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
        let result = QueryResult::from_arrow(&bytes).expect("from_arrow");

        assert_eq!(result.members, vec!["city", "amount", "created_at"]);
        assert_eq!(result.row_count, 3);
        assert_eq!(result.data.len(), 3);

        assert_eq!(
            result.data[0].as_slice(),
            &[
                DBResponsePrimitive::String("Berlin".to_string()),
                DBResponsePrimitive::Null,
                DBResponsePrimitive::String("Lisbon".to_string()),
            ]
        );
        assert_eq!(
            result.data[1].as_slice(),
            &[
                DBResponsePrimitive::Float64(1.5),
                DBResponsePrimitive::Float64(2.0),
                DBResponsePrimitive::Null,
            ]
        );

        // Timestamps land in the dedicated variant and serialize to the ISO format.
        match &result.data[2].as_slice()[0] {
            DBResponsePrimitive::Timestamp(_) => {}
            other => panic!("expected Timestamp, got {other:?}"),
        }
        assert_eq!(result.data[2].as_slice()[1], DBResponsePrimitive::Null);
        let json = serde_json::to_value(result.data[2].as_slice()).unwrap();
        assert_eq!(json[0], "1970-01-01T00:00:00.000");
        assert_eq!(json[1], serde_json::Value::Null);
        assert_eq!(json[2], "1970-01-01T00:00:01.000");
    }

    #[test]
    fn test_from_arrow_unsupported_type() {
        use arrow::array::BinaryArray;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

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
    }

    #[test]
    fn test_from_cubestore_fb_arrow_query_result() {
        use arrow::array::{Float64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use cubeshared::codegen::{
            HttpMessage, HttpMessageArgs, HttpQueryResult, HttpQueryResultArgs,
            HttpQueryResultArrow, HttpQueryResultArrowArgs, HttpQueryResultData,
        };
        use cubeshared::flatbuffers::FlatBufferBuilder;
        use std::sync::Arc;

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

        let result = QueryResult::from_cubestore_fb(&msg_data).expect("from_cubestore_fb arrow");
        assert_eq!(result.members, vec!["city", "amount"]);
        assert_eq!(result.row_count, 2);
        assert_eq!(
            result.data[0].as_slice(),
            &[
                DBResponsePrimitive::String("Berlin".to_string()),
                DBResponsePrimitive::String("Lisbon".to_string()),
            ]
        );
        assert_eq!(
            result.data[1].as_slice(),
            &[
                DBResponsePrimitive::Float64(1.5),
                DBResponsePrimitive::Float64(2.0),
            ]
        );
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
