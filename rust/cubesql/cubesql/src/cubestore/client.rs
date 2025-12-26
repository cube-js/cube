use datafusion::arrow::{array::*, datatypes::*, record_batch::RecordBatch};
use flatbuffers::FlatBufferBuilder;
use futures_util::{SinkExt, StreamExt};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::CubeError;
use cubeshared::codegen::*;

#[derive(Debug)]
pub struct CubeStoreClient {
    url: String,
    connection_id: String,
    message_counter: AtomicU32,
}

impl CubeStoreClient {
    pub fn new(url: String) -> Self {
        Self {
            url,
            connection_id: uuid::Uuid::new_v4().to_string(),
            message_counter: AtomicU32::new(1),
        }
    }

    pub async fn query(&self, sql: String) -> Result<Vec<RecordBatch>, CubeError> {
        // Connect to WebSocket
        let (ws_stream, _) = connect_async(&self.url)
            .await
            .map_err(|e| CubeError::internal(format!("WebSocket connection failed: {}", e)))?;

        let (mut write, mut read) = ws_stream.split();

        // Build and send FlatBuffers query message
        let msg_bytes = self.build_query_message(&sql);
        write
            .send(Message::Binary(msg_bytes))
            .await
            .map_err(|e| CubeError::internal(format!("Failed to send query: {}", e)))?;

        // Receive response with timeout
        let timeout_duration = Duration::from_secs(30);

        tokio::select! {
            msg_result = read.next() => {
                match msg_result {
                    Some(Ok(msg)) => {
                        let data = msg.into_data();
                        let http_msg = root_as_http_message(&data)
                            .map_err(|e| CubeError::internal(format!("Failed to parse FlatBuffers message: {}", e)))?;

                        match http_msg.command_type() {
                            HttpCommand::HttpResultSet => {
                                let result_set = http_msg
                                    .command_as_http_result_set()
                                    .ok_or_else(|| CubeError::internal("Invalid result set".to_string()))?;

                                self.flatbuffers_to_arrow(result_set)
                            }
                            HttpCommand::HttpError => {
                                let error = http_msg
                                    .command_as_http_error()
                                    .ok_or_else(|| CubeError::internal("Invalid error message".to_string()))?;

                                Err(CubeError::user(
                                    error.error().unwrap_or("Unknown error").to_string()
                                ))
                            }
                            _ => Err(CubeError::internal(format!("Unexpected command type: {:?}", http_msg.command_type()))),
                        }
                    }
                    Some(Err(e)) => Err(CubeError::internal(format!("WebSocket error: {}", e))),
                    None => Err(CubeError::internal("Connection closed unexpectedly".to_string())),
                }
            }
            _ = tokio::time::sleep(timeout_duration) => {
                Err(CubeError::internal("Query timeout".to_string()))
            }
        }
    }

    fn build_query_message(&self, sql: &str) -> Vec<u8> {
        let mut builder = FlatBufferBuilder::new();

        // Build query string
        let query_str = builder.create_string(sql);
        let conn_id_str = builder.create_string(&self.connection_id);

        // Build HttpQuery
        let query_args = HttpQueryArgs {
            query: Some(query_str),
            trace_obj: None,
            inline_tables: None,
        };
        let query_obj = HttpQuery::create(&mut builder, &query_args);

        // Build HttpMessage wrapper
        let msg_id = self.message_counter.fetch_add(1, Ordering::SeqCst);
        let message_args = HttpMessageArgs {
            message_id: msg_id,
            command_type: HttpCommand::HttpQuery,
            command: Some(query_obj.as_union_value()),
            connection_id: Some(conn_id_str),
        };
        let message = HttpMessage::create(&mut builder, &message_args);

        builder.finish(message, None);
        builder.finished_data().to_vec()
    }

    fn flatbuffers_to_arrow(
        &self,
        result_set: HttpResultSet,
    ) -> Result<Vec<RecordBatch>, CubeError> {
        let columns = result_set
            .columns()
            .ok_or_else(|| CubeError::internal("Missing columns in result set".to_string()))?;

        let rows = result_set
            .rows()
            .ok_or_else(|| CubeError::internal("Missing rows in result set".to_string()))?;

        // Handle empty result set
        if rows.len() == 0 {
            let fields: Vec<Field> = columns
                .iter()
                .map(|col| Field::new(col, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let empty_batch = RecordBatch::new_empty(schema);
            return Ok(vec![empty_batch]);
        }

        // Infer schema from data
        let fields: Vec<Field> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let dtype = self.infer_arrow_type(&rows, idx);
                Field::new(col, dtype, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Build columnar arrays
        let arrays = self.build_columnar_arrays(&schema, &rows)?;

        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| CubeError::internal(format!("Failed to create RecordBatch: {}", e)))?;

        Ok(vec![batch])
    }

    fn infer_arrow_type(
        &self,
        rows: &flatbuffers::Vector<flatbuffers::ForwardsUOffset<HttpRow>>,
        col_idx: usize,
    ) -> DataType {
        // Sample first non-null value to infer type
        // CubeStore returns all values as strings in FlatBuffers
        for row in rows {
            if let Some(values) = row.values() {
                if col_idx < values.len() {
                    let value = values.get(col_idx);
                    if let Some(s) = value.string_value() {
                        // Try parsing as different types
                        if s.parse::<i64>().is_ok() {
                            return DataType::Int64;
                        } else if s.parse::<f64>().is_ok() {
                            return DataType::Float64;
                        } else if s == "true" || s == "false" {
                            return DataType::Boolean;
                        }
                        // Default to string
                        return DataType::Utf8;
                    }
                }
            }
        }

        DataType::Utf8 // Default
    }

    fn build_columnar_arrays(
        &self,
        schema: &SchemaRef,
        rows: &flatbuffers::Vector<flatbuffers::ForwardsUOffset<HttpRow>>,
    ) -> Result<Vec<ArrayRef>, CubeError> {
        let mut arrays = Vec::new();
        let row_count = rows.len();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array: ArrayRef = match field.data_type() {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new(row_count);
                    for row in rows {
                        if let Some(values) = row.values() {
                            if col_idx < values.len() {
                                let value = values.get(col_idx);
                                match value.string_value() {
                                    Some(s) => builder.append_value(s)?,
                                    None => builder.append_null()?,
                                }
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_null()?;
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new(row_count);
                    for row in rows {
                        if let Some(values) = row.values() {
                            if col_idx < values.len() {
                                let value = values.get(col_idx);
                                match value.string_value() {
                                    Some(s) => match s.parse::<i64>() {
                                        Ok(n) => builder.append_value(n)?,
                                        Err(_) => builder.append_null()?,
                                    },
                                    None => builder.append_null()?,
                                }
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_null()?;
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new(row_count);
                    for row in rows {
                        if let Some(values) = row.values() {
                            if col_idx < values.len() {
                                let value = values.get(col_idx);
                                match value.string_value() {
                                    Some(s) => match s.parse::<f64>() {
                                        Ok(n) => builder.append_value(n)?,
                                        Err(_) => builder.append_null()?,
                                    },
                                    None => builder.append_null()?,
                                }
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_null()?;
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::new(row_count);
                    for row in rows {
                        if let Some(values) = row.values() {
                            if col_idx < values.len() {
                                let value = values.get(col_idx);
                                match value.string_value() {
                                    Some(s) => match s.to_lowercase().as_str() {
                                        "true" | "t" | "1" => builder.append_value(true)?,
                                        "false" | "f" | "0" => builder.append_value(false)?,
                                        _ => builder.append_null()?,
                                    },
                                    None => builder.append_null()?,
                                }
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_null()?;
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    // Fallback: treat as string
                    let mut builder = StringBuilder::new(row_count);
                    for row in rows {
                        if let Some(values) = row.values() {
                            if col_idx < values.len() {
                                let value = values.get(col_idx);
                                match value.string_value() {
                                    Some(s) => builder.append_value(s)?,
                                    None => builder.append_null()?,
                                }
                            } else {
                                builder.append_null()?;
                            }
                        } else {
                            builder.append_null()?;
                        }
                    }
                    Arc::new(builder.finish())
                }
            };

            arrays.push(array);
        }

        Ok(arrays)
    }
}
