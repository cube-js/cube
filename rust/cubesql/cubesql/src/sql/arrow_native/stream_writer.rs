use crate::sql::arrow_ipc::ArrowIPCSerializer;
use crate::CubeError;
use datafusion::arrow::ipc::writer::StreamWriter as ArrowStreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use super::protocol::{write_message, Message};

pub struct StreamWriter;

impl StreamWriter {
    /// Write schema message from the stream
    pub async fn write_schema<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        stream: &mut SendableRecordBatchStream,
    ) -> Result<(), CubeError> {
        // Serialize schema to Arrow IPC format
        let schema = stream.schema();
        let arrow_ipc_schema = Self::serialize_schema(&schema)?;

        // Send schema message
        let msg = Message::QueryResponseSchema { arrow_ipc_schema };
        write_message(writer, &msg).await?;

        Ok(())
    }

    /// Stream all batches from SendableRecordBatchStream directly to the writer
    pub async fn stream_batches<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        stream: &mut SendableRecordBatchStream,
    ) -> Result<i64, CubeError> {
        let mut total_rows = 0i64;
        let mut batch_count = 0;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| {
                CubeError::internal(format!("Error reading batch from stream: {}", e))
            })?;

            batch_count += 1;
            let batch_rows = batch.num_rows() as i64;
            total_rows += batch_rows;

            log::info!(
                "📦 Arrow Flight batch #{}: {} rows, {} columns (total so far: {} rows)",
                batch_count,
                batch_rows,
                batch.num_columns(),
                total_rows
            );

            // Serialize batch to Arrow IPC format
            let arrow_ipc_batch = Self::serialize_batch(&batch)?;

            log::info!(
                "📨 Serialized to {} bytes of Arrow IPC data",
                arrow_ipc_batch.len()
            );

            // Send batch message
            let msg = Message::QueryResponseBatch { arrow_ipc_batch };
            write_message(writer, &msg).await?;
        }

        log::info!(
            "✅ Arrow Flight streamed {} batches with {} total rows",
            batch_count,
            total_rows
        );

        Ok(total_rows)
    }

    /// Write complete message indicating end of query results
    pub async fn write_complete<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        rows_affected: i64,
    ) -> Result<(), CubeError> {
        let msg = Message::QueryComplete { rows_affected };
        write_message(writer, &msg).await?;
        Ok(())
    }

    /// Complete flow: stream schema, batches, and completion
    pub async fn stream_query_results<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        mut stream: SendableRecordBatchStream,
    ) -> Result<(), CubeError> {
        // Write schema
        Self::write_schema(writer, &mut stream).await?;

        // Stream all batches
        let rows_affected = Self::stream_batches(writer, &mut stream).await?;

        // Write completion
        Self::write_complete(writer, rows_affected).await?;

        Ok(())
    }

    /// Stream cached batches (already materialized)
    ///
    /// # Arguments
    /// * `writer` - Output stream
    /// * `batches` - Record batches to stream
    /// * `from_cache` - True if serving from cache, false if serving fresh query results
    pub async fn stream_cached_batches<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        batches: &[RecordBatch],
        from_cache: bool,
    ) -> Result<(), CubeError> {
        if batches.is_empty() {
            return Err(CubeError::internal(
                "Cannot stream empty batch list".to_string(),
            ));
        }

        // Get schema from first batch
        let schema = batches[0].schema();
        let arrow_ipc_schema = Self::serialize_schema(&schema)?;

        // Send schema message
        let msg = Message::QueryResponseSchema { arrow_ipc_schema };
        write_message(writer, &msg).await?;

        // Stream all batches
        let mut total_rows = 0i64;
        for (idx, batch) in batches.iter().enumerate() {
            let batch_rows = batch.num_rows() as i64;
            total_rows += batch_rows;

            if from_cache {
                log::debug!(
                    "📦 Cached batch #{}: {} rows, {} columns (total so far: {} rows)",
                    idx + 1,
                    batch_rows,
                    batch.num_columns(),
                    total_rows
                );
            } else {
                log::debug!(
                    "📦 Serving batch #{} from CubeStore: {} rows, {} columns (total so far: {} rows)",
                    idx + 1,
                    batch_rows,
                    batch.num_columns(),
                    total_rows
                );
            }

            // Serialize batch to Arrow IPC format
            let arrow_ipc_batch = Self::serialize_batch(batch)?;

            // Send batch message
            let msg = Message::QueryResponseBatch { arrow_ipc_batch };
            write_message(writer, &msg).await?;
        }

        if from_cache {
            log::info!(
                "✅ Streamed {} cached batches with {} total rows",
                batches.len(),
                total_rows
            );
        } else {
            log::info!(
                "✅ Served {} batches from CubeStore with {} total rows",
                batches.len(),
                total_rows
            );
        }

        // Write completion
        Self::write_complete(writer, total_rows).await?;

        Ok(())
    }

    /// Serialize Arrow schema to IPC format
    fn serialize_schema(
        schema: &Arc<datafusion::arrow::datatypes::Schema>,
    ) -> Result<Vec<u8>, CubeError> {
        use datafusion::arrow::ipc::writer::IpcWriteOptions;
        use std::io::Cursor;

        let mut cursor = Cursor::new(Vec::new());
        let options = IpcWriteOptions::default();

        // Write schema message
        let mut writer =
            ArrowStreamWriter::try_new_with_options(&mut cursor, schema.as_ref(), options)
                .map_err(|e| CubeError::internal(format!("Failed to create IPC writer: {}", e)))?;

        writer
            .finish()
            .map_err(|e| CubeError::internal(format!("Failed to finish schema write: {}", e)))?;

        drop(writer);

        Ok(cursor.into_inner())
    }

    /// Serialize RecordBatch to Arrow IPC format
    fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>, CubeError> {
        // Use existing ArrowIPCSerializer for single batch
        ArrowIPCSerializer::serialize_single(batch)
    }

    /// Send error message
    pub async fn write_error<W: AsyncWriteExt + Unpin>(
        writer: &mut W,
        code: String,
        message: String,
    ) -> Result<(), CubeError> {
        let msg = Message::Error { code, message };
        write_message(writer, &msg).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_serialize_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let result = StreamWriter::serialize_schema(&schema);
        assert!(result.is_ok());

        let ipc_data = result.unwrap();
        assert!(!ipc_data.is_empty());
    }

    #[tokio::test]
    async fn test_serialize_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let result = StreamWriter::serialize_batch(&batch);
        assert!(result.is_ok());

        let ipc_data = result.unwrap();
        assert!(!ipc_data.is_empty());
    }

    #[tokio::test]
    async fn test_write_error() {
        let mut buf = Vec::new();
        let result = StreamWriter::write_error(
            &mut buf,
            "TEST_ERROR".to_string(),
            "Test error message".to_string(),
        )
        .await;

        assert!(result.is_ok());
        assert!(!buf.is_empty());
    }
}
