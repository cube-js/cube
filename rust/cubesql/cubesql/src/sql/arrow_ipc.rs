//! Arrow IPC Serializer for Cube.js query results
//!
//! This module provides serialization of Arrow RecordBatch to Arrow IPC Streaming Format,
//! allowing clients to receive query results in Arrow's native columnar format.
//!
//! Arrow IPC Streaming Format (RFC 0017) is a standard format for interprocess communication
//! with zero-copy capability, making it suitable for streaming large datasets.

use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::Cursor;

use crate::error::CubeError;

/// ArrowIPCSerializer handles serialization of RecordBatch to Arrow IPC format
///
/// Arrow IPC Streaming Format structure:
/// ```text
/// [Message Header]
///   - Magic Number (4 bytes): 0xFFFFFFFF
///   - Message Type (1 byte): SCHEMA or RECORD_BATCH
///   - Message Length (4 bytes)
/// [Message Body - FlatBuffer]
///   - Schema Definition (first message)
///   - RecordBatch Metadata (per batch)
/// [Data Buffers]
///   - Validity Bitmap (nullable columns)
///   - Data Buffers (column data)
///   - Optional Offsets (variable length)
/// ```
pub struct ArrowIPCSerializer;

impl ArrowIPCSerializer {
    /// Serialize a single RecordBatch to Arrow IPC Streaming Format bytes
    ///
    /// # Arguments
    /// * `batch` - The RecordBatch to serialize
    ///
    /// # Returns
    /// * `Result<Vec<u8>>` - Serialized Arrow IPC bytes or error
    ///
    /// # Example
    /// ```ignore
    /// let batch = /* RecordBatch from query result */;
    /// let ipc_bytes = ArrowIPCSerializer::serialize_single(&batch)?;
    /// socket.write_all(&ipc_bytes).await?;
    /// ```
    pub fn serialize_single(batch: &RecordBatch) -> Result<Vec<u8>, CubeError> {
        let schema = batch.schema();
        let output = Vec::new();
        let mut cursor = Cursor::new(output);

        {
            let mut writer = StreamWriter::try_new(&mut cursor, &schema).map_err(|e| {
                CubeError::internal(format!("Failed to create Arrow IPC writer: {}", e))
            })?;

            writer.write(batch).map_err(|e| {
                CubeError::internal(format!("Failed to write Arrow IPC record batch: {}", e))
            })?;

            writer.finish().map_err(|e| {
                CubeError::internal(format!("Failed to finish Arrow IPC writer: {}", e))
            })?;
        }

        Ok(cursor.into_inner())
    }

    /// Serialize multiple RecordBatches to Arrow IPC Streaming Format bytes
    ///
    /// All batches must have the same schema. The schema is written once,
    /// followed by all record batches.
    ///
    /// # Arguments
    /// * `batches` - Slice of RecordBatches to serialize (must be non-empty)
    ///
    /// # Returns
    /// * `Result<Vec<u8>>` - Serialized Arrow IPC bytes or error
    ///
    /// # Example
    /// ```ignore
    /// let batches = vec![batch1, batch2, batch3];
    /// let ipc_bytes = ArrowIPCSerializer::serialize_streaming(&batches)?;
    /// socket.write_all(&ipc_bytes).await?;
    /// ```
    pub fn serialize_streaming(batches: &[RecordBatch]) -> Result<Vec<u8>, CubeError> {
        if batches.is_empty() {
            // Empty result set - return empty IPC
            return Ok(Vec::new());
        }

        let schema = batches[0].schema();
        let output = Vec::new();
        let mut cursor = Cursor::new(output);

        {
            let mut writer = StreamWriter::try_new(&mut cursor, &schema).map_err(|e| {
                CubeError::internal(format!("Failed to create Arrow IPC writer: {}", e))
            })?;

            // Write all batches
            for batch in batches {
                // Verify schema consistency
                if batch.schema() != schema {
                    return Err(CubeError::internal(
                        "All record batches must have the same schema".to_string(),
                    ));
                }

                writer.write(batch).map_err(|e| {
                    CubeError::internal(format!("Failed to write Arrow IPC record batch: {}", e))
                })?;
            }

            writer.finish().map_err(|e| {
                CubeError::internal(format!("Failed to finish Arrow IPC writer: {}", e))
            })?;
        }

        Ok(cursor.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::ipc::reader::StreamReader;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let names = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
        let ages = Arc::new(Int64Array::from(vec![25, 30, 35]));

        RecordBatch::try_new(schema, vec![names, ages]).unwrap()
    }

    fn create_test_batches() -> Vec<RecordBatch> {
        vec![create_test_batch(), create_test_batch()]
    }

    #[test]
    fn test_serialize_single_batch() {
        let batch = create_test_batch();
        let result = ArrowIPCSerializer::serialize_single(&batch);

        assert!(result.is_ok());
        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
    }

    #[test]
    fn test_serialize_multiple_batches() {
        let batches = create_test_batches();
        let result = ArrowIPCSerializer::serialize_streaming(&batches);

        assert!(result.is_ok());
        let ipc_bytes = result.unwrap();
        assert!(!ipc_bytes.is_empty());
    }

    #[test]
    fn test_serialize_empty_batch_list() {
        let batches: Vec<RecordBatch> = vec![];
        let result = ArrowIPCSerializer::serialize_streaming(&batches);

        assert!(result.is_ok());
        let ipc_bytes = result.unwrap();
        assert!(ipc_bytes.is_empty());
    }

    #[test]
    fn test_roundtrip_single_batch() {
        let batch = create_test_batch();

        // Serialize
        let ipc_bytes = ArrowIPCSerializer::serialize_single(&batch).unwrap();

        // Deserialize
        let cursor = Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let read_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        // Verify
        assert_eq!(read_batches.len(), 1);
        let read_batch = &read_batches[0];
        assert_eq!(read_batch.schema(), batch.schema());
        assert_eq!(read_batch.num_rows(), batch.num_rows());
        assert_eq!(read_batch.num_columns(), batch.num_columns());
    }

    #[test]
    fn test_roundtrip_multiple_batches() {
        let batches = create_test_batches();

        // Serialize
        let ipc_bytes = ArrowIPCSerializer::serialize_streaming(&batches).unwrap();

        // Deserialize
        let cursor = Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let read_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

        // Verify
        assert_eq!(read_batches.len(), batches.len());
        for (original, read) in batches.iter().zip(read_batches.iter()) {
            assert_eq!(read.schema(), original.schema());
            assert_eq!(read.num_rows(), original.num_rows());
        }
    }

    #[test]
    fn test_roundtrip_preserves_data() {
        let batch = create_test_batch();

        // Serialize
        let ipc_bytes = ArrowIPCSerializer::serialize_single(&batch).unwrap();

        // Deserialize
        let cursor = Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).unwrap();
        let read_batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let read_batch = &read_batches[0];

        // Verify data content
        let names = read_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ages = read_batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Charlie");
        assert_eq!(ages.value(0), 25);
        assert_eq!(ages.value(1), 30);
        assert_eq!(ages.value(2), 35);
    }

    #[test]
    fn test_schema_mismatch_error() {
        let schema1 = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let schema2 = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        let batch1 =
            RecordBatch::try_new(schema1, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let batch2 = RecordBatch::try_new(
            schema2,
            vec![Arc::new(StringArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap();

        let result = ArrowIPCSerializer::serialize_streaming(&[batch1, batch2]);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("same schema"));
    }
}
