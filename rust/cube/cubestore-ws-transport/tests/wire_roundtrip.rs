//! Round-trip the on-wire FlatBuffer: encode an HttpQuery, parse it back, and verify shape.

use std::sync::Arc;

use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use cubeshared::codegen::{
    root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpCommand, HttpMessage,
    HttpMessageArgs, HttpQueryResult, HttpQueryResultArgs, HttpQueryResultArrow,
    HttpQueryResultArrowArgs, HttpQueryResultCompleted, HttpQueryResultCompletedArgs,
    HttpQueryResultData, HttpResultSet, HttpResultSetArgs, HttpRow, HttpRowArgs, QueryResultFormat,
};
use cubestore_ws_transport::codec::{decode_frame, encode_query, DecodedResponse};
use cubestore_ws_transport::{ResponseFormat, ResultData, TransportError};
use flatbuffers::FlatBufferBuilder;

#[test]
fn query_round_trip() -> Result<(), TransportError> {
    let bytes = encode_query(7, "conn-xyz", "SELECT 42");
    let msg = root_as_http_message(&bytes).expect("parse encoded message");

    assert_eq!(msg.message_id(), 7);
    assert_eq!(msg.connection_id(), Some("conn-xyz"));
    assert_eq!(msg.command_type(), HttpCommand::HttpQuery);

    let q = msg.command_as_http_query().expect("HttpQuery variant");
    assert_eq!(q.query(), Some("SELECT 42"));
    assert_eq!(q.response_format(), QueryResultFormat::Arrow);
    assert!(q.trace_obj().is_none());
    assert!(q.inline_tables().is_none());
    assert!(q.parameters().is_none());

    Ok(())
}

#[test]
fn decode_preserves_all_columns_for_wide_table() -> Result<(), TransportError> {
    let mut b = FlatBufferBuilder::with_capacity(8 * 1024);

    let col_names = [
        "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11",
    ];
    let col_offsets: Vec<_> = col_names.iter().map(|n| b.create_string(n)).collect();
    let cols_vec = b.create_vector(&col_offsets);

    let cell_offsets: Vec<_> = (0..col_names.len())
        .map(|i| {
            let s = b.create_string(&format!("v{i}"));
            HttpColumnValue::create(
                &mut b,
                &HttpColumnValueArgs {
                    string_value: Some(s),
                },
            )
        })
        .collect();
    let values = b.create_vector(&cell_offsets);
    let row = HttpRow::create(
        &mut b,
        &HttpRowArgs {
            values: Some(values),
        },
    );
    let rows = b.create_vector(&[row]);

    let rs = HttpResultSet::create(
        &mut b,
        &HttpResultSetArgs {
            columns: Some(cols_vec),
            rows: Some(rows),
        },
    );
    let conn = b.create_string("c");
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id: 9,
            command_type: HttpCommand::HttpResultSet,
            command: Some(rs.as_union_value()),
            connection_id: Some(conn),
        },
    );
    b.finish(msg, None);
    let bytes = b.finished_data().to_vec();

    let decoded = decode_frame(&bytes)?;
    let r = match decoded.response {
        DecodedResponse::Ok(r) => r,
        DecodedResponse::Error(e) => panic!("err: {e}"),
    };
    assert_eq!(
        r.get_columns(),
        col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "all column names should decode"
    );
    let rows = match &r.data {
        ResultData::Legacy { rows, .. } => rows,
        _ => panic!("expected ResultData::Legacy"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), col_names.len(), "all cells should decode");
    for (i, cell) in rows[0].iter().enumerate() {
        assert_eq!(cell.as_deref(), Some(format!("v{i}").as_str()));
    }

    Ok(())
}

#[test]
fn decode_preserves_all_rows_and_long_strings() -> Result<(), TransportError> {
    // Build a synthetic 500-row HttpResultSet with progressively longer string values.
    let mut b = FlatBufferBuilder::with_capacity(64 * 1024);
    let col_a = b.create_string("id");
    let col_b = b.create_string("payload");
    let cols = b.create_vector(&[col_a, col_b]);

    let mut row_offsets = Vec::with_capacity(500);
    for i in 0..500u32 {
        let id_str = b.create_string(&i.to_string());
        let id_cell = HttpColumnValue::create(
            &mut b,
            &HttpColumnValueArgs {
                string_value: Some(id_str),
            },
        );
        // Long-ish payload to make sure nothing is truncated.
        let payload = "x".repeat((i as usize % 256) + 1);
        let payload_str = b.create_string(&payload);
        let payload_cell = HttpColumnValue::create(
            &mut b,
            &HttpColumnValueArgs {
                string_value: Some(payload_str),
            },
        );
        let values = b.create_vector(&[id_cell, payload_cell]);
        let row = HttpRow::create(
            &mut b,
            &HttpRowArgs {
                values: Some(values),
            },
        );
        row_offsets.push(row);
    }
    let rows = b.create_vector(&row_offsets);
    let rs = HttpResultSet::create(
        &mut b,
        &HttpResultSetArgs {
            columns: Some(cols),
            rows: Some(rows),
        },
    );
    let conn = b.create_string("cx");
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id: 1,
            command_type: HttpCommand::HttpResultSet,
            command: Some(rs.as_union_value()),
            connection_id: Some(conn),
        },
    );
    b.finish(msg, None);
    let bytes = b.finished_data().to_vec();

    let decoded = decode_frame(&bytes)?;
    assert_eq!(decoded.message_id, 1);
    let result = match decoded.response {
        DecodedResponse::Ok(r) => r,
        DecodedResponse::Error(e) => panic!("unexpected error: {e}"),
    };

    assert_eq!(
        result.get_columns(),
        vec!["id".to_string(), "payload".to_string()]
    );
    let rows = match &result.data {
        ResultData::Legacy { rows, .. } => rows,
        _ => panic!("expected ResultData::Legacy"),
    };
    assert_eq!(rows.len(), 500, "expected all 500 rows decoded");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.len(), 2);
        assert_eq!(row[0].as_deref(), Some(i.to_string().as_str()));
        let expected_payload_len = (i % 256) + 1;
        assert_eq!(
            row[1].as_deref().map(|s| s.len()),
            Some(expected_payload_len),
            "payload truncated at row {i}"
        );
    }

    Ok(())
}

#[test]
fn decode_arrow_ipc_result_with_nulls() -> Result<(), TransportError> {
    // Build a small RecordBatch mirroring what the server would write via
    // datafusion::arrow::ipc::writer::StreamWriter, wrap the IPC bytes into
    // the HttpQueryResult / HttpQueryResultArrow flatbuffer, and decode.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec![Some("alice"), None, Some("carol")]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])
        .expect("record batch");

    let mut ipc_bytes: Vec<u8> = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut ipc_bytes, schema.as_ref()).expect("ipc writer");
        w.write(&batch).expect("write batch");
        w.finish().expect("finish ipc");
    }

    let mut b = FlatBufferBuilder::with_capacity(ipc_bytes.len() + 1024);
    let ipc_vec = b.create_vector(&ipc_bytes);
    let arrow_payload = HttpQueryResultArrow::create(
        &mut b,
        &HttpQueryResultArrowArgs {
            data: Some(ipc_vec),
            is_last: true,
        },
    );
    let qr = HttpQueryResult::create(
        &mut b,
        &HttpQueryResultArgs {
            data_type: HttpQueryResultData::HttpQueryResultArrow,
            data: Some(arrow_payload.as_union_value()),
        },
    );
    let conn = b.create_string("c");
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id: 42,
            command_type: HttpCommand::HttpQueryResult,
            command: Some(qr.as_union_value()),
            connection_id: Some(conn),
        },
    );
    b.finish(msg, None);
    let bytes = b.finished_data().to_vec();

    let decoded = decode_frame(&bytes)?;
    assert_eq!(decoded.message_id, 42);
    let result = match decoded.response {
        DecodedResponse::Ok(r) => r,
        DecodedResponse::Error(e) => panic!("unexpected error: {e}"),
    };

    assert_eq!(
        result.get_columns(),
        vec!["id".to_string(), "name".to_string()]
    );
    assert_eq!(result.get_format(), ResponseFormat::Arrow);
    assert_eq!(result.row_count(), 3);

    let batches = match result.data {
        ResultData::Arrow { batches, .. } => batches,
        _ => panic!("expected ResultData::Arrow"),
    };
    assert_eq!(batches.len(), 1, "single record batch expected");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32 id column");
    assert_eq!(id_col.values(), &[1, 2, 3]);
    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 name column");
    assert_eq!(name_col.value(0), "alice");
    assert!(name_col.is_null(1), "row 1 name should be NULL");
    assert_eq!(name_col.value(2), "carol");

    Ok(())
}

#[test]
fn decode_query_result_completed() -> Result<(), TransportError> {
    // Commands that complete without a result set (CREATE TABLE/INSERT,
    // queue/cache writes) come back as HttpQueryResult carrying the
    // HttpQueryResultCompleted marker instead of an Arrow IPC payload.
    let mut b = FlatBufferBuilder::with_capacity(1024);
    let completed = HttpQueryResultCompleted::create(&mut b, &HttpQueryResultCompletedArgs {});
    let qr = HttpQueryResult::create(
        &mut b,
        &HttpQueryResultArgs {
            data_type: HttpQueryResultData::HttpQueryResultCompleted,
            data: Some(completed.as_union_value()),
        },
    );
    let conn = b.create_string("c");
    let msg = HttpMessage::create(
        &mut b,
        &HttpMessageArgs {
            message_id: 7,
            command_type: HttpCommand::HttpQueryResult,
            command: Some(qr.as_union_value()),
            connection_id: Some(conn),
        },
    );
    b.finish(msg, None);
    let bytes = b.finished_data().to_vec();

    let decoded = decode_frame(&bytes)?;
    assert_eq!(decoded.message_id, 7);
    let result = match decoded.response {
        DecodedResponse::Ok(r) => r,
        DecodedResponse::Error(e) => panic!("unexpected error: {e}"),
    };

    assert!(matches!(result.data, ResultData::Completed));
    assert!(result.is_empty());
    assert_eq!(result.row_count(), 0);
    assert_eq!(result.get_columns(), Vec::<String>::new());
    assert_eq!(result.get_format(), ResponseFormat::Completed);

    Ok(())
}
