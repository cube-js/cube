//! Round-trip the on-wire FlatBuffer: encode an HttpQuery, parse it back, and verify shape.

use cubeshared::codegen::{
    root_as_http_message, HttpColumnValue, HttpColumnValueArgs, HttpCommand, HttpMessage,
    HttpMessageArgs, HttpResultSet, HttpResultSetArgs, HttpRow, HttpRowArgs, QueryResultFormat,
};
use cubestore_ws_transport::codec::{decode_frame, encode_query, DecodedResponse};
use flatbuffers::FlatBufferBuilder;

#[test]
fn query_round_trip() {
    let bytes = encode_query(7, "conn-xyz", "SELECT 42");
    let msg = root_as_http_message(&bytes).expect("parse encoded message");

    assert_eq!(msg.message_id(), 7);
    assert_eq!(msg.connection_id(), Some("conn-xyz"));
    assert_eq!(msg.command_type(), HttpCommand::HttpQuery);

    let q = msg.command_as_http_query().expect("HttpQuery variant");
    assert_eq!(q.query(), Some("SELECT 42"));
    assert_eq!(q.response_format(), QueryResultFormat::Legacy);
    assert!(q.trace_obj().is_none());
    assert!(q.inline_tables().is_none());
    assert!(q.parameters().is_none());
}

#[test]
fn decode_preserves_all_columns_for_wide_table() {
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

    let decoded = decode_frame(&bytes).expect("decode");
    let r = match decoded.response {
        DecodedResponse::Ok(r) => r,
        DecodedResponse::Error(e) => panic!("err: {e}"),
    };
    assert_eq!(
        r.columns,
        col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        "all column names should decode"
    );
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0].len(), col_names.len(), "all cells should decode");
    for (i, cell) in r.rows[0].iter().enumerate() {
        assert_eq!(cell.as_deref(), Some(format!("v{i}").as_str()));
    }
}

#[test]
fn decode_preserves_all_rows_and_long_strings() {
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

    let decoded = decode_frame(&bytes).expect("decode");
    assert_eq!(decoded.message_id, 1);
    let result = match decoded.response {
        DecodedResponse::Ok(r) => r,
        DecodedResponse::Error(e) => panic!("unexpected error: {e}"),
    };

    assert_eq!(
        result.columns,
        vec!["id".to_string(), "payload".to_string()]
    );
    assert_eq!(result.rows.len(), 500, "expected all 500 rows decoded");
    for (i, row) in result.rows.iter().enumerate() {
        assert_eq!(row.len(), 2);
        assert_eq!(row[0].as_deref(), Some(i.to_string().as_str()));
        let expected_payload_len = (i % 256) + 1;
        assert_eq!(
            row[1].as_deref().map(|s| s.len()),
            Some(expected_payload_len),
            "payload truncated at row {i}"
        );
    }
}
