use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use cubeshared::codegen::{
    root_as_http_message, HttpCommand, HttpMessage, HttpMessageArgs, HttpQuery, HttpQueryArgs,
    HttpQueryResultData, QueryResultFormat,
};
use flatbuffers::FlatBufferBuilder;

use crate::error::TransportError;
use crate::result::{QueryResult, ResultData};

/// Build a binary FlatBuffer payload carrying an `HttpQuery` command.
pub fn encode_query(message_id: u32, connection_id: &str, sql: &str) -> Bytes {
    let mut builder = FlatBufferBuilder::with_capacity(1024);
    let query_offset = builder.create_string(sql);

    let http_query = HttpQuery::create(
        &mut builder,
        &HttpQueryArgs {
            query: Some(query_offset),
            trace_obj: None,
            inline_tables: None,
            parameters: None,
            response_format: QueryResultFormat::Arrow,
        },
    );

    let connection_id_offset = builder.create_string(connection_id);

    let message = HttpMessage::create(
        &mut builder,
        &HttpMessageArgs {
            message_id,
            command_type: HttpCommand::HttpQuery,
            command: Some(http_query.as_union_value()),
            connection_id: Some(connection_id_offset),
        },
    );
    builder.finish(message, None);
    Bytes::copy_from_slice(builder.finished_data())
}

/// Decoded response from the server.
pub enum DecodedResponse {
    Ok(QueryResult),
    Error(String),
}

pub struct DecodedFrame {
    pub message_id: u32,
    pub response: DecodedResponse,
}

/// Parse an incoming binary frame, extracting message id and result/error.
pub fn decode_frame(bytes: &[u8]) -> Result<DecodedFrame, TransportError> {
    let msg = root_as_http_message(bytes)
        .map_err(|e| TransportError::Protocol(format!("flatbuffer decode: {e}")))?;
    let message_id = msg.message_id();

    let response = match msg.command_type() {
        HttpCommand::HttpError => {
            let err = msg.command_as_http_error().ok_or_else(|| {
                TransportError::Protocol("HttpError union variant missing".into())
            })?;
            DecodedResponse::Error(err.error().unwrap_or("unknown error").to_string())
        }
        HttpCommand::HttpResultSet => {
            let rs = msg.command_as_http_result_set().ok_or_else(|| {
                TransportError::Protocol("HttpResultSet union variant missing".into())
            })?;

            let columns: Vec<String> = rs
                .columns()
                .map(|cols| cols.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default();

            let mut rows: Vec<Vec<Option<String>>> = Vec::new();
            if let Some(row_vec) = rs.rows() {
                rows.reserve(row_vec.len());
                for row in row_vec.iter() {
                    let mut out: Vec<Option<String>> = Vec::with_capacity(columns.len());
                    if let Some(values) = row.values() {
                        for v in values.iter() {
                            out.push(v.string_value().map(|s| s.to_string()));
                        }
                    }
                    rows.push(out);
                }
            }

            log::debug!(
                "decoded HttpResultSet: {} columns, {} rows",
                columns.len(),
                rows.len()
            );

            DecodedResponse::Ok(QueryResult {
                data: ResultData::Legacy { columns, rows },
            })
        }
        HttpCommand::HttpQueryResult => {
            let qr = msg.command_as_http_query_result().ok_or_else(|| {
                TransportError::Protocol("HttpQueryResult union variant missing".into())
            })?;

            match qr.data_type() {
                HttpQueryResultData::HttpQueryResultArrow => {
                    let arrow = qr.data_as_http_query_result_arrow().ok_or_else(|| {
                        TransportError::Protocol(
                            "HttpQueryResult.data variant is not HttpQueryResultArrow".into(),
                        )
                    })?;

                    let result = decode_arrow_ipc(arrow.data().bytes())?;
                    log::debug!(
                        "decoded HttpQueryResult (Arrow IPC): {} columns, {} rows",
                        result.get_columns().len(),
                        result.row_count()
                    );

                    DecodedResponse::Ok(result)
                }
                HttpQueryResultData::HttpQueryResultCompleted => {
                    // Command completed without a result set (zero columns).
                    log::debug!("decoded HttpQueryResult (Completed): no result set");
                    DecodedResponse::Ok(QueryResult {
                        data: ResultData::Completed,
                    })
                }
                other => {
                    return Err(TransportError::Protocol(format!(
                        "unsupported HttpQueryResult.data variant: {:?}",
                        other.variant_name()
                    )));
                }
            }
        }
        other => {
            return Err(TransportError::Protocol(format!(
                "unexpected command variant: {:?}",
                other.variant_name()
            )));
        }
    };

    Ok(DecodedFrame {
        message_id,
        response,
    })
}

fn decode_arrow_ipc(bytes: &[u8]) -> Result<QueryResult, TransportError> {
    let reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|e| TransportError::Protocol(format!("arrow IPC open: {e}")))?;

    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<_, _>>()
        .map_err(|e| TransportError::Protocol(format!("arrow IPC read batch: {e}")))?;

    Ok(QueryResult {
        data: ResultData::Arrow { schema, batches },
    })
}
