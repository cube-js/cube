use std::sync::Arc;

use pg_srv::protocol;
use sqlparser::ast::Value;

use super::shim::ConnectionError;
use crate::transport::SpanId;

pub fn parse_fetch_limit(
    limit: &Value,
    span_id: &Option<Arc<SpanId>>,
) -> Result<usize, ConnectionError> {
    match limit {
        Value::Number(v, negative) => {
            if *negative {
                return Err(ConnectionError::Protocol(
                    protocol::ErrorResponse::error(
                        protocol::ErrorCode::ObjectNotInPrerequisiteState,
                        "cursor can only scan forward".to_string(),
                    )
                    .into(),
                    span_id.clone(),
                ));
            }
            v.parse::<usize>().map_err(|err| {
                ConnectionError::Protocol(
                    protocol::ErrorResponse::error(
                        protocol::ErrorCode::ProtocolViolation,
                        format!(r#"Unable to parse number "{}" for fetch limit: {}"#, v, err),
                    )
                    .into(),
                    span_id.clone(),
                )
            })
        }
        other => Err(ConnectionError::Protocol(
            protocol::ErrorResponse::error(
                protocol::ErrorCode::ProtocolViolation,
                format!("FETCH limit must be a number, got: {}", other),
            )
            .into(),
            span_id.clone(),
        )),
    }
}
