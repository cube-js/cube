use std::{backtrace::Backtrace, sync::Arc};

use datafusion::{arrow::error::ArrowError, error::DataFusionError};
use pg_srv::{
    protocol::{self, ErrorResponse},
    ProtocolError,
};

use crate::{compile::CompilationError, transport::SpanId, CubeError};

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("CubeError: {0}")]
    Cube(CubeError, Option<Arc<SpanId>>),
    #[error("DataFusionError: {0}")]
    DataFusion(DataFusionError, Option<Arc<SpanId>>),
    #[error("ArrowError: {0}")]
    Arrow(ArrowError, Option<Arc<SpanId>>),
    #[error("CompilationError: {0}")]
    CompilationError(CompilationError, Option<Arc<SpanId>>),
    #[error("ProtocolError: {0}")]
    Protocol(ProtocolError, Option<Arc<SpanId>>),
}

impl ConnectionError {
    /// Return Backtrace from any variant of Enum
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match &self {
            ConnectionError::Cube(e, _) => e.backtrace(),
            ConnectionError::CompilationError(e, _) => e.backtrace(),
            ConnectionError::Protocol(e, _) => e.backtrace(),
            ConnectionError::DataFusion(_, _) | ConnectionError::Arrow(_, _) => None,
        }
    }

    /// Converts Error to protocol::ErrorResponse which is usefully for writing response to the client
    pub fn to_error_response(self) -> protocol::ErrorResponse {
        match self {
            ConnectionError::Cube(e, _) => Self::cube_to_error_response(&e),
            ConnectionError::DataFusion(e, _) => Self::df_to_error_response(&e),
            ConnectionError::Arrow(e, _) => Self::arrow_to_error_response(&e),
            ConnectionError::CompilationError(e, _) => {
                fn to_error_response(e: CompilationError) -> protocol::ErrorResponse {
                    match e {
                        CompilationError::Internal(_, _, _) => protocol::ErrorResponse::error(
                            protocol::ErrorCode::InternalError,
                            e.to_string(),
                        ),
                        CompilationError::User(_, _) => protocol::ErrorResponse::error(
                            protocol::ErrorCode::InvalidSqlStatement,
                            e.to_string(),
                        ),
                        CompilationError::Unsupported(_, _) => protocol::ErrorResponse::error(
                            protocol::ErrorCode::FeatureNotSupported,
                            e.to_string(),
                        ),
                        CompilationError::Fatal(_, _) => protocol::ErrorResponse::fatal(
                            protocol::ErrorCode::InternalError,
                            e.to_string(),
                        ),
                    }
                }

                to_error_response(e)
            }
            ConnectionError::Protocol(e, _) => e.to_error_response(),
        }
    }

    pub fn with_span_id(self, span_id: Option<Arc<SpanId>>) -> Self {
        match self {
            ConnectionError::Cube(e, _) => ConnectionError::Cube(e, span_id),
            ConnectionError::DataFusion(e, _) => ConnectionError::DataFusion(e, span_id),
            ConnectionError::Arrow(e, _) => ConnectionError::Arrow(e, span_id),
            ConnectionError::CompilationError(e, _) => {
                ConnectionError::CompilationError(e, span_id)
            }
            ConnectionError::Protocol(e, _) => ConnectionError::Protocol(e, span_id),
        }
    }

    pub fn span_id(&self) -> Option<Arc<SpanId>> {
        match self {
            ConnectionError::Cube(_, span_id) => span_id.clone(),
            ConnectionError::DataFusion(_, span_id) => span_id.clone(),
            ConnectionError::Arrow(_, span_id) => span_id.clone(),
            ConnectionError::CompilationError(_, span_id) => span_id.clone(),
            ConnectionError::Protocol(_, span_id) => span_id.clone(),
        }
    }

    fn cube_to_error_response(e: &CubeError) -> protocol::ErrorResponse {
        let message = e.to_string();
        // Remove `Error: ` prefix that can come from JS
        let message = if let Some(message) = message.strip_prefix("Error: ") {
            message.to_string()
        } else {
            message
        };
        protocol::ErrorResponse::error(protocol::ErrorCode::InternalError, message)
    }

    fn df_to_error_response(e: &DataFusionError) -> protocol::ErrorResponse {
        match e {
            DataFusionError::ArrowError(arrow_err) => {
                return Self::arrow_to_error_response(arrow_err);
            }
            DataFusionError::External(err) => {
                if let Some(cube_err) = err.downcast_ref::<CubeError>() {
                    return Self::cube_to_error_response(cube_err);
                }
            }
            _ => {}
        }
        protocol::ErrorResponse::error(
            protocol::ErrorCode::InternalError,
            format!("Post-processing Error: {}", e),
        )
    }

    fn arrow_to_error_response(e: &ArrowError) -> protocol::ErrorResponse {
        match e {
            ArrowError::ExternalError(err) => {
                if let Some(df_err) = err.downcast_ref::<DataFusionError>() {
                    return Self::df_to_error_response(df_err);
                }
                if let Some(cube_err) = err.downcast_ref::<CubeError>() {
                    return Self::cube_to_error_response(cube_err);
                }
            }
            _ => {}
        }
        protocol::ErrorResponse::error(
            protocol::ErrorCode::InternalError,
            format!("Post-processing Error: {}", e),
        )
    }
}

impl From<CubeError> for ConnectionError {
    fn from(e: CubeError) -> Self {
        ConnectionError::Cube(e, None)
    }
}

impl From<CompilationError> for ConnectionError {
    fn from(e: CompilationError) -> Self {
        ConnectionError::CompilationError(e, None)
    }
}

impl From<ProtocolError> for ConnectionError {
    fn from(e: ProtocolError) -> Self {
        ConnectionError::Protocol(e, None)
    }
}

impl From<tokio::task::JoinError> for ConnectionError {
    fn from(e: tokio::task::JoinError) -> Self {
        ConnectionError::Cube(e.into(), None)
    }
}

impl From<DataFusionError> for ConnectionError {
    fn from(e: DataFusionError) -> Self {
        ConnectionError::DataFusion(e, None)
    }
}

impl From<ArrowError> for ConnectionError {
    fn from(e: ArrowError) -> Self {
        ConnectionError::Arrow(e, None)
    }
}

/// Auto converting for all kind of io:Error to ConnectionError, sugar
impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Protocol(e.into(), None)
    }
}

/// Auto converting for all kind of io:Error to ConnectionError, sugar
impl From<ErrorResponse> for ConnectionError {
    fn from(e: ErrorResponse) -> Self {
        ConnectionError::Protocol(e.into(), None)
    }
}
