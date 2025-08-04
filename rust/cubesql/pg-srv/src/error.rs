//! Protocol error types for PostgreSQL wire protocol

use crate::protocol;
use std::{backtrace::Backtrace, fmt::Formatter};

/// Protocol error abstract of handled/unhandled errors, it should not handle any kind of business logic errors
/// TODO: Migrate back to thiserror crate, when Rust will stabilize feature(error_generic_member_access)
#[derive(Debug)]
pub enum ProtocolError {
    IO {
        source: std::io::Error,
        backtrace: Backtrace,
    },
    ErrorResponse {
        source: protocol::ErrorResponse,
        backtrace: Backtrace,
    },
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolError::IO { source, .. } => f.write_fmt(format_args!("IO error: {}", source)),
            ProtocolError::ErrorResponse { source, .. } => {
                f.write_fmt(format_args!("Error: {}", source.message))
            }
        }
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(source: std::io::Error) -> Self {
        ProtocolError::IO {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<protocol::ErrorResponse> for ProtocolError {
    fn from(source: protocol::ErrorResponse) -> Self {
        ProtocolError::ErrorResponse {
            source,
            backtrace: Backtrace::capture(),
        }
    }
}

impl ProtocolError {
    /// Return Backtrace from any variant of Enum
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match &self {
            ProtocolError::IO { backtrace, .. } => Some(backtrace),
            ProtocolError::ErrorResponse { backtrace, .. } => Some(backtrace),
        }
    }

    /// Converts Error to protocol::ErrorResponse which is usefully for writing response to the client
    pub fn to_error_response(self) -> protocol::ErrorResponse {
        match self {
            ProtocolError::IO { source, .. } => protocol::ErrorResponse::error(
                protocol::ErrorCode::InternalError,
                source.to_string(),
            ),
            ProtocolError::ErrorResponse { source, .. } => source,
        }
    }
}
