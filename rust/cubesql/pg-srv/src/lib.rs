#![feature(backtrace)]
#![feature(type_ascription)]

pub mod buffer;
mod decoding;
pub mod extended;
pub mod pg_type;
pub mod protocol;

pub use buffer::*;
pub use decoding::*;
pub use extended::*;
pub use pg_type::*;

use std::backtrace::Backtrace;

/// Protocol error abstract of handled/unhandled errors, it should not handle any kind of business logic errors
#[derive(thiserror::Error, Debug)]
pub enum ProtocolError {
    #[error("IO Error: {}", .source)]
    IO {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("Error: {}", .source.message)]
    ErrorResponse {
        #[from]
        source: protocol::ErrorResponse,
        backtrace: Backtrace,
    },
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
