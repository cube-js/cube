#![feature(in_band_lifetimes)]
#![feature(test)]
#![feature(backtrace)]
#![feature(async_closure)]
#![feature(drain_filter)]
#![feature(box_patterns)]
#![feature(slice_internals)]
#![feature(raw)]
#![feature(total_cmp)]
#![feature(vec_into_raw_parts)]
#![feature(hash_set_entry)]
#![feature(map_first_last)]
#![feature(arc_new_cyclic)]
#![feature(bindings_after_at)]

#[macro_use]
extern crate lazy_static;

use core::fmt;
use log::SetLoggerError;
use serde_derive::{Deserialize, Serialize};
use smallvec::alloc::fmt::{Debug, Formatter};
use sqlparser::parser::ParserError;
use std::backtrace::Backtrace;
use std::num::ParseIntError;
use tokio::sync::mpsc::error::SendError;
use tokio::time::error::Elapsed;

pub mod compile;
pub mod config;
pub mod mysql;
pub mod schema;
pub mod telemetry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeError {
    pub message: String,
    pub cause: CubeErrorCauseType,
}

impl std::error::Error for CubeError {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CubeErrorCauseType {
    User,
    Internal,
}

impl CubeError {
    pub fn user(message: String) -> CubeError {
        CubeError {
            message,
            cause: CubeErrorCauseType::User,
        }
    }

    pub fn internal(message: String) -> CubeError {
        CubeError {
            message,
            cause: CubeErrorCauseType::Internal,
        }
    }

    pub fn from_error<E: fmt::Display>(error: E) -> CubeError {
        CubeError {
            message: format!("{}\n{}", error, Backtrace::capture()),
            cause: CubeErrorCauseType::Internal,
        }
    }

    fn from_debug_error<E: Debug>(error: E) -> CubeError {
        CubeError {
            message: format!("{:?}\n{}", error, Backtrace::capture()),
            cause: CubeErrorCauseType::Internal,
        }
    }
}

impl fmt::Display for CubeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{:?}: {}", self.cause, self.message))
    }
}

impl<T> From<cubeclient::apis::Error<T>> for CubeError {
    fn from(v: cubeclient::apis::Error<T>) -> Self {
        CubeError::from_error(v)
    }
}

impl From<crate::compile::CompilationError> for CubeError {
    fn from(v: crate::compile::CompilationError) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<std::io::Error> for CubeError {
    fn from(v: std::io::Error) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<ParserError> for CubeError {
    fn from(v: ParserError) -> Self {
        CubeError::internal(format!("{:?}", v))
    }
}

impl From<tokio::task::JoinError> for CubeError {
    fn from(v: tokio::task::JoinError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl<T> From<SendError<T>> for CubeError
where
    T: Debug,
{
    fn from(v: SendError<T>) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<std::time::SystemTimeError> for CubeError {
    fn from(v: std::time::SystemTimeError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<Elapsed> for CubeError {
    fn from(v: Elapsed) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for CubeError {
    fn from(v: tokio::sync::broadcast::error::RecvError) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<datafusion::error::DataFusionError> for CubeError {
    fn from(v: datafusion::error::DataFusionError) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<chrono::ParseError> for CubeError {
    fn from(v: chrono::ParseError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<std::string::FromUtf8Error> for CubeError {
    fn from(v: std::string::FromUtf8Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for CubeError {
    fn from(v: tokio::sync::oneshot::error::RecvError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<Box<bincode::ErrorKind>> for CubeError {
    fn from(v: Box<bincode::ErrorKind>) -> Self {
        CubeError::from_debug_error(v)
    }
}

impl From<tokio::sync::watch::error::SendError<bool>> for CubeError {
    fn from(v: tokio::sync::watch::error::SendError<bool>) -> Self {
        CubeError::from_error(v)
    }
}

impl From<tokio::sync::watch::error::RecvError> for CubeError {
    fn from(v: tokio::sync::watch::error::RecvError) -> Self {
        CubeError::from_error(v)
    }
}
impl From<ParseIntError> for CubeError {
    fn from(v: ParseIntError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<reqwest::Error> for CubeError {
    fn from(v: reqwest::Error) -> Self {
        CubeError::from_error(v)
    }
}

impl From<SetLoggerError> for CubeError {
    fn from(v: SetLoggerError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<serde_json::Error> for CubeError {
    fn from(v: serde_json::Error) -> Self {
        CubeError::from_error(v)
    }
}

impl From<std::num::ParseFloatError> for CubeError {
    fn from(v: std::num::ParseFloatError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<base64::DecodeError> for CubeError {
    fn from(v: base64::DecodeError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<tokio::sync::AcquireError> for CubeError {
    fn from(v: tokio::sync::AcquireError) -> Self {
        CubeError::from_error(v)
    }
}
