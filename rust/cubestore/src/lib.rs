#![feature(in_band_lifetimes)]
#![feature(specialization)]
#![feature(test)]
#![feature(backtrace)]
#![feature(async_closure)]
#![feature(drain_filter)]
// #![feature(trace_macros)]

// trace_macros!(true);
#[macro_use]
extern crate lazy_static;

use arrow::error::ArrowError;
use core::fmt;
use flexbuffers::{DeserializationError, ReaderError};
use log::SetLoggerError;
use parquet::errors::ParquetError;
use serde_derive::{Deserialize, Serialize};
use smallvec::alloc::fmt::{Debug, Formatter};
use sqlparser::parser::ParserError;
use std::backtrace::Backtrace;
use std::num::ParseIntError;
use tokio::sync::mpsc::error::SendError;
use std::sync::PoisonError;
use crate::metastore::TableId;

pub mod cluster;
pub mod config;
pub mod http;
pub mod import;
pub mod metastore;
pub mod mysql;
pub mod queryplanner;
pub mod remotefs;
pub mod scheduler;
pub mod sql;
pub mod store;
pub mod table;
pub mod telemetry;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CubeError {
    message: String,
    cause: CubeErrorCauseType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CubeErrorCauseType {
    User,
    Internal,
}

impl CubeError {
    fn user(message: String) -> CubeError {
        CubeError {
            message,
            cause: CubeErrorCauseType::User,
        }
    }

    fn internal(message: String) -> CubeError {
        CubeError {
            message,
            cause: CubeErrorCauseType::Internal,
        }
    }

    fn from_error<E: fmt::Display>(error: E) -> CubeError {
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

impl From<flexbuffers::DeserializationError> for CubeError {
    fn from(v: DeserializationError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<rocksdb::Error> for CubeError {
    fn from(v: rocksdb::Error) -> Self {
        CubeError::internal(v.into_string())
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

impl From<CubeError> for warp::reject::Rejection {
    fn from(_: CubeError) -> Self {
        // TODO
        warp::reject()
    }
}

impl From<ParquetError> for CubeError {
    fn from(v: ParquetError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::task::JoinError> for CubeError {
    fn from(v: tokio::task::JoinError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<SendError<metastore::MetaStoreEvent>> for CubeError {
    fn from(v: SendError<metastore::MetaStoreEvent>) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<std::time::SystemTimeError> for CubeError {
    fn from(v: std::time::SystemTimeError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::time::Elapsed> for CubeError {
    fn from(v: tokio::time::Elapsed) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for CubeError {
    fn from(v: datafusion::error::DataFusionError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<CubeError> for datafusion::error::DataFusionError {
    fn from(v: CubeError) -> Self {
        datafusion::error::DataFusionError::Execution(v.to_string())
    }
}

impl From<arrow::error::ArrowError> for CubeError {
    fn from(v: ArrowError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::broadcast::RecvError> for CubeError {
    fn from(v: tokio::sync::broadcast::RecvError) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<tokio::sync::broadcast::SendError<metastore::MetaStoreEvent>> for CubeError {
    fn from(v: tokio::sync::broadcast::SendError<metastore::MetaStoreEvent>) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<tokio::sync::broadcast::SendError<cluster::JobEvent>> for CubeError {
    fn from(v: tokio::sync::broadcast::SendError<cluster::JobEvent>) -> Self {
        CubeError::internal(format!("{:?}\n{}", v, Backtrace::capture()))
    }
}

impl From<bigdecimal::ParseBigDecimalError> for CubeError {
    fn from(v: bigdecimal::ParseBigDecimalError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<flexbuffers::SerializationError> for CubeError {
    fn from(v: flexbuffers::SerializationError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<s3::S3Error> for CubeError {
    fn from(v: s3::S3Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<awscreds::AwsCredsError> for CubeError {
    fn from(v: awscreds::AwsCredsError) -> Self {
        CubeError::user(v.to_string())
    }
}

impl From<awsregion::AwsRegionError> for CubeError {
    fn from(v: awsregion::AwsRegionError) -> Self {
        CubeError::user(v.to_string())
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

impl From<procspawn::SpawnError> for CubeError {
    fn from(v: procspawn::SpawnError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::oneshot::error::RecvError> for CubeError {
    fn from(v: tokio::sync::oneshot::error::RecvError) -> Self {
        CubeError::from_error(v)
    }
}

impl From<ipc_channel::ipc::IpcError> for CubeError {
    fn from(v: ipc_channel::ipc::IpcError) -> Self {
        CubeError::from_debug_error(v)
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

impl From<PoisonError<std::sync::MutexGuard<'_, std::collections::HashMap<TableId, u64>>>> for CubeError {
    fn from(v: PoisonError<std::sync::MutexGuard<'_, std::collections::HashMap<TableId, u64>>>) -> Self {
        CubeError::from_error(v)
    }
}

impl From<ReaderError> for CubeError {
    fn from(v: ReaderError) -> Self {
        CubeError::from_error(v)
    }
}