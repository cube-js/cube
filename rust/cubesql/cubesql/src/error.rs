use cubeclient::apis::default_api::{LoadV1Error, MetaV1Error};
use datafusion::arrow;
use log::SetLoggerError;
use sqlparser::parser::ParserError;
use std::{
    any::Any,
    backtrace::Backtrace,
    collections::HashMap,
    fmt,
    fmt::{Debug, Formatter},
    num::ParseIntError,
};
use tokio::{sync::mpsc::error::SendError, time::error::Elapsed};

#[derive(thiserror::Error, Debug)]
pub struct CubeError {
    pub message: String,
    pub cause: CubeErrorCauseType,
    pub backtrace: Option<Backtrace>,
}

#[derive(Debug, Clone)]
pub enum CubeErrorCauseType {
    User(Option<HashMap<String, String>>),
    Internal(Option<HashMap<String, String>>),
}

impl CubeError {
    pub fn user(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::User(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn internal(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Internal(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn internal_with_bt(message: String, backtrace: Option<Backtrace>) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Internal(None),
            backtrace,
        }
    }

    pub fn panic(error: Box<dyn Any + Send>) -> Self {
        if let Some(reason) = error.downcast_ref::<&str>() {
            CubeError::internal(format!("Unexpected panic. Reason: {}", reason))
        } else {
            CubeError::internal("Unexpected panic without reason".to_string())
        }
    }
}

impl CubeError {
    pub fn backtrace(&self) -> Option<&Backtrace> {
        self.backtrace.as_ref()
    }

    pub fn to_backtrace(self) -> Option<Backtrace> {
        self.backtrace
    }
}

impl fmt::Display for CubeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fn write_fmt(
            cause: CubeErrorCauseType,
            message: String,
            f: &mut Formatter<'_>,
        ) -> fmt::Result {
            match &cause {
                CubeErrorCauseType::User(meta) => {
                    f.write_fmt(format_args!("{} {:?}", message, meta))
                }
                CubeErrorCauseType::Internal(meta) => {
                    f.write_fmt(format_args!("{:?}: {} {:?}", cause, message, meta))
                }
            }
        }

        write_fmt(self.cause.clone(), self.message.clone(), f)
    }
}

impl From<cubeclient::apis::Error<LoadV1Error>> for CubeError {
    fn from(v: cubeclient::apis::Error<LoadV1Error>) -> Self {
        let message: String = match v {
            cubeclient::apis::Error::ResponseError(e) => match e.entity {
                None => e.content,
                Some(LoadV1Error::UnknownValue(_)) => e.content,
                Some(LoadV1Error::Status4XX(unwrapped)) => unwrapped.error,
                Some(LoadV1Error::Status5XX(unwrapped)) => unwrapped.error,
            },
            _ => v.to_string(),
        };
        return CubeError::internal(message);
    }
}

impl From<cubeclient::apis::Error<MetaV1Error>> for CubeError {
    fn from(v: cubeclient::apis::Error<MetaV1Error>) -> Self {
        let message: String = match v {
            cubeclient::apis::Error::ResponseError(e) => match e.entity {
                None => e.content,
                Some(MetaV1Error::UnknownValue(_)) => e.content,
                Some(MetaV1Error::Status4XX(unwrapped)) => unwrapped.error,
                Some(MetaV1Error::Status5XX(unwrapped)) => unwrapped.error,
            },
            _ => v.to_string(),
        };
        return CubeError::internal(message);
    }
}

impl From<crate::compile::CompilationError> for CubeError {
    fn from(v: crate::compile::CompilationError) -> Self {
        let cause = match &v {
            crate::compile::CompilationError::User(_, meta)
            | crate::compile::CompilationError::Unsupported(_, meta)
            | crate::compile::CompilationError::Internal(_, _, meta) => {
                CubeErrorCauseType::Internal(meta.clone())
            }
        };
        let mut err = CubeError::internal_with_bt(v.to_string(), v.to_backtrace());
        err.cause = cause;

        err
    }
}

impl From<std::io::Error> for CubeError {
    fn from(v: std::io::Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<ParserError> for CubeError {
    fn from(v: ParserError) -> Self {
        CubeError::internal(format!("{:?}", v))
    }
}

impl From<rust_decimal::Error> for CubeError {
    fn from(v: rust_decimal::Error) -> Self {
        CubeError::internal(format!("{:?}", v))
    }
}

impl From<tokio::task::JoinError> for CubeError {
    fn from(v: tokio::task::JoinError) -> Self {
        if v.is_panic() {
            CubeError::panic(v.into_panic())
        } else {
            // JoinError can return CanceledError
            CubeError::internal(v.to_string())
        }
    }
}

impl<T> From<SendError<T>> for CubeError
where
    T: Debug,
{
    fn from(v: SendError<T>) -> Self {
        CubeError::internal(v.to_string())
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
        CubeError::internal(v.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for CubeError {
    fn from(v: datafusion::error::DataFusionError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<arrow::error::ArrowError> for CubeError {
    fn from(v: arrow::error::ArrowError) -> Self {
        CubeError::internal(v.to_string())
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
        CubeError::internal(v.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for CubeError {
    fn from(v: Box<bincode::ErrorKind>) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::watch::error::SendError<bool>> for CubeError {
    fn from(v: tokio::sync::watch::error::SendError<bool>) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::watch::error::RecvError> for CubeError {
    fn from(v: tokio::sync::watch::error::RecvError) -> Self {
        CubeError::internal(v.to_string())
    }
}
impl From<ParseIntError> for CubeError {
    fn from(v: ParseIntError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<reqwest::Error> for CubeError {
    fn from(v: reqwest::Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<SetLoggerError> for CubeError {
    fn from(v: SetLoggerError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<serde_json::Error> for CubeError {
    fn from(v: serde_json::Error) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<std::num::ParseFloatError> for CubeError {
    fn from(v: std::num::ParseFloatError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<base64::DecodeError> for CubeError {
    fn from(v: base64::DecodeError) -> Self {
        CubeError::internal(v.to_string())
    }
}

impl From<tokio::sync::AcquireError> for CubeError {
    fn from(v: tokio::sync::AcquireError) -> Self {
        CubeError::internal(v.to_string())
    }
}
