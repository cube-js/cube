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
    // User Error is an uncategorized error caused by user input or action
    User(Option<HashMap<String, String>>),
    // Internal Error is an uncategorized error caused by internal failures
    Internal(Option<HashMap<String, String>>),
    // REST API Error is an error thrown by REST API when running in standalone mode
    RestApi(Option<HashMap<String, String>>),
    // SQL Parser Error is an error thrown when SQL cannot be parsed
    SqlParser(Option<HashMap<String, String>>),
    // Unsupported Error is an error thrown when a feature/option is not supported
    Unsupported(Option<HashMap<String, String>>),
    // Planning Error is an error thrown when the query plan is invalid
    Planning(Option<HashMap<String, String>>),
    // Post-Processing Error is an error thrown during execution of the query
    PostProcessing(Option<HashMap<String, String>>),
    // Rewrite Error is an error thrown during logical plan e-graph rewriting
    Rewrite(Option<HashMap<String, String>>),
    // Database Execution Error is an error thrown during execution of the query in the database
    DatabaseExecution(Option<HashMap<String, String>>),
    // Continue wait is an error used internally to indicate that the query is still
    // being processed and the client should send the request again
    ContinueWait,
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

    pub fn rest_api(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::RestApi(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn sql_parser(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::SqlParser(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn unsupported(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Unsupported(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn planning(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Planning(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn post_processing(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::PostProcessing(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn rewrite(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Rewrite(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn database_execution(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::DatabaseExecution(None),
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn continue_wait() -> Self {
        Self {
            message: "Continue wait".to_string(),
            cause: CubeErrorCauseType::ContinueWait,
            backtrace: None,
        }
    }

    pub fn panic(error: Box<dyn Any + Send>) -> Self {
        Self::panic_with_message(error, "Unexpected panic")
    }

    pub fn panic_with_message(error: Box<dyn Any + Send>, message: &str) -> Self {
        if let Some(reason) = error.downcast_ref::<&str>() {
            CubeError::internal(format!("{}. Reason: {}", message, reason))
        } else if let Some(reason) = error.downcast_ref::<String>() {
            CubeError::internal(format!("{}. Reason: {}", message, reason))
        } else {
            CubeError::internal(format!("{} without reason", message))
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

impl CubeErrorCauseType {
    pub fn meta(&self) -> Option<&HashMap<String, String>> {
        match self {
            CubeErrorCauseType::User(meta)
            | CubeErrorCauseType::Internal(meta)
            | CubeErrorCauseType::RestApi(meta)
            | CubeErrorCauseType::SqlParser(meta)
            | CubeErrorCauseType::Unsupported(meta)
            | CubeErrorCauseType::Planning(meta)
            | CubeErrorCauseType::PostProcessing(meta)
            | CubeErrorCauseType::Rewrite(meta)
            | CubeErrorCauseType::DatabaseExecution(meta) => meta.as_ref(),
            CubeErrorCauseType::ContinueWait => None,
        }
    }
}

impl fmt::Display for CubeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.cause {
            CubeErrorCauseType::User(_) => {
                f.write_fmt(format_args!("User Error: {}", self.message))
            }
            CubeErrorCauseType::Internal(_) => {
                f.write_fmt(format_args!("Internal Error: {}", self.message))
            }
            CubeErrorCauseType::RestApi(_) => {
                f.write_fmt(format_args!("REST API Error: {}", self.message))
            }
            CubeErrorCauseType::SqlParser(_) => {
                f.write_fmt(format_args!("SQL Parser Error: {}", self.message))
            }
            CubeErrorCauseType::Unsupported(_) => {
                f.write_fmt(format_args!("Unsupported Error: {}", self.message))
            }
            CubeErrorCauseType::Planning(_) => {
                f.write_fmt(format_args!("Planning Error: {}", self.message))
            }
            CubeErrorCauseType::PostProcessing(_) => {
                f.write_fmt(format_args!("Post-Processing Error: {}", self.message))
            }
            CubeErrorCauseType::Rewrite(_) => f.write_fmt(format_args!(
                "Rewrite Error: {}. Please check logs for additional information",
                self.message
            )),
            CubeErrorCauseType::DatabaseExecution(_) => {
                f.write_fmt(format_args!("Database Execution Error: {}", self.message))
            }
            CubeErrorCauseType::ContinueWait => write!(f, "Continue wait"),
        }
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
        return CubeError::rest_api(message);
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
        return CubeError::rest_api(message);
    }
}

impl From<crate::compile::CompilationError> for CubeError {
    fn from(v: crate::compile::CompilationError) -> Self {
        let (message, cause) = match &v {
            crate::compile::CompilationError::Internal(message, _, meta)
            | crate::compile::CompilationError::Fatal(message, meta) => {
                (message.clone(), CubeErrorCauseType::Internal(meta.clone()))
            }
            crate::compile::CompilationError::User(message, meta) => {
                (message.clone(), CubeErrorCauseType::User(meta.clone()))
            }
            crate::compile::CompilationError::RestApi(message, meta) => {
                (message.clone(), CubeErrorCauseType::RestApi(meta.clone()))
            }
            crate::compile::CompilationError::SqlParser(message, meta) => {
                (message.clone(), CubeErrorCauseType::SqlParser(meta.clone()))
            }
            crate::compile::CompilationError::Unsupported(message, meta) => (
                message.clone(),
                CubeErrorCauseType::Unsupported(meta.clone()),
            ),
            crate::compile::CompilationError::Planning(message, meta) => {
                (message.clone(), CubeErrorCauseType::Planning(meta.clone()))
            }
            crate::compile::CompilationError::PostProcessing(message, meta) => (
                message.clone(),
                CubeErrorCauseType::PostProcessing(meta.clone()),
            ),
            crate::compile::CompilationError::Rewrite(message, meta) => {
                (message.clone(), CubeErrorCauseType::Rewrite(meta.clone()))
            }
            crate::compile::CompilationError::DatabaseExecution(message, meta) => (
                message.clone(),
                CubeErrorCauseType::DatabaseExecution(meta.clone()),
            ),
            crate::compile::CompilationError::ContinueWait => (
                "Continue wait".to_string(),
                CubeErrorCauseType::ContinueWait,
            ),
        };
        let mut err = CubeError::internal_with_bt(message, v.to_backtrace());
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
        match v {
            ParserError::ParserError(message) => CubeError::sql_parser(message),
            ParserError::TokenizerError(message) => CubeError::sql_parser(message),
        }
    }
}

impl From<rust_decimal::Error> for CubeError {
    fn from(v: rust_decimal::Error) -> Self {
        CubeError::internal(format!("Decimal Error: {}", v))
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
        match v {
            datafusion::error::DataFusionError::ArrowError(e) => CubeError::from(e),
            datafusion::error::DataFusionError::SQL(e) => CubeError::from(e),
            datafusion::error::DataFusionError::NotImplemented(e) => CubeError::unsupported(e),
            datafusion::error::DataFusionError::Internal(e) => CubeError::internal(e),
            datafusion::error::DataFusionError::Plan(e) => CubeError::planning(e),
            datafusion::error::DataFusionError::Execution(e) => CubeError::post_processing(e),
            datafusion::error::DataFusionError::External(e) => match e.downcast::<CubeError>() {
                Ok(e) => *e,
                Err(e) => match e.downcast::<arrow::error::ArrowError>() {
                    Ok(e) => CubeError::from(*e),
                    Err(e) => CubeError::internal(e.to_string()),
                },
            },
            _ => CubeError::internal(v.to_string()),
        }
    }
}

impl From<arrow::error::ArrowError> for CubeError {
    fn from(v: arrow::error::ArrowError) -> Self {
        match v {
            arrow::error::ArrowError::NotYetImplemented(e) => CubeError::unsupported(e),
            arrow::error::ArrowError::ExternalError(e) => match e.downcast::<CubeError>() {
                Ok(e) => *e,
                Err(e) => match e.downcast::<datafusion::error::DataFusionError>() {
                    Ok(e) => CubeError::from(*e),
                    Err(e) => CubeError::internal(e.to_string()),
                },
            },
            v @ arrow::error::ArrowError::CastError(_)
            | v @ arrow::error::ArrowError::ParseError(_)
            | v @ arrow::error::ArrowError::ComputeError(_)
            | v @ arrow::error::ArrowError::DivideByZero => {
                CubeError::post_processing(v.to_string())
            }
            _ => CubeError::internal(v.to_string()),
        }
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

impl<T> From<tokio::sync::watch::error::SendError<T>> for CubeError {
    fn from(v: tokio::sync::watch::error::SendError<T>) -> Self {
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
