use std::{backtrace::Backtrace, collections::HashMap, fmt::Formatter};

use crate::{CubeError, CubeErrorCauseType};

/// TODO: Migrate back to thiserror crate, when Rust will stabilize feature(error_generic_member_access)
#[derive(Debug)]
pub enum CompilationError {
    // Internal Error is an uncategorized error caused by internal failures
    Internal(String, Backtrace, Option<HashMap<String, String>>),
    // User Error is an uncategorized error caused by user input or action
    User(String, Option<HashMap<String, String>>),
    // REST API Error is an error thrown by REST API when running in standalone mode
    RestApi(String, Option<HashMap<String, String>>),
    // SQL Parser Error is an error thrown when SQL cannot be parsed
    SqlParser(String, Option<HashMap<String, String>>),
    // Unsupported Error is an error thrown when a feature/option is not supported
    Unsupported(String, Option<HashMap<String, String>>),
    // Planning Error is an error thrown when the query plan is invalid
    Planning(String, Option<HashMap<String, String>>),
    // Post-Processing Error is an error thrown during execution of the query
    PostProcessing(String, Option<HashMap<String, String>>),
    // Rewrite Error is an error thrown during logical plan e-graph rewriting
    Rewrite(String, Option<HashMap<String, String>>),
    // Database Execution Error is an error thrown during execution of the query in the database
    DatabaseExecution(String, Option<HashMap<String, String>>),
    // Fatal Error is an error used internally with the PostgreSQL protocol
    // to indicate that the connection should be closed immediately
    // after sending the error response to the client
    Fatal(String, Option<HashMap<String, String>>),
    // Continue wait is an error used internally to indicate that the query is still
    // being processed and the client should send the request again
    ContinueWait,
}

impl std::fmt::Display for CompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompilationError::Internal(message, _, _) => {
                f.write_fmt(format_args!("Internal Error: {}", message))
            }
            CompilationError::User(message, _) => {
                f.write_fmt(format_args!("User Error: {}", message))
            }
            CompilationError::RestApi(message, _) => {
                f.write_fmt(format_args!("REST API Error: {}", message))
            }
            CompilationError::SqlParser(message, _) => {
                f.write_fmt(format_args!("SQL Parser Error: {}", message))
            }
            CompilationError::Unsupported(message, _) => {
                f.write_fmt(format_args!("Unsupported Error: {}", message))
            }
            CompilationError::Planning(message, _) => {
                f.write_fmt(format_args!("Planning Error: {}", message))
            }
            CompilationError::PostProcessing(message, _) => {
                f.write_fmt(format_args!("Post-Processing Error: {}", message))
            }
            CompilationError::Rewrite(message, _) => f.write_fmt(format_args!(
                "Rewrite Error: {}. Please check logs for additional information",
                message
            )),
            CompilationError::DatabaseExecution(message, _) => {
                f.write_fmt(format_args!("Database Execution Error: {}", message))
            }
            CompilationError::Fatal(message, _) => {
                f.write_fmt(format_args!("Fatal Error: {}", message))
            }
            CompilationError::ContinueWait => f.write_str("Continue wait"),
        }
    }
}

impl PartialEq for CompilationError {
    fn eq(&self, other: &Self) -> bool {
        match &self {
            CompilationError::Internal(left, _, _) => match other {
                CompilationError::Internal(right, _, _) => left == right,
                _ => false,
            },
            CompilationError::User(left, _) => match other {
                CompilationError::User(right, _) => left == right,
                _ => false,
            },
            CompilationError::RestApi(left, _) => match other {
                CompilationError::RestApi(right, _) => left == right,
                _ => false,
            },
            CompilationError::SqlParser(left, _) => match other {
                CompilationError::SqlParser(right, _) => left == right,
                _ => false,
            },
            CompilationError::Unsupported(left, _) => match other {
                CompilationError::Unsupported(right, _) => left == right,
                _ => false,
            },
            CompilationError::Planning(left, _) => match other {
                CompilationError::Planning(right, _) => left == right,
                _ => false,
            },
            CompilationError::PostProcessing(left, _) => match other {
                CompilationError::PostProcessing(right, _) => left == right,
                _ => false,
            },
            CompilationError::Rewrite(left, _) => match other {
                CompilationError::Rewrite(right, _) => left == right,
                _ => false,
            },
            CompilationError::DatabaseExecution(left, _) => match other {
                CompilationError::DatabaseExecution(right, _) => left == right,
                _ => false,
            },
            CompilationError::Fatal(left, _) => match other {
                CompilationError::Fatal(right, _) => left == right,
                _ => false,
            },
            CompilationError::ContinueWait => matches!(other, CompilationError::ContinueWait),
        }
    }

    fn ne(&self, other: &Self) -> bool {
        !self.eq(other)
    }
}

impl CompilationError {
    pub fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            CompilationError::Internal(_, bt, _) => Some(bt),
            CompilationError::User(_, _) => None,
            CompilationError::RestApi(_, _) => None,
            CompilationError::SqlParser(_, _) => None,
            CompilationError::Unsupported(_, _) => None,
            CompilationError::Planning(_, _) => None,
            CompilationError::PostProcessing(_, _) => None,
            CompilationError::Rewrite(_, _) => None,
            CompilationError::DatabaseExecution(_, _) => None,
            CompilationError::Fatal(_, _) => None,
            CompilationError::ContinueWait => None,
        }
    }

    pub fn to_backtrace(self) -> Option<Backtrace> {
        match self {
            CompilationError::Internal(_, bt, _) => Some(bt),
            CompilationError::User(_, _) => None,
            CompilationError::RestApi(_, _) => None,
            CompilationError::SqlParser(_, _) => None,
            CompilationError::Unsupported(_, _) => None,
            CompilationError::Planning(_, _) => None,
            CompilationError::PostProcessing(_, _) => None,
            CompilationError::Rewrite(_, _) => None,
            CompilationError::DatabaseExecution(_, _) => None,
            CompilationError::Fatal(_, _) => None,
            CompilationError::ContinueWait => None,
        }
    }
}

impl CompilationError {
    pub fn internal(message: String) -> Self {
        Self::Internal(message, Backtrace::capture(), None)
    }

    pub fn internal_with_bt(message: String, bt: Backtrace) -> Self {
        Self::Internal(message, bt, None)
    }

    pub fn user(message: String) -> Self {
        Self::User(message, None)
    }

    pub fn rest_api(message: String) -> Self {
        Self::RestApi(message, None)
    }

    pub fn sql_parser(message: String) -> Self {
        Self::SqlParser(message, None)
    }

    pub fn unsupported(message: String) -> Self {
        Self::Unsupported(message, None)
    }

    pub fn planning(message: String) -> Self {
        Self::Planning(message, None)
    }

    pub fn post_processing(message: String) -> Self {
        Self::PostProcessing(message, None)
    }

    pub fn rewrite(message: String) -> Self {
        Self::Rewrite(message, None)
    }

    pub fn database_execution(message: String) -> Self {
        Self::DatabaseExecution(message, None)
    }

    pub fn fatal(message: String) -> Self {
        Self::Fatal(message, None)
    }

    pub fn continue_wait() -> Self {
        Self::ContinueWait
    }
}

impl CompilationError {
    pub fn message(&self) -> String {
        match self {
            CompilationError::Internal(msg, _, _)
            | CompilationError::User(msg, _)
            | CompilationError::RestApi(msg, _)
            | CompilationError::SqlParser(msg, _)
            | CompilationError::Unsupported(msg, _)
            | CompilationError::Planning(msg, _)
            | CompilationError::PostProcessing(msg, _)
            | CompilationError::Rewrite(msg, _)
            | CompilationError::DatabaseExecution(msg, _)
            | CompilationError::Fatal(msg, _) => msg.clone(),
            CompilationError::ContinueWait => "Continue wait".to_string(),
        }
    }

    pub fn with_message(self, msg: String) -> Self {
        match self {
            CompilationError::Internal(_, bts, meta) => CompilationError::Internal(msg, bts, meta),
            CompilationError::User(_, meta) => CompilationError::User(msg, meta),
            CompilationError::RestApi(_, meta) => CompilationError::RestApi(msg, meta),
            CompilationError::SqlParser(_, meta) => CompilationError::SqlParser(msg, meta),
            CompilationError::Unsupported(_, meta) => CompilationError::Unsupported(msg, meta),
            CompilationError::Planning(_, meta) => CompilationError::Planning(msg, meta),
            CompilationError::PostProcessing(_, meta) => {
                CompilationError::PostProcessing(msg, meta)
            }
            CompilationError::Rewrite(_, meta) => CompilationError::Rewrite(msg, meta),
            CompilationError::DatabaseExecution(_, meta) => {
                CompilationError::DatabaseExecution(msg, meta)
            }
            CompilationError::Fatal(_, meta) => CompilationError::Fatal(msg, meta),
            CompilationError::ContinueWait => CompilationError::ContinueWait,
        }
    }
}

impl CompilationError {
    pub fn with_meta(self, meta: Option<HashMap<String, String>>) -> Self {
        match self {
            CompilationError::Internal(msg, bts, _) => CompilationError::Internal(msg, bts, meta),
            CompilationError::User(msg, _) => CompilationError::User(msg, meta),
            CompilationError::RestApi(msg, _) => CompilationError::RestApi(msg, meta),
            CompilationError::SqlParser(msg, _) => CompilationError::SqlParser(msg, meta),
            CompilationError::Unsupported(msg, _) => CompilationError::Unsupported(msg, meta),
            CompilationError::Planning(msg, _) => CompilationError::Planning(msg, meta),
            CompilationError::PostProcessing(msg, _) => CompilationError::PostProcessing(msg, meta),
            CompilationError::Rewrite(msg, _) => CompilationError::Rewrite(msg, meta),
            CompilationError::DatabaseExecution(msg, _) => {
                CompilationError::DatabaseExecution(msg, meta)
            }
            CompilationError::Fatal(msg, _) => CompilationError::Fatal(msg, meta),
            CompilationError::ContinueWait => CompilationError::ContinueWait,
        }
    }
}

pub type CompilationResult<T> = std::result::Result<T, CompilationError>;

impl From<regex::Error> for CompilationError {
    fn from(v: regex::Error) -> Self {
        CompilationError::internal(format!("{:?}", v))
    }
}

impl From<serde_json::Error> for CompilationError {
    fn from(v: serde_json::Error) -> Self {
        CompilationError::internal(format!("{:?}", v))
    }
}

impl From<CubeError> for CompilationError {
    fn from(v: CubeError) -> Self {
        match v.cause {
            CubeErrorCauseType::User(meta) => CompilationError::User(v.message, meta),
            CubeErrorCauseType::Internal(meta) => CompilationError::Internal(
                v.message,
                v.backtrace.unwrap_or_else(|| Backtrace::capture()),
                meta,
            ),
            CubeErrorCauseType::RestApi(meta) => CompilationError::RestApi(v.message, meta),
            CubeErrorCauseType::SqlParser(meta) => CompilationError::SqlParser(v.message, meta),
            CubeErrorCauseType::Unsupported(meta) => CompilationError::Unsupported(v.message, meta),
            CubeErrorCauseType::Planning(meta) => CompilationError::Planning(v.message, meta),
            CubeErrorCauseType::PostProcessing(meta) => {
                CompilationError::PostProcessing(v.message, meta)
            }
            CubeErrorCauseType::Rewrite(meta) => CompilationError::Rewrite(v.message, meta),
            CubeErrorCauseType::DatabaseExecution(meta) => {
                CompilationError::DatabaseExecution(v.message, meta)
            }
            CubeErrorCauseType::ContinueWait => CompilationError::ContinueWait,
        }
    }
}
