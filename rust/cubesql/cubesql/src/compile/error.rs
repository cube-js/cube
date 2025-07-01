use std::{backtrace::Backtrace, collections::HashMap, fmt::Formatter};

/// TODO: Migrate back to thiserror crate, when Rust will stabilize feature(error_generic_member_access)
#[derive(Debug)]
pub enum CompilationError {
    Internal(String, Backtrace, Option<HashMap<String, String>>),
    User(String, Option<HashMap<String, String>>),
    Unsupported(String, Option<HashMap<String, String>>),
    Fatal(String, Option<HashMap<String, String>>),
}

impl std::fmt::Display for CompilationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompilationError::Internal(message, _, _) => {
                f.write_fmt(format_args!("SQLCompilationError: Internal: {}", message))
            }
            CompilationError::User(message, _) => {
                f.write_fmt(format_args!("SQLCompilationError: User: {}", message))
            }
            CompilationError::Unsupported(message, _) => f.write_fmt(format_args!(
                "SQLCompilationError: Unsupported: {}",
                message
            )),
            CompilationError::Fatal(message, _) => {
                f.write_fmt(format_args!("SQLCompilationError: Fatal: {}", message))
            }
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
            CompilationError::Unsupported(left, _) => match other {
                CompilationError::Unsupported(right, _) => left == right,
                _ => false,
            },
            CompilationError::Fatal(left, _) => match other {
                CompilationError::Fatal(right, _) => left == right,
                _ => false,
            },
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
            CompilationError::Unsupported(_, _) => None,
            CompilationError::Fatal(_, _) => None,
        }
    }

    pub fn to_backtrace(self) -> Option<Backtrace> {
        match self {
            CompilationError::Internal(_, bt, _) => Some(bt),
            CompilationError::User(_, _) => None,
            CompilationError::Unsupported(_, _) => None,
            CompilationError::Fatal(_, _) => None,
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

    pub fn unsupported(message: String) -> Self {
        Self::Unsupported(message, None)
    }

    pub fn fatal(message: String) -> Self {
        Self::Fatal(message, None)
    }
}

impl CompilationError {
    pub fn message(&self) -> String {
        match self {
            CompilationError::Internal(msg, _, _)
            | CompilationError::User(msg, _)
            | CompilationError::Unsupported(msg, _) => msg.clone(),
            CompilationError::Fatal(msg, _) => msg.clone(),
        }
    }

    pub fn with_message(self, msg: String) -> Self {
        match self {
            CompilationError::Internal(_, bts, meta) => CompilationError::Internal(msg, bts, meta),
            CompilationError::User(_, meta) => CompilationError::User(msg, meta),
            CompilationError::Unsupported(_, meta) => CompilationError::Unsupported(msg, meta),
            CompilationError::Fatal(_, meta) => CompilationError::Fatal(msg, meta),
        }
    }
}

impl CompilationError {
    pub fn with_meta(self, meta: Option<HashMap<String, String>>) -> Self {
        match self {
            CompilationError::Internal(msg, bts, _) => CompilationError::Internal(msg, bts, meta),
            CompilationError::User(msg, _) => CompilationError::User(msg, meta),
            CompilationError::Unsupported(msg, _) => CompilationError::Unsupported(msg, meta),
            CompilationError::Fatal(msg, _) => CompilationError::Fatal(msg, meta),
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
