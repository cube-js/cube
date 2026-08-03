use neon::result::Throw;
use std::{
    backtrace::Backtrace,
    fmt,
    fmt::{Debug, Formatter},
};

#[derive(thiserror::Error, Debug)]
pub struct CubeError {
    pub message: String,
    pub cause: CubeErrorCauseType,
    pub backtrace: Option<Backtrace>,
}

#[derive(Debug)]
pub enum CubeErrorCauseType {
    User,
    Internal,
    NeonThrow(Throw),
}

impl CubeError {
    pub fn user(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::User,
            backtrace: Some(Backtrace::capture()),
        }
    }

    pub fn internal(message: String) -> Self {
        Self {
            message,
            cause: CubeErrorCauseType::Internal,
            backtrace: Some(Backtrace::capture()),
        }
    }
    fn neon_throw(e: Throw) -> Self {
        Self {
            message: e.to_string(),
            cause: CubeErrorCauseType::NeonThrow(e),
            backtrace: Some(Backtrace::capture()),
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
        match &self.cause {
            CubeErrorCauseType::User => f.write_fmt(format_args!("{}", self.message)),
            CubeErrorCauseType::Internal => f.write_fmt(format_args!("{}", self.message)),
            CubeErrorCauseType::NeonThrow(_) => f.write_fmt(format_args!("{}", self.message)),
        }
    }
}

impl From<Throw> for CubeError {
    fn from(e: Throw) -> Self {
        Self::neon_throw(e)
    }
}
