use crate::{CubeError, CubeErrorCauseType};
use serde::{de, ser};
use std::{fmt, fmt::Display};
#[derive(Debug)]
pub enum NativeObjSerializerError {
    Message(String),
    /// Keeps the native error intact: a `NeonThrow` means a JS exception is
    /// pending and has to reach JS as-is, a new error cannot be thrown over it.
    Native(CubeError),
}

impl NativeObjSerializerError {
    pub fn context(self, context: impl Display) -> Self {
        match self {
            Self::Message(msg) => Self::Message(format!("{context}: {msg}")),
            Self::Native(mut err) => {
                err.message = format!("{context}: {}", err.message);
                Self::Native(err)
            }
        }
    }

    pub fn into_cube_error(self, context: impl Display) -> CubeError {
        match self {
            Self::Native(err) if matches!(err.cause, CubeErrorCauseType::NeonThrow(_)) => err,
            err => CubeError::internal(format!("{context}: {err}")),
        }
    }
}

impl ser::Error for NativeObjSerializerError {
    fn custom<T: Display>(msg: T) -> Self {
        NativeObjSerializerError::Message(msg.to_string())
    }
}

impl de::Error for NativeObjSerializerError {
    fn custom<T: Display>(msg: T) -> Self {
        NativeObjSerializerError::Message(msg.to_string())
    }
}

impl Display for NativeObjSerializerError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NativeObjSerializerError::Message(msg) => formatter.write_str(msg),
            NativeObjSerializerError::Native(err) => write!(formatter, "{err}"),
        }
    }
}

impl std::error::Error for NativeObjSerializerError {}

impl From<CubeError> for NativeObjSerializerError {
    fn from(value: CubeError) -> Self {
        Self::Native(value)
    }
}
