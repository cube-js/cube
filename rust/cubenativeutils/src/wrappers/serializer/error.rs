use cubesql::CubeError;
use serde::{de, ser};
use std::{fmt, fmt::Display};
#[derive(Debug)]
pub enum NativeObjSerializerError {
    Message(String),
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
        }
    }
}

impl std::error::Error for NativeObjSerializerError {}

impl From<CubeError> for NativeObjSerializerError {
    fn from(value: CubeError) -> Self {
        Self::Message(value.to_string())
    }
}
