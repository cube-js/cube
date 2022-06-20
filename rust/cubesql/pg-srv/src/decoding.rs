use crate::{
    protocol::{ErrorCode, ErrorResponse, Format},
    ProtocolError,
};
use byteorder::{ByteOrder, LittleEndian};

// This trait explains how to decode values from the protocol
// It's used in the Bind message
pub trait FromProtocolValue<T> {
    // Converts native type to raw value in text format
    fn from_protocol(raw: &Vec<u8>, format: Format) -> Result<T, ProtocolError> {
        match format {
            Format::Text => Self::from_text(raw),
            Format::Binary => Self::from_binary(raw),
        }
    }

    // Decodes raw value to native type in text format
    fn from_text(raw: &Vec<u8>) -> Result<T, ProtocolError>;

    // Decodes raw value to native type in binary format
    fn from_binary(raw: &Vec<u8>) -> Result<T, ProtocolError>;
}

impl FromProtocolValue<String> for String {
    fn from_text(raw: &Vec<u8>) -> Result<Self, ProtocolError> {
        String::from_utf8(raw.clone()).map_err(|err| {
            ErrorResponse::error(ErrorCode::ProtocolViolation, err.to_string()).into():
                ProtocolError
        })
    }

    fn from_binary(raw: &Vec<u8>) -> Result<Self, ProtocolError> {
        String::from_utf8(raw.clone()).map_err(|err| {
            ErrorResponse::error(ErrorCode::ProtocolViolation, err.to_string()).into():
                ProtocolError
        })
    }
}

impl FromProtocolValue<i64> for i64 {
    fn from_text(raw: &Vec<u8>) -> Result<Self, ProtocolError> {
        let as_str = String::from_utf8(raw.clone()).map_err(|err| {
            ErrorResponse::error(ErrorCode::ProtocolViolation, err.to_string()).into():
                ProtocolError
        })?;

        as_str.parse::<i64>().map_err(|err| {
            ErrorResponse::error(ErrorCode::ProtocolViolation, err.to_string()).into():
                ProtocolError
        })
    }

    fn from_binary(raw: &Vec<u8>) -> Result<Self, ProtocolError> {
        Ok(LittleEndian::read_i64(&raw[..]))
    }
}
