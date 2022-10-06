//! Decoding values from the Protocol representation

use crate::{
    protocol::{ErrorCode, ErrorResponse, Format},
    ProtocolError,
};
use byteorder::{BigEndian, ByteOrder};

/// This trait explains how to decode values from the protocol
/// It's used in the Bind message
pub trait FromProtocolValue {
    // Converts native type to raw value in specific format
    fn from_protocol(raw: &Vec<u8>, format: Format) -> Result<Self, ProtocolError>
    where
        Self: Sized,
    {
        match format {
            Format::Text => Self::from_text(raw),
            Format::Binary => Self::from_binary(raw),
        }
    }

    /// Decodes raw value to native type in text format
    fn from_text(raw: &Vec<u8>) -> Result<Self, ProtocolError>
    where
        Self: Sized;

    /// Decodes raw value to native type in binary format
    fn from_binary(raw: &Vec<u8>) -> Result<Self, ProtocolError>
    where
        Self: Sized;
}

impl FromProtocolValue for String {
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

impl FromProtocolValue for i64 {
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
        Ok(BigEndian::read_i64(&raw[..]))
    }
}

impl FromProtocolValue for bool {
    fn from_text(raw: &Vec<u8>) -> Result<Self, ProtocolError> {
        match raw[0] {
            b't' => Ok(true),
            b'f' => Ok(false),
            other => Err(ErrorResponse::error(
                ErrorCode::ProtocolViolation,
                format!("Unable to decode bool from text, actual: {}", other),
            )
            .into(): ProtocolError),
        }
    }

    fn from_binary(raw: &Vec<u8>) -> Result<Self, ProtocolError> {
        match raw[0] {
            1 => Ok(true),
            0 => Ok(false),
            other => Err(ErrorResponse::error(
                ErrorCode::ProtocolViolation,
                format!("Unable to decode bool from binary, actual: {}", other),
            )
            .into(): ProtocolError),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    use crate::protocol::Format;
    use bytes::BytesMut;

    fn assert_test_decode<T: ToProtocolValue + FromProtocolValue + std::cmp::PartialEq>(
        value: T,
        format: Format,
    ) -> Result<(), ProtocolError> {
        let mut buf = BytesMut::new();
        value.to_protocol(&mut buf, format)?;

        // skip length
        let mut encoded = Vec::with_capacity(buf.len() - 4);
        encoded.extend_from_slice(&buf.as_ref()[4..]);

        assert_eq!(value, T::from_protocol(&encoded, format)?);

        Ok(())
    }

    #[test]
    fn test_text_decoders() -> Result<(), ProtocolError> {
        assert_test_decode("test".to_string(), Format::Text)?;
        assert_test_decode(true, Format::Text)?;
        assert_test_decode(false, Format::Text)?;
        assert_test_decode(1_i64, Format::Text)?;
        assert_test_decode(100_i64, Format::Text)?;

        Ok(())
    }

    #[test]
    fn test_binary_decoders() -> Result<(), ProtocolError> {
        assert_test_decode("test".to_string(), Format::Binary)?;
        assert_test_decode(true, Format::Binary)?;
        assert_test_decode(false, Format::Binary)?;
        assert_test_decode(1_i64, Format::Binary)?;
        assert_test_decode(100_i64, Format::Binary)?;

        Ok(())
    }
}
