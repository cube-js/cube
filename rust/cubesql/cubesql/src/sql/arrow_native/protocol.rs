use crate::CubeError;
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Protocol version
pub const PROTOCOL_VERSION: u32 = 1;

/// Message types for the Arrow Native Protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    HandshakeRequest = 0x01,
    HandshakeResponse = 0x02,
    AuthRequest = 0x03,
    AuthResponse = 0x04,
    QueryRequest = 0x10,
    QueryResponseSchema = 0x11,
    QueryResponseBatch = 0x12,
    QueryComplete = 0x13,
    Error = 0xFF,
}

impl MessageType {
    pub fn from_u8(value: u8) -> Result<Self, CubeError> {
        match value {
            0x01 => Ok(MessageType::HandshakeRequest),
            0x02 => Ok(MessageType::HandshakeResponse),
            0x03 => Ok(MessageType::AuthRequest),
            0x04 => Ok(MessageType::AuthResponse),
            0x10 => Ok(MessageType::QueryRequest),
            0x11 => Ok(MessageType::QueryResponseSchema),
            0x12 => Ok(MessageType::QueryResponseBatch),
            0x13 => Ok(MessageType::QueryComplete),
            0xFF => Ok(MessageType::Error),
            _ => Err(CubeError::internal(format!(
                "Unknown message type: 0x{:02x}",
                value
            ))),
        }
    }
}

/// Protocol message
#[derive(Debug)]
pub enum Message {
    HandshakeRequest {
        version: u32,
    },
    HandshakeResponse {
        version: u32,
        server_version: String,
    },
    AuthRequest {
        token: String,
        database: Option<String>,
    },
    AuthResponse {
        success: bool,
        session_id: String,
    },
    QueryRequest {
        sql: String,
    },
    QueryResponseSchema {
        arrow_ipc_schema: Vec<u8>,
    },
    QueryResponseBatch {
        arrow_ipc_batch: Vec<u8>,
    },
    QueryComplete {
        rows_affected: i64,
    },
    Error {
        code: String,
        message: String,
    },
}

impl Message {
    /// Encode message to bytes
    pub fn encode(&self) -> Result<Vec<u8>, CubeError> {
        let mut buf = BytesMut::new();

        match self {
            Message::HandshakeRequest { version } => {
                buf.put_u8(MessageType::HandshakeRequest as u8);
                buf.put_u32(*version);
            }
            Message::HandshakeResponse {
                version,
                server_version,
            } => {
                buf.put_u8(MessageType::HandshakeResponse as u8);
                buf.put_u32(*version);
                Self::put_string(&mut buf, server_version);
            }
            Message::AuthRequest { token, database } => {
                buf.put_u8(MessageType::AuthRequest as u8);
                Self::put_string(&mut buf, token);
                Self::put_optional_string(&mut buf, database.as_deref());
            }
            Message::AuthResponse {
                success,
                session_id,
            } => {
                buf.put_u8(MessageType::AuthResponse as u8);
                buf.put_u8(if *success { 1 } else { 0 });
                Self::put_string(&mut buf, session_id);
            }
            Message::QueryRequest { sql } => {
                buf.put_u8(MessageType::QueryRequest as u8);
                Self::put_string(&mut buf, sql);
            }
            Message::QueryResponseSchema { arrow_ipc_schema } => {
                buf.put_u8(MessageType::QueryResponseSchema as u8);
                Self::put_bytes(&mut buf, arrow_ipc_schema);
            }
            Message::QueryResponseBatch { arrow_ipc_batch } => {
                buf.put_u8(MessageType::QueryResponseBatch as u8);
                Self::put_bytes(&mut buf, arrow_ipc_batch);
            }
            Message::QueryComplete { rows_affected } => {
                buf.put_u8(MessageType::QueryComplete as u8);
                buf.put_i64(*rows_affected);
            }
            Message::Error { code, message } => {
                buf.put_u8(MessageType::Error as u8);
                Self::put_string(&mut buf, code);
                Self::put_string(&mut buf, message);
            }
        }

        // Prepend length (excluding the length field itself)
        let payload_len = buf.len() as u32;
        let mut result = BytesMut::with_capacity(4 + buf.len());
        result.put_u32(payload_len);
        result.put(buf);

        Ok(result.to_vec())
    }

    /// Decode message from bytes
    pub fn decode(data: &[u8]) -> Result<Self, CubeError> {
        if data.is_empty() {
            return Err(CubeError::internal("Empty message data".to_string()));
        }

        let mut cursor = Cursor::new(data);
        let msg_type = MessageType::from_u8(cursor.get_u8())?;

        match msg_type {
            MessageType::HandshakeRequest => {
                let version = cursor.get_u32();
                Ok(Message::HandshakeRequest { version })
            }
            MessageType::HandshakeResponse => {
                let version = cursor.get_u32();
                let server_version = Self::get_string(&mut cursor)?;
                Ok(Message::HandshakeResponse {
                    version,
                    server_version,
                })
            }
            MessageType::AuthRequest => {
                let token = Self::get_string(&mut cursor)?;
                let database = Self::get_optional_string(&mut cursor)?;
                Ok(Message::AuthRequest { token, database })
            }
            MessageType::AuthResponse => {
                let success = cursor.get_u8() != 0;
                let session_id = Self::get_string(&mut cursor)?;
                Ok(Message::AuthResponse {
                    success,
                    session_id,
                })
            }
            MessageType::QueryRequest => {
                let sql = Self::get_string(&mut cursor)?;
                Ok(Message::QueryRequest { sql })
            }
            MessageType::QueryResponseSchema => {
                let arrow_ipc_schema = Self::get_bytes(&mut cursor)?;
                Ok(Message::QueryResponseSchema { arrow_ipc_schema })
            }
            MessageType::QueryResponseBatch => {
                let arrow_ipc_batch = Self::get_bytes(&mut cursor)?;
                Ok(Message::QueryResponseBatch { arrow_ipc_batch })
            }
            MessageType::QueryComplete => {
                let rows_affected = cursor.get_i64();
                Ok(Message::QueryComplete { rows_affected })
            }
            MessageType::Error => {
                let code = Self::get_string(&mut cursor)?;
                let message = Self::get_string(&mut cursor)?;
                Ok(Message::Error { code, message })
            }
        }
    }

    // Helper methods for encoding/decoding strings and bytes
    fn put_string(buf: &mut BytesMut, s: &str) {
        let bytes = s.as_bytes();
        buf.put_u32(bytes.len() as u32);
        buf.put(bytes);
    }

    fn put_optional_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(s) => {
                buf.put_u8(1);
                Self::put_string(buf, s);
            }
            None => {
                buf.put_u8(0);
            }
        }
    }

    fn put_bytes(buf: &mut BytesMut, bytes: &[u8]) {
        buf.put_u32(bytes.len() as u32);
        buf.put(bytes);
    }

    fn get_string(cursor: &mut Cursor<&[u8]>) -> Result<String, CubeError> {
        let len = cursor.get_u32() as usize;
        let pos = cursor.position() as usize;
        let data = cursor.get_ref();

        if pos + len > data.len() {
            return Err(CubeError::internal(
                "Insufficient data for string".to_string(),
            ));
        }

        let s = String::from_utf8(data[pos..pos + len].to_vec())
            .map_err(|e| CubeError::internal(format!("Invalid UTF-8 string: {}", e)))?;

        cursor.set_position((pos + len) as u64);
        Ok(s)
    }

    fn get_optional_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, CubeError> {
        let has_value = cursor.get_u8() != 0;
        if has_value {
            Ok(Some(Self::get_string(cursor)?))
        } else {
            Ok(None)
        }
    }

    fn get_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, CubeError> {
        let len = cursor.get_u32() as usize;
        let pos = cursor.position() as usize;
        let data = cursor.get_ref();

        if pos + len > data.len() {
            return Err(CubeError::internal(
                "Insufficient data for bytes".to_string(),
            ));
        }

        let bytes = data[pos..pos + len].to_vec();
        cursor.set_position((pos + len) as u64);
        Ok(bytes)
    }
}

/// Read a message from an async stream
pub async fn read_message<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Message, CubeError> {
    // Read length prefix
    let len = reader
        .read_u32()
        .await
        .map_err(|e| CubeError::internal(format!("Failed to read message length: {}", e)))?;

    if len == 0 {
        return Err(CubeError::internal("Invalid message length: 0".to_string()));
    }

    if len > 100 * 1024 * 1024 {
        // 100MB max message size
        return Err(CubeError::internal(format!(
            "Message too large: {} bytes",
            len
        )));
    }

    // Read payload
    let mut payload = vec![0u8; len as usize];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| CubeError::internal(format!("Failed to read message payload: {}", e)))?;

    // Decode message
    Message::decode(&payload)
}

/// Write a message to an async stream
pub async fn write_message<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    message: &Message,
) -> Result<(), CubeError> {
    let encoded = message.encode()?;
    writer
        .write_all(&encoded)
        .await
        .map_err(|e| CubeError::internal(format!("Failed to write message: {}", e)))?;
    writer
        .flush()
        .await
        .map_err(|e| CubeError::internal(format!("Failed to flush message: {}", e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_request_roundtrip() {
        let msg = Message::HandshakeRequest { version: 1 };
        let encoded = msg.encode().unwrap();
        let decoded = Message::decode(&encoded[4..]).unwrap();

        match decoded {
            Message::HandshakeRequest { version } => assert_eq!(version, 1),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_query_request_roundtrip() {
        let msg = Message::QueryRequest {
            sql: "SELECT * FROM table".to_string(),
        };
        let encoded = msg.encode().unwrap();
        let decoded = Message::decode(&encoded[4..]).unwrap();

        match decoded {
            Message::QueryRequest { sql } => assert_eq!(sql, "SELECT * FROM table"),
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_auth_request_with_database() {
        let msg = Message::AuthRequest {
            token: "secret_token".to_string(),
            database: Some("my_db".to_string()),
        };
        let encoded = msg.encode().unwrap();
        let decoded = Message::decode(&encoded[4..]).unwrap();

        match decoded {
            Message::AuthRequest { token, database } => {
                assert_eq!(token, "secret_token");
                assert_eq!(database, Some("my_db".to_string()));
            }
            _ => panic!("Wrong message type"),
        }
    }

    #[test]
    fn test_error_message() {
        let msg = Message::Error {
            code: "INTERNAL_ERROR".to_string(),
            message: "Something went wrong".to_string(),
        };
        let encoded = msg.encode().unwrap();
        let decoded = Message::decode(&encoded[4..]).unwrap();

        match decoded {
            Message::Error { code, message } => {
                assert_eq!(code, "INTERNAL_ERROR");
                assert_eq!(message, "Something went wrong");
            }
            _ => panic!("Wrong message type"),
        }
    }
}
