//! Helpers for reading/writing from/to the connection's socket

use async_trait::async_trait;
use bytes::{BufMut, BytesMut};
use std::{
    convert::TryFrom,
    fmt::Debug,
    io::{Cursor, Error, ErrorKind},
    marker::Send,
    sync::Arc,
};

use crate::{
    protocol::{ErrorCode, ErrorResponse},
    ProtocolError,
};
use log::trace;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::protocol::{self, Deserialize, FrontendMessage, Serialize};

#[async_trait]
pub trait MessageTagParser: Sync + Send + Debug {
    async fn parse(
        &self,
        tag: u8,
        cursor: Cursor<Vec<u8>>,
    ) -> Result<FrontendMessage, ProtocolError>;
}

#[derive(Default, Debug)]
pub struct MessageTagParserDefaultImpl {}

impl MessageTagParserDefaultImpl {
    pub fn new() -> Self {
        Self {}
    }

    pub fn with_arc() -> Arc<dyn MessageTagParser> {
        Arc::new(Self::new())
    }
}

#[async_trait]
impl MessageTagParser for MessageTagParserDefaultImpl {
    async fn parse(
        &self,
        tag: u8,
        cursor: Cursor<Vec<u8>>,
    ) -> Result<FrontendMessage, ProtocolError> {
        let message = match tag {
            b'Q' => FrontendMessage::Query(protocol::Query::deserialize(cursor).await?),
            b'P' => FrontendMessage::Parse(protocol::Parse::deserialize(cursor).await?),
            b'B' => FrontendMessage::Bind(protocol::Bind::deserialize(cursor).await?),
            b'D' => FrontendMessage::Describe(protocol::Describe::deserialize(cursor).await?),
            b'E' => FrontendMessage::Execute(protocol::Execute::deserialize(cursor).await?),
            b'C' => FrontendMessage::Close(protocol::Close::deserialize(cursor).await?),
            b'p' => FrontendMessage::PasswordMessage(
                protocol::PasswordMessage::deserialize(cursor).await?,
            ),
            b'X' => FrontendMessage::Terminate,
            b'H' => FrontendMessage::Flush,
            b'S' => FrontendMessage::Sync,
            identifier => {
                return Err(ErrorResponse::error(
                    ErrorCode::DataException,
                    format!("Unknown message identifier: {:X?}", identifier),
                )
                .into())
            }
        };
        Ok(message)
    }
}

pub async fn read_message<Reader: AsyncReadExt + Unpin + Send>(
    reader: &mut Reader,
    parser: Arc<dyn MessageTagParser>,
) -> Result<FrontendMessage, ProtocolError> {
    // https://www.postgresql.org/docs/14/protocol-message-formats.html
    let message_tag = reader.read_u8().await?;
    let cursor = read_contents(reader, message_tag).await?;
    let message = parser.parse(message_tag, cursor).await?;

    trace!("[pg] Decoded {:X?}", message,);

    Ok(message)
}

pub async fn read_contents<Reader: AsyncReadExt + Unpin>(
    reader: &mut Reader,
    message_tag: u8,
) -> Result<Cursor<Vec<u8>>, Error> {
    // protocol defines length for all types of messages
    let length = reader.read_u32().await?;
    if length < 4 {
        return Err(Error::new(
            ErrorKind::Other,
            "Unexpectedly small (<0) message size",
        ));
    }

    trace!(
        "[pg] Receive package {:X?} with length {}",
        message_tag,
        length
    );

    let length = usize::try_from(length - 4).map_err(|_| {
        Error::new(
            ErrorKind::OutOfMemory,
            "Unable to convert message length to a suitable memory size",
        )
    })?;

    let buffer = if length == 0 {
        vec![0; 0]
    } else {
        let mut buffer = vec![0; length];
        reader.read_exact(&mut buffer).await?;

        buffer
    };

    let cursor = Cursor::new(buffer);

    Ok(cursor)
}

pub async fn read_string<Reader: AsyncReadExt + Unpin>(
    reader: &mut Reader,
) -> Result<String, Error> {
    let mut bytes = Vec::with_capacity(64);

    loop {
        // PostgreSQL uses a null-terminated string (C-style string)
        let byte = reader.read_u8().await?;
        if byte == 0 {
            break;
        }

        bytes.push(byte);
    }

    let string = String::from_utf8(bytes).map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "Unable to parse bytes as a UTF-8 string",
        )
    })?;

    Ok(string)
}

pub async fn read_format<Reader: AsyncReadExt + Unpin>(
    reader: &mut Reader,
) -> Result<protocol::Format, ProtocolError> {
    match reader.read_i16().await? {
        0 => Ok(protocol::Format::Text),
        1 => Ok(protocol::Format::Binary),
        format_code => Err(protocol::ErrorResponse::error(
            protocol::ErrorCode::ProtocolViolation,
            format!("Unknown format code: {}", format_code),
        )
        .into()),
    }
}

/// Same as the write_message function, but it doesn’t append header for frame (code + size).
pub async fn write_direct<Writer: AsyncWriteExt + Unpin, Message: Serialize>(
    partial_write: &mut BytesMut,
    writer: &mut Writer,
    message: Message,
) -> Result<(), ProtocolError> {
    let mut bytes_mut = BytesMut::new();
    if let Some(buffer) = message.serialize() {
        // TODO: Yet another memory copy.
        bytes_mut.extend_from_slice(&buffer);
        *partial_write = bytes_mut;
        writer.write_all_buf(partial_write).await?;
        *partial_write = BytesMut::new();
        writer.flush().await?;
    }

    Ok(())
}

fn message_serialize<Message: Serialize>(
    message: Message,
    packet_buffer: &mut BytesMut,
) -> Result<(), ProtocolError> {
    if message.code() != 0x00 {
        packet_buffer.put_u8(message.code());
    }

    if let Some(buffer) = message.serialize() {
        let size = u32::try_from(buffer.len() + 4).map_err(|_| {
            ErrorResponse::error(
                ErrorCode::InternalError,
                "Unable to convert buffer length to a suitable memory size".to_string(),
            )
        })?;
        packet_buffer.extend_from_slice(&size.to_be_bytes());
        packet_buffer.extend_from_slice(&buffer);
    }

    Ok(())
}

/// Write multiple F messages with frame's headers to the writer.  The variable
/// `*partial_write` is set for graceful shutdown attempts with partial writes.
/// Upon a successful write, it is left empty.
pub async fn write_messages<Writer: AsyncWriteExt + Unpin, Message: Serialize>(
    partial_write: &mut BytesMut,
    writer: &mut Writer,
    messages: Vec<Message>,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(64 * messages.len());

    for message in messages {
        message_serialize(message, &mut buffer)?;
    }

    // For simplicity we obviously don't save message boundary data with
    // `*partial_write`, which means that a AdminShutdown fatal error message
    // would have to be written after _all_ these messages.
    *partial_write = buffer;
    writer.write_all_buf(partial_write).await?;
    *partial_write = BytesMut::new();

    // (We _could_ reuse the buffer in *partial_write, doing fewer allocations -- after
    // making other serialization logic allocate less and thinking about memory usage.)

    writer.flush().await?;
    Ok(())
}

/// Write single F message with frame's headers to the writer.  As with the
/// function `write_messages`, `*partial_write` is set for graceful shutdown
/// attempts with partial writes.  Upon a successful write, it is left empty.
pub async fn write_message<Writer: AsyncWriteExt + Unpin, Message: Serialize>(
    partial_write: &mut BytesMut,
    writer: &mut Writer,
    message: Message,
) -> Result<(), ProtocolError> {
    let mut buffer = BytesMut::with_capacity(64);
    message_serialize(message, &mut buffer)?;

    *partial_write = buffer;
    writer.write_all_buf(partial_write).await?;
    *partial_write = BytesMut::new();
    writer.flush().await?;
    Ok(())
}

pub fn write_string(buffer: &mut Vec<u8>, string: &str) {
    buffer.extend_from_slice(string.as_bytes());
    buffer.push(0);
}
