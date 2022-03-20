use std::{
    convert::TryFrom,
    io::{Cursor, Error, ErrorKind},
    marker::Send,
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::protocol::{self, Deserialize, FrontendMessage, Serialize};

pub async fn read_message<Reader: AsyncReadExt + Unpin + Send>(
    reader: &mut Reader,
) -> Result<FrontendMessage, Error> {
    // https://www.postgresql.org/docs/14/protocol-message-formats.html
    Ok(match reader.read_u8().await? {
        b'Q' => FrontendMessage::Query(protocol::Query::read_from(reader).await?),
        b'P' => FrontendMessage::Parse(protocol::Parse::read_from(reader).await?),
        b'B' => FrontendMessage::Bind(protocol::Bind::read_from(reader).await?),
        b'D' => FrontendMessage::Describe(protocol::Describe::read_from(reader).await?),
        b'p' => {
            FrontendMessage::PasswordMessage(protocol::PasswordMessage::read_from(reader).await?)
        }
        b'X' => FrontendMessage::Terminate,
        b'S' => FrontendMessage::Sync,
        identifier => {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unknown message identifier: {}", identifier),
            ))
        }
    })
}

pub async fn read_contents<Reader: AsyncReadExt + Unpin>(
    reader: &mut Reader,
) -> Result<Cursor<Vec<u8>>, Error> {
    let length = reader.read_u32().await?;
    if length < 4 {
        return Err(Error::new(
            ErrorKind::Other,
            "Unexpectedly small (<0) message size",
        ));
    }
    let length = usize::try_from(length - 4).map_err(|_| {
        Error::new(
            ErrorKind::OutOfMemory,
            "Unable to convert message length to a suitable memory size",
        )
    })?;
    let mut buffer = vec![0; length];
    reader.read_exact(&mut buffer).await?;
    let cursor = Cursor::new(buffer);
    Ok(cursor)
}

pub async fn read_string<Reader: AsyncReadExt + Unpin>(
    reader: &mut Reader,
) -> Result<String, Error> {
    let mut bytes = Vec::with_capacity(64);
    loop {
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
) -> Result<protocol::Format, Error> {
    let format = match reader.read_i16().await? {
        0 => protocol::Format::Text,
        1 => protocol::Format::Binary,
        format_code => {
            return Err(Error::new(
                std::io::ErrorKind::Unsupported,
                format!("Unknown format code: {}", format_code),
            ));
        }
    };

    Ok(format)
}

pub async fn write_message<Writer: AsyncWriteExt + Unpin, Message: Serialize>(
    writer: &mut Writer,
    message: Message,
) -> Result<(), Error> {
    let mut packet_buffer = Vec::with_capacity(64);
    packet_buffer.push(message.code());
    match message.serialize() {
        Some(buffer) => {
            let size = u32::try_from(buffer.len() + 4).map_err(|_| {
                Error::new(
                    ErrorKind::OutOfMemory,
                    "Unable to convert buffer length to a suitable memory size",
                )
            })?;
            packet_buffer.extend_from_slice(&size.to_be_bytes());
            packet_buffer.extend_from_slice(&buffer);
        }
        _ => (),
    };
    writer.write_all(&packet_buffer).await?;
    writer.flush().await?;
    Ok(())
}

pub fn write_string(buffer: &mut Vec<u8>, string: &str) {
    buffer.extend_from_slice(string.as_bytes());
    buffer.push(0);
}
