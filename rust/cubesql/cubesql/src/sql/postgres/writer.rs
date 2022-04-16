use crate::sql::protocol::{Format, Serialize};

use bytes::{BufMut, BytesMut};
use std::convert::TryFrom;
use std::io;
use std::mem;

pub trait ToPostgresValue {
    // Converts native type to raw value in text format
    fn to_text(&self, buf: &mut BytesMut) -> io::Result<()>;

    // Converts native type to raw value in binary format
    fn to_binary(&self, buf: &mut BytesMut) -> io::Result<()>;
}

impl ToPostgresValue for String {
    fn to_text(&self, buf: &mut BytesMut) -> io::Result<()> {
        buf.put_i32(self.len() as i32);
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }

    fn to_binary(&self, buf: &mut BytesMut) -> io::Result<()> {
        buf.put_i32(self.len() as i32);
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }
}

impl ToPostgresValue for bool {
    fn to_text(&self, buf: &mut BytesMut) -> io::Result<()> {
        if *self {
            "t".to_string().to_text(buf)
        } else {
            "v".to_string().to_text(buf)
        }
    }

    fn to_binary(&self, buf: &mut BytesMut) -> io::Result<()> {
        buf.extend_from_slice(&1_u32.to_be_bytes());
        buf.extend_from_slice(if *self { &[1] } else { &[0] });

        Ok(())
    }
}

impl<T: ToPostgresValue> ToPostgresValue for Option<T> {
    fn to_text(&self, buf: &mut BytesMut) -> io::Result<()> {
        match &self {
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(v) => v.to_text(buf)?,
        };

        Ok(())
    }

    fn to_binary(&self, buf: &mut BytesMut) -> io::Result<()> {
        match &self {
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(v) => v.to_binary(buf)?,
        };

        Ok(())
    }
}

macro_rules! impl_primitive {
    ($type: ident) => {
        impl ToPostgresValue for $type {
            fn to_text(&self, buf: &mut BytesMut) -> io::Result<()> {
                self.to_string().to_text(buf)
            }

            fn to_binary(&self, buf: &mut BytesMut) -> io::Result<()> {
                buf.extend_from_slice(&(mem::size_of::<$type>() as u32).to_be_bytes());
                buf.extend_from_slice(&self.to_be_bytes());

                Ok(())
            }
        }
    };
}

impl_primitive!(i32);
impl_primitive!(i64);
impl_primitive!(f32);
impl_primitive!(f64);

pub struct BatchWriter {
    format: Format,
    // Data of whole rows
    data: BytesMut,
    // Current row
    current: u32,
    row: BytesMut,
}

impl BatchWriter {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            data: BytesMut::new(),
            row: BytesMut::new(),
            current: 0,
        }
    }

    pub fn write_value<T: ToPostgresValue>(&mut self, value: T) -> io::Result<()> {
        self.current += 1;

        match self.format {
            Format::Text => value.to_text(&mut self.row)?,
            Format::Binary => value.to_binary(&mut self.row)?,
        };

        Ok(())
    }

    pub fn end_row(&mut self) -> io::Result<()> {
        self.data.extend_from_slice(&b'D'.to_be_bytes());
        let buffer = self.row.split();

        self.data.put_i32(buffer.len() as i32 + 4 + 2);

        let fields_count = u16::try_from(self.current).unwrap();
        self.data.extend_from_slice(&fields_count.to_be_bytes());

        self.data.extend(buffer);
        self.current = 0;

        Ok(())
    }
}

impl<'a> Serialize for BatchWriter {
    const CODE: u8 = b'D';

    fn serialize(&self) -> Option<Vec<u8>> {
        let mut r = vec![];
        r.extend_from_slice(&self.data[..]);

        Some(r)
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::buffer;
    use crate::sql::protocol::Format;
    use crate::sql::writer::BatchWriter;
    use crate::CubeError;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_backend_writer_text() -> Result<(), CubeError> {
        let mut cursor = Cursor::new(vec![]);

        let mut writer = BatchWriter::new(Format::Text);
        writer.write_value("test1".to_string())?;
        writer.write_value(true)?;
        writer.end_row()?;

        writer.write_value("test2".to_string())?;
        writer.write_value(true)?;
        writer.end_row()?;

        buffer::write_direct(&mut cursor, writer).await?;

        assert_eq!(
            cursor.get_ref()[0..],
            vec![
                // row
                68, 0, 0, 0, 20, 0, 2, 0, 0, 0, 5, 116, 101, 115, 116, 49, 0, 0, 0, 1, 116,
                // row
                68, 0, 0, 0, 20, 0, 2, 0, 0, 0, 5, 116, 101, 115, 116, 50, 0, 0, 0, 1, 116
            ]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_backend_writer_binary() -> Result<(), CubeError> {
        let mut cursor = Cursor::new(vec![]);

        let mut writer = BatchWriter::new(Format::Binary);
        writer.write_value("test1".to_string())?;
        writer.write_value(true)?;
        writer.end_row()?;

        writer.write_value("test2".to_string())?;
        writer.write_value(true)?;
        writer.end_row()?;

        buffer::write_direct(&mut cursor, writer).await?;

        assert_eq!(
            cursor.get_ref()[0..],
            vec![
                // row
                68, 0, 0, 0, 20, 0, 2, 0, 0, 0, 5, 116, 101, 115, 116, 49, 0, 0, 0, 1, 1,
                // row
                68, 0, 0, 0, 20, 0, 2, 0, 0, 0, 5, 116, 101, 115, 116, 50, 0, 0, 0, 1, 1
            ]
        );

        Ok(())
    }
}
