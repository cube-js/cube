use crate::sql::protocol::{Format, Serialize};

use crate::arrow::array::{
    ArrayRef, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use crate::arrow::datatypes::DataType;
use crate::sql::df_type_to_pg_tid;
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

impl ToPostgresValue for ArrayRef {
    fn to_text(&self, buf: &mut BytesMut) -> io::Result<()> {
        let mut values: Vec<String> = Vec::with_capacity(self.len());

        macro_rules! write_native_array_to_buffer {
            ($ARRAY:expr, $BUFF:expr, $ARRAY_TYPE: ident) => {{
                let arr = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

                for i in 0..$ARRAY.len() {
                    if self.is_null(i) {
                        $BUFF.push("null".to_string());
                    } else {
                        $BUFF.push(arr.value(i).to_string());
                    }
                }
            }};
        }

        match self.data_type() {
            DataType::Float16 => write_native_array_to_buffer!(self, values, Float16Array),
            DataType::Float32 => write_native_array_to_buffer!(self, values, Float32Array),
            DataType::Float64 => write_native_array_to_buffer!(self, values, Float64Array),
            // PG doesnt support i8, casting to i16
            DataType::Int8 => write_native_array_to_buffer!(self, values, Int8Array),
            DataType::Int16 => write_native_array_to_buffer!(self, values, Int16Array),
            DataType::Int32 => write_native_array_to_buffer!(self, values, Int32Array),
            DataType::Int64 => write_native_array_to_buffer!(self, values, Int64Array),
            // PG doesnt support i8, casting to i16
            DataType::UInt8 => write_native_array_to_buffer!(self, values, UInt8Array),
            DataType::UInt16 => write_native_array_to_buffer!(self, values, UInt16Array),
            DataType::UInt32 => write_native_array_to_buffer!(self, values, UInt32Array),
            DataType::UInt64 => write_native_array_to_buffer!(self, values, UInt64Array),
            DataType::Boolean => write_native_array_to_buffer!(self, values, BooleanArray),
            DataType::Utf8 => write_native_array_to_buffer!(self, values, StringArray),
            dt => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Unsupported type for list serializing: {}", dt),
                ))
            }
        };

        ("{".to_string() + &values.join(",") + "}").to_text(buf)
    }

    // Example for ARRAY[1,2,3]::int8[]
    // 0000   00 00 00 01 00 00 00 00 00 00 00 14 00 00 00 03   ................
    // 0010   00 00 00 01 00 00 00 08 00 00 00 00 00 00 00 01   ................
    // 0020   00 00 00 08 00 00 00 00 00 00 00 02 00 00 00 08   ................
    // 0030   00 00 00 00 00 00 00 03                           ........
    //
    // Example for ARRAY['test1', 'test2']
    // 0000   00 00 00 01 00 00 00 00 00 00 00 19 00 00 00 02   ................
    // 0010   00 00 00 01 00 00 00 05 74 65 73 74 31 00 00 00   ........test1...
    // 0020   05 74 65 73 74 32                                 .test2
    fn to_binary(&self, buf: &mut BytesMut) -> io::Result<()> {
        let mut column_data = BytesMut::with_capacity(3 * 5 + self.len() * 2);
        // row1 from the comment
        // dimensions
        column_data.put_i32(1);
        // has_nulls
        column_data.put_i32((self.null_count() > 0) as i32);
        column_data.put_u32(df_type_to_pg_tid(self.data_type())? as u32);
        column_data.put_u32(self.len() as u32);

        // row2 from the comment
        column_data.put_u32(1);

        macro_rules! write_native_array_as_binary {
            ($ARRAY:expr, $ARRAY_TYPE: ident, $NATIVE: tt) => {{
                let arr = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

                for i in 0..$ARRAY.len() {
                    if self.is_null(i) {
                        let n: Option<$NATIVE> = None;
                        n.to_binary(&mut column_data)?
                    } else {
                        (arr.value(i) as $NATIVE).to_binary(&mut column_data)?
                    }
                }
            }};
        }

        match self.data_type() {
            // DataType::Float16 => write_native_array_as_binary!(self, Float16Array, f32),
            DataType::Float32 => write_native_array_as_binary!(self, Float32Array, f32),
            DataType::Float64 => write_native_array_as_binary!(self, Float64Array, f64),
            // PG doesnt support i8, casting to i16
            DataType::Int8 => write_native_array_as_binary!(self, Int8Array, i16),
            DataType::Int16 => write_native_array_as_binary!(self, Int16Array, i16),
            DataType::Int32 => write_native_array_as_binary!(self, Int32Array, i32),
            DataType::Int64 => write_native_array_as_binary!(self, Int64Array, i64),
            // PG doesnt support i8, casting to i16
            DataType::UInt8 => write_native_array_as_binary!(self, UInt8Array, i16),
            DataType::UInt16 => write_native_array_as_binary!(self, UInt16Array, i16),
            DataType::UInt32 => write_native_array_as_binary!(self, UInt32Array, i32),
            DataType::UInt64 => write_native_array_as_binary!(self, UInt64Array, i64),
            DataType::Boolean => write_native_array_as_binary!(self, BooleanArray, bool),
            DataType::Utf8 => {
                let arr = self.as_any().downcast_ref::<StringArray>().unwrap();

                for i in 0..self.len() {
                    if self.is_null(i) {
                        "null".to_string().to_binary(&mut column_data)?
                    } else {
                        arr.value(i).to_string().to_binary(&mut column_data)?
                    }
                }
            }
            dt => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Unsupported type for list serializing: {}", dt),
                ))
            }
        };

        buf.put_i32(column_data.len() as i32);
        buf.extend_from_slice(&column_data[..]);

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

impl_primitive!(i16);
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
    use crate::arrow::array::{ArrayRef, Int64Builder};
    use crate::sql::buffer;
    use crate::sql::protocol::Format;
    use crate::sql::writer::BatchWriter;
    use crate::CubeError;
    use std::io::Cursor;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_backend_writer_text_simple() -> Result<(), CubeError> {
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
    async fn test_backend_writer_binary_simple() -> Result<(), CubeError> {
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

    #[tokio::test]
    async fn test_backend_writer_binary_int8_array() -> Result<(), CubeError> {
        let mut cursor = Cursor::new(vec![]);
        let mut writer = BatchWriter::new(Format::Binary);

        // Row 1
        let mut col = Int64Builder::new(3);
        col.append_value(1)?;
        col.append_value(2)?;
        col.append_value(3)?;

        writer.write_value(Arc::new(col.finish()) as ArrayRef)?;
        writer.end_row()?;

        // Row 2
        let mut col = Int64Builder::new(3);
        col.append_null()?;
        col.append_value(2)?;
        col.append_null()?;

        writer.write_value(Arc::new(col.finish()) as ArrayRef)?;
        writer.end_row()?;

        buffer::write_direct(&mut cursor, writer).await?;

        assert_eq!(
            cursor.get_ref()[0..],
            vec![
                68, 0, 0, 0, 66, 0, 1, 0, 0, 0, 56, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0,
                3, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0,
                2, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 3, 68, 0, 0, 0, 50, 0, 1, 0, 0, 0, 40, 0, 0, 0,
                1, 0, 0, 0, 1, 0, 0, 0, 20, 0, 0, 0, 3, 0, 0, 0, 1, 255, 255, 255, 255, 0, 0, 0, 8,
                0, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255
            ]
        );

        Ok(())
    }
}
