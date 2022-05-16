use crate::{
    arrow::{
        array::{
            ArrayRef, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array,
            Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array, UInt64Array,
            UInt8Array,
        },
        datatypes::DataType,
    },
    sql::{dataframe::TimestampValue, df_type_to_pg_tid},
};
use bytes::{BufMut, BytesMut};
use chrono::{
    format::{
        Fixed, Item,
        Numeric::{Day, Hour, Minute, Month, Second, Year},
        Pad::Zero,
    },
    prelude::*,
};
use pg_srv::{
    protocol,
    protocol::{Format, Serialize},
    ProtocolError,
};
use std::{convert::TryFrom, io, io::Error, mem};

pub trait ToPostgresValue {
    // Converts native type to raw value in text format
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>;

    // Converts native type to raw value in binary format
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>;
}

impl ToPostgresValue for String {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_i32(self.len() as i32);
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_i32(self.len() as i32);
        buf.extend_from_slice(self.as_bytes());

        Ok(())
    }
}

impl ToPostgresValue for bool {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        if *self {
            "t".to_string().to_text(buf)
        } else {
            "f".to_string().to_text(buf)
        }
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        buf.put_i32(1_i32);
        buf.extend_from_slice(if *self { &[1] } else { &[0] });

        Ok(())
    }
}

// POSTGRES_EPOCH_JDATE
fn pg_base_date_epoch() -> NaiveDateTime {
    NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)
}

impl ToPostgresValue for TimestampValue {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let ndt = match self.tz_ref() {
            None => self.to_naive_datetime(),
            Some(_) => self.to_fixed_datetime()?.naive_utc(),
        };

        // 2022-04-25 15:36:49.39705+00
        let as_str = ndt
            .format_with_items(
                [
                    Item::Numeric(Year, Zero),
                    Item::Literal("-"),
                    Item::Numeric(Month, Zero),
                    Item::Literal("-"),
                    Item::Numeric(Day, Zero),
                    Item::Literal(" "),
                    Item::Numeric(Hour, Zero),
                    Item::Literal(":"),
                    Item::Numeric(Minute, Zero),
                    Item::Literal(":"),
                    Item::Numeric(Second, Zero),
                    Item::Fixed(Fixed::Nanosecond6),
                ]
                .iter(),
            )
            .to_string();

        match self.tz_ref() {
            None => as_str.to_text(buf),
            Some(_) => (as_str + &"+00").to_text(buf),
        }
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        match self.tz_ref() {
            None => {
                let seconds = self
                    .to_naive_datetime()
                    .signed_duration_since(pg_base_date_epoch())
                    .num_microseconds()
                    .ok_or(Error::new(
                        io::ErrorKind::Other,
                        "Unable to extract number of seconds from timestamp",
                    ))?;

                buf.put_i32(8_i32);
                buf.put_i64(seconds)
            }
            Some(tz) => {
                let seconds = self
                    .to_fixed_datetime()?
                    .naive_utc()
                    .signed_duration_since(pg_base_date_epoch())
                    .num_microseconds()
                    .ok_or(Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "Unable to extract number of seconds from timestamp with tz: {}",
                            tz
                        ),
                    ))?;

                buf.put_i32(8_i32);
                buf.put_i64(seconds)
            }
        };

        Ok(())
    }
}

impl<T: ToPostgresValue> ToPostgresValue for Option<T> {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        match &self {
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(v) => v.to_text(buf)?,
        };

        Ok(())
    }

    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        match &self {
            None => buf.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(v) => v.to_binary(buf)?,
        };

        Ok(())
    }
}

impl ToPostgresValue for ArrayRef {
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
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
                return Err(protocol::ErrorResponse::error(
                    protocol::ErrorCode::InternalError,
                    format!("Unsupported type for list serializing: {}", dt),
                )
                .into());
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
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
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
                return Err(protocol::ErrorResponse::error(
                    protocol::ErrorCode::InternalError,
                    format!("Unsupported type for list serializing: {}", dt),
                )
                .into())
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
            fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
                self.to_string().to_text(buf)
            }

            fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
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
    rows: u32,
    row: BytesMut,
}

impl BatchWriter {
    pub fn new(format: Format) -> Self {
        Self {
            format,
            data: BytesMut::new(),
            row: BytesMut::new(),
            current: 0,
            rows: 0,
        }
    }

    pub fn write_value<T: ToPostgresValue>(&mut self, value: T) -> Result<(), ProtocolError> {
        self.current += 1;

        match self.format {
            Format::Text => value.to_text(&mut self.row)?,
            Format::Binary => value.to_binary(&mut self.row)?,
        };

        Ok(())
    }

    pub fn end_row(&mut self) -> Result<(), ProtocolError> {
        self.data.extend_from_slice(&b'D'.to_be_bytes());
        let buffer = self.row.split();

        self.data.put_i32(buffer.len() as i32 + 4 + 2);

        let fields_count = u16::try_from(self.current).unwrap();
        self.data.extend_from_slice(&fields_count.to_be_bytes());

        self.data.extend(buffer);
        self.current = 0;
        self.rows += 1;

        Ok(())
    }

    pub fn num_rows(&self) -> u32 {
        self.rows
    }

    pub fn has_data(&self) -> bool {
        self.rows > 0
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
    use crate::{
        arrow::array::{ArrayRef, Int64Builder},
        sql::{
            dataframe::TimestampValue,
            shim::ConnectionError,
            writer::{BatchWriter, ToPostgresValue},
        },
    };
    use bytes::BytesMut;
    use pg_srv::{buffer, protocol::Format};
    use std::{io::Cursor, sync::Arc};

    fn assert_text_encode<T: ToPostgresValue>(value: T, expected: &[u8]) {
        let mut buf = BytesMut::new();
        value.to_text(&mut buf).unwrap();

        assert_eq!(&buf.as_ref()[..], expected);
    }

    #[test]
    fn test_text_encoders() -> Result<(), ConnectionError> {
        assert_text_encode(true, &[0, 0, 0, 1, 116]);
        assert_text_encode(false, &[0, 0, 0, 1, 102]);
        assert_text_encode("str".to_string(), &[0, 0, 0, 3, 115, 116, 114]);
        assert_text_encode(
            TimestampValue::new(1650890322, None),
            &[
                0, 0, 0, 26, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58,
                48, 49, 46, 54, 53, 48, 56, 57, 48,
            ],
        );
        assert_text_encode(
            TimestampValue::new(1650890322, Some("UTC".to_string())),
            &[
                0, 0, 0, 29, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58,
                48, 49, 46, 54, 53, 48, 56, 57, 48, 43, 48, 48,
            ],
        );

        Ok(())
    }

    fn assert_bind_encode<T: ToPostgresValue>(value: T, expected: &[u8]) {
        let mut buf = BytesMut::new();
        value.to_binary(&mut buf).unwrap();

        assert_eq!(&buf.as_ref()[..], expected);
    }

    #[test]
    fn test_binary_encoders() -> Result<(), ConnectionError> {
        assert_bind_encode(true, &[0, 0, 0, 1, 1]);
        assert_bind_encode(false, &[0, 0, 0, 1, 0]);
        assert_bind_encode(
            TimestampValue::new(1650890322, None),
            &[0, 0, 0, 8, 255, 252, 162, 254, 196, 225, 80, 202],
        );
        assert_bind_encode(
            TimestampValue::new(1650890322, Some("UTC".to_string())),
            &[0, 0, 0, 8, 255, 252, 162, 254, 196, 225, 80, 202],
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_backend_writer_text_simple() -> Result<(), ConnectionError> {
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
    async fn test_backend_writer_binary_simple() -> Result<(), ConnectionError> {
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
    async fn test_backend_writer_binary_int8_array() -> Result<(), ConnectionError> {
        let mut cursor = Cursor::new(vec![]);
        let mut writer = BatchWriter::new(Format::Binary);

        // Row 1
        let mut col = Int64Builder::new(3);
        col.append_value(1).unwrap();
        col.append_value(2).unwrap();
        col.append_value(3).unwrap();

        writer.write_value(Arc::new(col.finish()) as ArrayRef)?;
        writer.end_row()?;

        // Row 2
        let mut col = Int64Builder::new(3);
        col.append_null().unwrap();
        col.append_value(2).unwrap();
        col.append_null().unwrap();

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
