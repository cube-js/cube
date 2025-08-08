//! Encoding native values to the Protocol representation

use crate::{protocol::Format, ProtocolError};
use bytes::{BufMut, BytesMut};

/// This trait explains how to encode values to the protocol format
pub trait ToProtocolValue: std::fmt::Debug {
    // Converts raw value to native type in specific format
    fn to_protocol(&self, buf: &mut BytesMut, format: Format) -> Result<(), ProtocolError>
    where
        Self: Sized,
    {
        match format {
            Format::Text => self.to_text(buf),
            Format::Binary => self.to_binary(buf),
        }
    }

    /// Converts native type to raw value in text format
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>
    where
        Self: Sized;

    /// Converts native type to raw value in binary format
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>
    where
        Self: Sized;
}

impl ToProtocolValue for String {
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

impl ToProtocolValue for bool {
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

impl<T: ToProtocolValue> ToProtocolValue for Option<T> {
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

macro_rules! impl_primitive {
    ($type: ident) => {
        impl ToProtocolValue for $type {
            fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
                self.to_string().to_text(buf)
            }

            fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
                buf.extend_from_slice(&(std::mem::size_of::<$type>() as u32).to_be_bytes());
                buf.extend_from_slice(&self.to_be_bytes());

                Ok(())
            }
        }
    };
}

impl_primitive!(i8);
impl_primitive!(i16);
impl_primitive!(i32);
impl_primitive!(i64);
impl_primitive!(f32);
impl_primitive!(f64);

#[cfg(test)]
mod tests {
    use crate::*;
    use bytes::BytesMut;
    #[cfg(feature = "with-chrono")]
    use chrono::NaiveDate;

    fn assert_text_encode<T: ToProtocolValue>(value: T, expected: &[u8]) {
        let mut buf = BytesMut::new();
        value.to_text(&mut buf).unwrap();

        assert_eq!(buf.as_ref(), expected);
    }

    #[test]
    fn test_text_encoders() -> Result<(), ProtocolError> {
        assert_text_encode(true, &[0, 0, 0, 1, 116]);
        assert_text_encode(false, &[0, 0, 0, 1, 102]);
        assert_text_encode("str".to_string(), &[0, 0, 0, 3, 115, 116, 114]);
        assert_text_encode(
            IntervalValue::new(0, 0, 0, 0, 0, 0),
            &[
                0, 0, 0, 46, 48, 32, 121, 101, 97, 114, 115, 32, 48, 32, 109, 111, 110, 115, 32,
                48, 32, 100, 97, 121, 115, 32, 48, 32, 104, 111, 117, 114, 115, 32, 48, 32, 109,
                105, 110, 115, 32, 48, 46, 48, 48, 32, 115, 101, 99, 115,
            ],
        );
        assert_text_encode(
            IntervalValue::new(1, 2, 3, 4, 5, 6),
            &[
                0, 0, 0, 50, 48, 32, 121, 101, 97, 114, 115, 32, 49, 32, 109, 111, 110, 115, 32,
                50, 32, 100, 97, 121, 115, 32, 51, 32, 104, 111, 117, 114, 115, 32, 52, 32, 109,
                105, 110, 115, 32, 53, 46, 48, 48, 48, 48, 48, 54, 32, 115, 101, 99, 115,
            ],
        );

        #[cfg(feature = "with-chrono")]
        {
            // Test TimestampValue encoding
            assert_text_encode(
                TimestampValue::new(0, None),
                &[
                    0, 0, 0, 26, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48,
                    58, 48, 48, 46, 48, 48, 48, 48, 48, 48,
                ],
            );
            assert_text_encode(
                TimestampValue::new(1650890322000000000, None),
                &[
                    0, 0, 0, 26, 50, 48, 50, 50, 45, 48, 52, 45, 50, 53, 32, 49, 50, 58, 51, 56,
                    58, 52, 50, 46, 48, 48, 48, 48, 48, 48,
                ],
            );

            // Test NaiveDate encoding
            assert_text_encode(
                NaiveDate::from_ymd_opt(2025, 8, 8).unwrap(),
                &[
                    0, 0, 0, 10, // length: 10 bytes
                    50, 48, 50, 53, 45, 48, 56, 45, 48, 56, // "2025-08-08"
                ],
            );
            assert_text_encode(
                NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
                &[
                    0, 0, 0, 10, // length: 10 bytes
                    50, 48, 48, 48, 45, 48, 49, 45, 48, 49, // "2000-01-01"
                ],
            );
            assert_text_encode(
                NaiveDate::from_ymd_opt(1999, 12, 31).unwrap(),
                &[
                    0, 0, 0, 10, // length: 10 bytes
                    49, 57, 57, 57, 45, 49, 50, 45, 51, 49, // "1999-12-31"
                ],
            );
        }

        Ok(())
    }

    fn assert_bind_encode<T: ToProtocolValue>(value: T, expected: &[u8]) {
        let mut buf = BytesMut::new();
        value.to_binary(&mut buf).unwrap();

        assert_eq!(buf.as_ref(), expected);
    }

    #[test]
    fn test_binary_encoders() -> Result<(), ProtocolError> {
        assert_bind_encode(true, &[0, 0, 0, 1, 1]);
        assert_bind_encode(false, &[0, 0, 0, 1, 0]);
        assert_bind_encode(
            IntervalValue::new(0, 0, 0, 0, 0, 0),
            &[0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        );
        assert_bind_encode(
            IntervalValue::new(1, 2, 3, 4, 5, 6),
            &[
                0, 0, 0, 16, 0, 0, 0, 2, 146, 85, 83, 70, 0, 0, 0, 2, 0, 0, 0, 1,
            ],
        );

        #[cfg(feature = "with-chrono")]
        {
            // Test TimestampValue binary encoding
            assert_bind_encode(
                TimestampValue::new(0, None),
                &[0, 0, 0, 8, 255, 252, 162, 254, 196, 200, 32, 0],
            );
            assert_bind_encode(
                TimestampValue::new(1650890322000000000, None),
                &[0, 0, 0, 8, 0, 2, 128, 120, 159, 252, 216, 128],
            );

            // Test NaiveDate binary encoding
            // PostgreSQL epoch is 2000-01-01, so this date should be 0 days
            assert_bind_encode(
                NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
                &[
                    0, 0, 0, 4, // length: 4 bytes
                    0, 0, 0, 0, // 0 days from epoch
                ],
            );
            // Date after epoch: 2025-08-08 is 9351 days after 2000-01-01
            assert_bind_encode(
                NaiveDate::from_ymd_opt(2025, 8, 8).unwrap(),
                &[
                    0, 0, 0, 4, // length: 4 bytes
                    0, 0, 36, 135, // 9351 days from epoch (0x2487 in hex)
                ],
            );
            // Date before epoch: 1999-12-31 is -1 day from 2000-01-01
            assert_bind_encode(
                NaiveDate::from_ymd_opt(1999, 12, 31).unwrap(),
                &[
                    0, 0, 0, 4, // length: 4 bytes
                    255, 255, 255, 255, // -1 in two's complement
                ],
            );
        }

        Ok(())
    }
}
