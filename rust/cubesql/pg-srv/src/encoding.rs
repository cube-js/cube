//! Encoding native values to the Protocol representation

use crate::ProtocolError;
use bytes::{BufMut, BytesMut};
#[cfg(feature = "with-chrono")]
use chrono::{NaiveDate, NaiveDateTime};
use std::io::{Error, ErrorKind};

/// This trait explains how to encode values to the protocol format
pub trait ToProtocolValue: std::fmt::Debug {
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

// POSTGRES_EPOCH_JDATE
#[cfg(feature = "with-chrono")]
fn pg_base_date_epoch() -> NaiveDateTime {
    NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)
}

#[cfg(feature = "with-chrono")]
impl ToProtocolValue for NaiveDate {
    // date_out - https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/date.c#L176
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        self.to_string().to_text(buf)
    }

    // date_send - https://github.com/postgres/postgres/blob/REL_14_4/src/backend/utils/adt/date.c#L223
    fn to_binary(&self, buf: &mut BytesMut) -> Result<(), ProtocolError> {
        let n = self
            .signed_duration_since(pg_base_date_epoch().date())
            .num_days();
        if n > (i32::MAX as i64) {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "value too large to store in the binary format (i32), actual: {}",
                    n
                ),
            )
            .into());
        }

        buf.put_i32(4);
        buf.put_i32(n as i32);

        Ok(())
    }
}
