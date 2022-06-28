use crate::ProtocolError;
use bytes::{BufMut, BytesMut};

// This trait explains how to encode values to the protocol format
pub trait ToProtocolValue: std::fmt::Debug {
    // Converts native type to raw value in text format
    fn to_text(&self, buf: &mut BytesMut) -> Result<(), ProtocolError>
    where
        Self: Sized;

    // Converts native type to raw value in binary format
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
