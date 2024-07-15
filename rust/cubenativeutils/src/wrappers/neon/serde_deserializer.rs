use neon::prelude::*;
use serde::{
    de,
    de::{DeserializeSeed, SeqAccess, Visitor},
    ser, Deserializer,
};
use std::fmt;
use std::fmt::Display;
use std::marker::PhantomData;

pub struct NeonObjDeserializer<'a, 'b, C>
where
    C: Context<'a>,
{
    context: &'b mut C,
    input: Handle<'a, JsValue>,
}

impl<'a, 'b, C: Context<'a>> NeonObjDeserializer<'a, 'b, C> {
    pub fn new(context: &'b mut C, input: Handle<'a, JsValue>) -> Self {
        Self { context, input }
    }
}

/* impl<'x, 'd, 'a, 'b, C: Context<'a>> Deserializer<'x> for &'d mut NeonObjDeserializer<'a, 'b, C> {
    type Error = NeonObjDeserializerError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'x>,
    {
        todo!()
    }
} */

#[doc(hidden)]
struct JsArrayAccess<'b, 'a, C: Context<'a>> {
    cx: &'b mut C,
    input: Handle<'a, JsArray>,
    idx: u32,
    len: u32,
}

#[doc(hidden)]
impl<'b, 'a, C: Context<'a>> JsArrayAccess<'b, 'a, C> {
    fn new(cx: &'b mut C, input: Handle<'a, JsArray>) -> Self {
        JsArrayAccess {
            cx,
            input,
            idx: 0,
            len: input.len(cx),
        }
    }
}

#[doc(hidden)]
impl<'x, 'b, 'a, C: Context<'a>> SeqAccess<'x> for JsArrayAccess<'b, 'a, C> {
    type Error = NeonObjDeserializerError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'x>,
    {
        if self.idx >= self.len {
            return Ok(None);
        }
        let v = self.input.get(self.cx, self.idx).map_err(|_| {
            NeonObjDeserializerError::Message("Error getting value from array".to_string())
        })?;
        self.idx += 1;

        let mut de = Deserializer::new(self.cx, v);
        seed.deserialize(&mut de).map(Some)
    }
}

#[derive(Debug)]
pub enum NeonObjDeserializerError {
    Message(String),
    // JsError(JsError),
}

impl ser::Error for NeonObjDeserializerError {
    fn custom<T: Display>(msg: T) -> Self {
        NeonObjDeserializerError::Message(msg.to_string())
    }
}

impl de::Error for NeonObjDeserializerError {
    fn custom<T: Display>(msg: T) -> Self {
        NeonObjDeserializerError::Message(msg.to_string())
    }
}

impl Display for NeonObjDeserializerError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NeonObjDeserializerError::Message(msg) => formatter.write_str(msg),
            // NeonObjSerializerError::JsError(err) => Display::fmt(err, formatter),
        }
    }
}

impl std::error::Error for NeonObjDeserializerError {}
