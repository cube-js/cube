use crate::wrappers::object::NativeObjectHolder;
use neon::context::Context;
use neon::handle::Handle;
use neon::object::Object;
use neon::types::{JsArray, JsObject, JsString, JsValue};
use serde::{de, ser, Serialize};
use std::fmt;
use std::fmt::Display;
use std::marker::PhantomData;

pub struct NeonObjSerializer<'a, 'b, C>
where
    C: Context<'a>,
{
    context: &'b mut C,
    phantom: PhantomData<&'a ()>,
}

pub struct NeonObjMapSerializer<'a, 'b, C>
where
    C: Context<'a>,
{
    context: &'b mut C,
    obj: Handle<'a, JsObject>,
}

pub struct NeonObjSeqSerializer<'a, 'b, C>
where
    C: Context<'a>,
{
    context: &'b mut C,
    last_id: u32,
    seq: Handle<'a, JsArray>,
}

pub struct NeonObjTupleSerializer<'a, 'b, C>
where
    C: Context<'a>,
{
    _context: &'b mut C,
    tuple: Handle<'a, JsArray>,
}

impl<'a, 'b, C: Context<'a>> NeonObjSerializer<'a, 'b, C> {
    pub fn new(context: &'b mut C) -> Self {
        NeonObjSerializer {
            context,
            phantom: Default::default(),
        }
    }
    pub fn serialize<T: ?Sized>(
        value: &T,
        context: &'b mut C,
    ) -> Result<Handle<'a, JsValue>, NeonObjSerializerError>
    where
        T: Serialize,
    {
        let mut serializer = NeonObjSerializer {
            context,
            phantom: Default::default(),
        };
        value.serialize(&mut serializer)
    }
}

impl<'a, 'b, 'c, C: Context<'a>> ser::Serializer for &'c mut NeonObjSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;
    type SerializeSeq = NeonObjSeqSerializer<'a, 'c, C>;
    type SerializeTuple = NeonObjTupleSerializer<'a, 'c, C>;
    type SerializeTupleStruct = NeonObjTupleSerializer<'a, 'c, C>;
    type SerializeTupleVariant = NeonObjTupleSerializer<'a, 'c, C>;
    type SerializeMap = NeonObjMapSerializer<'a, 'c, C>;
    type SerializeStruct = NeonObjMapSerializer<'a, 'c, C>;
    type SerializeStructVariant = NeonObjMapSerializer<'a, 'c, C>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.boolean(v).upcast())
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v as f64).upcast())
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.number(v).upcast())
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.string(String::from(v)).upcast())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.string(String::from(v)).upcast())
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(NeonObjSerializerError::Message(
            "serialize_bytes is not implemented".to_string(),
        ))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.undefined().upcast())
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        NeonObjSerializer::serialize(value, self.context)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.undefined().upcast())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(self.context.undefined().upcast())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(NeonObjSerializerError::Message(
            "serialize_unit_variant is not implemented".to_string(),
        ))
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "serialize_newtype_struct is not implemented".to_string(),
        ))
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "serialize_newtype_variant is not implemented".to_string(),
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let seq = self.context.empty_array();

        Ok(NeonObjSeqSerializer {
            context: self.context,
            last_id: 0,
            seq,
        })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(NeonObjSerializerError::Message(
            "serialize_tuple is not implemented".to_string(),
        ))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(NeonObjSerializerError::Message(
            "serialize_tuple_struct is not implemented".to_string(),
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(NeonObjSerializerError::Message(
            "serialize_tuple_variant is not implemented".to_string(),
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let obj = self.context.empty_object();
        Ok(NeonObjMapSerializer {
            context: self.context,
            obj,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let obj = self.context.empty_object();
        Ok(NeonObjMapSerializer {
            context: self.context,
            obj,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Err(NeonObjSerializerError::Message(
            "serialize_struct_variant is not implemented".to_string(),
        ))
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeMap for NeonObjMapSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "SerializeMap.serialize_key is not implemented".to_string(),
        ))
    }

    fn serialize_value<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "SerializeMap.serialize_value is not implemented".to_string(),
        ))
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Self::Error>
    where
        K: Serialize,
        V: Serialize,
    {
        let key_value = NeonObjSerializer::serialize(key, self.context)?;
        let value_value = NeonObjSerializer::serialize(value, self.context)?;
        let string_down_cast = key_value
            .downcast::<JsString, _>(self.context)
            .map_err(|e| {
                NeonObjSerializerError::Message(format!("Can't downcast key to JsString: {}", e))
            })?;
        let key_value = string_down_cast.value(self.context);
        self.obj
            .set(self.context, key_value.as_str(), value_value)
            .map_err(|e| {
                NeonObjSerializerError::Message(format!("Can't set value to obj: {}", e))
            })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.obj.upcast())
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeTuple for NeonObjTupleSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "NeonObjTupleSerializer is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.tuple.upcast())
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeTupleStruct for NeonObjTupleSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "SerializeTupleStruct is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.tuple.upcast())
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeTupleVariant for NeonObjTupleSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "SerializeTupleVariant is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.tuple.upcast())
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeStruct for NeonObjMapSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value_value = NeonObjSerializer::serialize(value, self.context)?;
        self.obj.set(self.context, key, value_value).map_err(|e| {
            NeonObjSerializerError::Message(format!("Can't set value to obj: {}", e))
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.obj.upcast())
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeStructVariant for NeonObjMapSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NeonObjSerializerError::Message(
            "SerializeStructVariant is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.obj.upcast())
    }
}

impl<'a, 'b, C: Context<'a>> ser::SerializeSeq for NeonObjSeqSerializer<'a, 'b, C> {
    type Ok = Handle<'a, JsValue>;
    type Error = NeonObjSerializerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value_value = NeonObjSerializer::serialize(value, self.context)?;
        self.seq
            .set(self.context, self.last_id, value_value)
            .map_err(|e| {
                NeonObjSerializerError::Message(format!("Can't set value to array: {}", e))
            })?;

        self.last_id += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.seq.upcast())
    }
}

#[derive(Debug)]
pub enum NeonObjSerializerError {
    Message(String),
    // JsError(JsError),
}

impl ser::Error for NeonObjSerializerError {
    fn custom<T: Display>(msg: T) -> Self {
        NeonObjSerializerError::Message(msg.to_string())
    }
}

impl de::Error for NeonObjSerializerError {
    fn custom<T: Display>(msg: T) -> Self {
        NeonObjSerializerError::Message(msg.to_string())
    }
}

impl Display for NeonObjSerializerError {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NeonObjSerializerError::Message(msg) => formatter.write_str(msg),
            // NeonObjSerializerError::JsError(err) => Display::fmt(err, formatter),
        }
    }
}

impl std::error::Error for NeonObjSerializerError {}
