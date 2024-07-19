use super::error::NativeObjSerializerError;
use crate::wrappers::context::NativeContextHolder;
use crate::wrappers::object::{NativeArray, NativeStruct};
use crate::wrappers::object_handle::NativeObjectHandle;
use serde;
use serde::de::{DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::forward_to_deserialize_any;
use serde::{de, ser, Deserializer};
use std::fmt;
use std::fmt::Display;

pub struct NativeSerdeDeserializer {
    input: NativeObjectHandle,
}

impl NativeSerdeDeserializer {
    pub fn new(input: NativeObjectHandle) -> Self {
        Self { input }
    }

    pub fn deserialize<T>(self) -> Result<T, NativeObjSerializerError>
    where
        T: DeserializeOwned,
    {
        T::deserialize(self)
    }
}

impl<'de> Deserializer<'de> for NativeSerdeDeserializer {
    type Error = NativeObjSerializerError;
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.input.is_null() || self.input.is_undefined() {
            visitor.visit_unit()
        } else if let Ok(val) = self.input.to_boolean() {
            visitor.visit_bool(val.value().unwrap())
        } else if let Ok(val) = self.input.to_string() {
            visitor.visit_string(val.value().unwrap())
        } else if let Ok(val) = self.input.to_number() {
            visitor.visit_f64(val.value().unwrap())
        } else if let Ok(val) = self.input.to_array() {
            let deserializer = NativeSeqDeserializer::new(val);
            visitor.visit_seq(deserializer)
        } else if let Ok(val) = self.input.to_struct() {
            let deserializer = NativeMapDeserializer::new(val)?;
            visitor.visit_map(deserializer)
        } else {
            Err(NativeObjSerializerError::Message(
                "deserializer is not implemented".to_string(),
            ))
        }
    }
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.input.is_null() || self.input.is_undefined() {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    forward_to_deserialize_any! {
       <V: Visitor<'de>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        unit unit_struct seq tuple tuple_struct map struct identifier
        newtype_struct
    }
}

pub struct NativeSeqDeserializer {
    input: Box<dyn NativeArray>,
    idx: u32,
    len: u32,
}

impl NativeSeqDeserializer {
    pub fn new(input: Box<dyn NativeArray>) -> Self {
        let len = input.len().unwrap();
        Self { input, idx: 0, len }
    }
}

impl<'de> SeqAccess<'de> for NativeSeqDeserializer {
    type Error = NativeObjSerializerError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.idx >= self.len {
            return Ok(None);
        }
        let v = self
            .input
            .get(self.idx)
            .map_err(|_| NativeObjSerializerError::Message("Failed to get element".to_string()))?;

        self.idx += 1;

        let de = NativeSerdeDeserializer::new(v);
        seed.deserialize(de).map(Some)
    }
}

struct NativeMapDeserializer {
    input: Box<dyn NativeStruct>,
    prop_names: Vec<NativeObjectHandle>,
    key_idx: u32,
    value_idx: u32,
    len: u32,
}

impl NativeMapDeserializer {
    pub fn new(input: Box<dyn NativeStruct>) -> Result<Self, NativeObjSerializerError> {
        let prop_names = input.get_own_property_names().map_err(|_| {
            NativeObjSerializerError::Message(format!("Failed to get property names"))
        })?;
        let len = prop_names.len() as u32;
        Ok(Self {
            input,
            prop_names,
            key_idx: 0,
            value_idx: 0,
            len,
        })
    }
}

impl<'de> MapAccess<'de> for NativeMapDeserializer {
    type Error = NativeObjSerializerError;
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        if self.key_idx >= self.len {
            return Ok(None);
        }
        let v = self
            .prop_names
            .get(self.key_idx as usize)
            .ok_or_else(|| NativeObjSerializerError::Message("Failed to get key".to_string()))?;
        self.key_idx += 1;
        seed.deserialize(NativeSerdeDeserializer::new(v.clone()))
            .map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        if self.value_idx >= self.len {
            return Err(NativeObjSerializerError::Message(format!(
                "Array index out of bounds"
            )));
        }
        let prop_name = self
            .prop_names
            .get(self.value_idx as usize)
            .ok_or_else(|| {
                NativeObjSerializerError::Message(format!("Array index out of bounds"))
            })?;
        let prop_string = prop_name
            .to_string()
            .and_then(|s| s.value())
            .map_err(|_| NativeObjSerializerError::Message(format!("key should be string")))?;

        let value = self.input.get_field(&prop_string).map_err(|_| {
            NativeObjSerializerError::Message(format!("Failed to get property name"))
        })?;

        self.value_idx += 1;
        let de = NativeSerdeDeserializer::new(value);
        let res = seed.deserialize(de)?;
        Ok(res)
    }
}
