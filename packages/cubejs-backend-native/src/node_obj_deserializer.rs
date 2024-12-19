use neon::prelude::*;
use neon::result::Throw;
use serde::de::{
    self, Deserializer, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, StdError,
    VariantAccess, Visitor,
};
use serde::forward_to_deserialize_any;
use std::fmt;

#[derive(Debug)]
pub struct JsDeserializationError(String);

impl fmt::Display for JsDeserializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JS Deserialization Error: {}", self.0)
    }
}

impl From<Throw> for JsDeserializationError {
    fn from(throw: Throw) -> Self {
        JsDeserializationError(throw.to_string())
    }
}

impl StdError for JsDeserializationError {}

impl de::Error for JsDeserializationError {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        JsDeserializationError(msg.to_string())
    }
}

pub struct JsValueDeserializer<'a, 'b> {
    pub cx: &'a mut FunctionContext<'b>,
    pub value: Handle<'a, JsValue>,
}

impl<'a, 'b> JsValueDeserializer<'a, 'b> {
    pub fn new(cx: &'a mut FunctionContext<'b>, value: Handle<'a, JsValue>) -> Self {
        Self { cx, value }
    }
}

impl<'de, 'a, 'b> Deserializer<'de> for JsValueDeserializer<'a, 'b> {
    type Error = JsDeserializationError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_a::<JsString, _>(self.cx) {
            let value = self
                .value
                .downcast::<JsString, _>(self.cx)
                .or_throw(self.cx)?
                .value(self.cx);
            visitor.visit_string(value)
        } else if self.value.is_a::<JsNumber, _>(self.cx) {
            let value = self
                .value
                .downcast::<JsNumber, _>(self.cx)
                .or_throw(self.cx)?
                .value(self.cx);

            // floats
            if value.fract() != 0.0 {
                return visitor.visit_f64(value);
            }

            // unsigned
            if value >= 0.0 {
                if value <= u8::MAX as f64 {
                    return visitor.visit_u8(value as u8);
                }

                if value <= u16::MAX as f64 {
                    return visitor.visit_u16(value as u16);
                }

                if value <= u32::MAX as f64 {
                    return visitor.visit_u32(value as u32);
                }

                if value <= u64::MAX as f64 {
                    return visitor.visit_u64(value as u64);
                }
            }

            if value >= i8::MIN as f64 && value <= i8::MAX as f64 {
                return visitor.visit_i8(value as i8);
            }

            if value >= i16::MIN as f64 && value <= i16::MAX as f64 {
                return visitor.visit_i16(value as i16);
            }

            if value >= i32::MIN as f64 && value <= i32::MAX as f64 {
                return visitor.visit_i32(value as i32);
            }

            if value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                return visitor.visit_i64(value as i64);
            }

            Err(JsDeserializationError(
                "Unsupported number type for deserialization".to_string(),
            ))
        } else if self.value.is_a::<JsBoolean, _>(self.cx) {
            let value = self
                .value
                .downcast::<JsBoolean, _>(self.cx)
                .or_throw(self.cx)?
                .value(self.cx);
            visitor.visit_bool(value)
        } else if self.value.is_a::<JsArray, _>(self.cx) {
            let js_array = self
                .value
                .downcast::<JsArray, _>(self.cx)
                .or_throw(self.cx)?;
            let deserializer = JsArrayDeserializer::new(self.cx, js_array);
            visitor.visit_seq(deserializer)
        } else if self.value.is_a::<JsObject, _>(self.cx) {
            let js_object = self
                .value
                .downcast::<JsObject, _>(self.cx)
                .or_throw(self.cx)?;
            let deserializer = JsObjectDeserializer::new(self.cx, js_object);
            visitor.visit_map(deserializer)
        } else if self.value.is_a::<JsNull, _>(self.cx)
            || self.value.is_a::<JsUndefined, _>(self.cx)
        {
            visitor.visit_none()
        } else if self.value.is_a::<JsFunction, _>(self.cx) {
            // We can do nothing with the JS functions in native
            visitor.visit_none()
        } else {
            Err(JsDeserializationError(
                "Unsupported type for deserialization".to_string(),
            ))
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_a::<JsNull, _>(self.cx) || self.value.is_a::<JsUndefined, _>(self.cx) {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_a::<JsArray, _>(self.cx) {
            let js_array = self
                .value
                .downcast::<JsArray, _>(self.cx)
                .or_throw(self.cx)?;
            let deserializer = JsArrayDeserializer::new(self.cx, js_array);
            visitor.visit_seq(deserializer)
        } else {
            Err(JsDeserializationError("expected an array".to_string()))
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.value.is_a::<JsObject, _>(self.cx) {
            let js_object = self
                .value
                .downcast::<JsObject, _>(self.cx)
                .or_throw(self.cx)?;
            let deserializer = JsObjectDeserializer::new(self.cx, js_object);
            visitor.visit_map(deserializer)
        } else {
            Err(JsDeserializationError("expected an object".to_string()))
        }
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let deserializer = JsEnumDeserializer::new(self.cx, self.value);
        visitor.visit_enum(deserializer)
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf identifier ignored_any
    }
}

struct JsObjectDeserializer<'a, 'b> {
    cx: &'a mut FunctionContext<'b>,
    js_object: Handle<'a, JsObject>,
    keys: Vec<String>,
    index: usize,
}

impl<'a, 'b> JsObjectDeserializer<'a, 'b> {
    fn new(cx: &'a mut FunctionContext<'b>, js_object: Handle<'a, JsObject>) -> Self {
        let keys = js_object
            .get_own_property_names(cx)
            .expect("Failed to get object keys")
            .to_vec(cx)
            .expect("Failed to convert keys to Vec")
            .iter()
            .filter_map(|k| {
                k.downcast_or_throw::<JsString, _>(cx)
                    .ok()
                    .map(|js_string| js_string.value(cx))
            })
            .collect::<Vec<String>>();
        Self {
            cx,
            js_object,
            keys,
            index: 0,
        }
    }
}

// `MapAccess` is provided to the `Visitor` to give it the ability to iterate
// through entries of the map.
impl<'de, 'a, 'b> MapAccess<'de> for JsObjectDeserializer<'a, 'b> {
    type Error = JsDeserializationError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de>,
    {
        if self.index >= self.keys.len() {
            return Ok(None);
        }
        let key = &self.keys[self.index];
        self.index += 1;
        seed.deserialize(key.as_str().into_deserializer()).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de>,
    {
        let key = &self.keys[self.index - 1];
        let value = self
            .js_object
            .get(self.cx, key.as_str())
            .expect("Failed to get value by key");
        seed.deserialize(JsValueDeserializer::new(self.cx, value))
    }
}

struct JsArrayDeserializer<'a, 'b> {
    cx: &'a mut FunctionContext<'b>,
    js_array: Handle<'a, JsArray>,
    index: usize,
    length: usize,
}

impl<'a, 'b> JsArrayDeserializer<'a, 'b> {
    fn new(cx: &'a mut FunctionContext<'b>, js_array: Handle<'a, JsArray>) -> Self {
        let length = js_array.len(cx) as usize;
        Self {
            cx,
            js_array,
            index: 0,
            length,
        }
    }
}

// `SeqAccess` is provided to the `Visitor` to give it the ability to iterate
// through elements of the sequence.
impl<'de, 'a, 'b> SeqAccess<'de> for JsArrayDeserializer<'a, 'b> {
    type Error = JsDeserializationError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        if self.index >= self.length {
            return Ok(None);
        }
        let value = self
            .js_array
            .get(self.cx, self.index as u32)
            .map_err(JsDeserializationError::from)?;
        self.index += 1;

        seed.deserialize(JsValueDeserializer::new(self.cx, value))
            .map(Some)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.length)
    }
}

pub struct JsEnumDeserializer<'a, 'b> {
    cx: &'a mut FunctionContext<'b>,
    value: Handle<'a, JsValue>,
}

impl<'a, 'b> JsEnumDeserializer<'a, 'b> {
    fn new(cx: &'a mut FunctionContext<'b>, value: Handle<'a, JsValue>) -> Self {
        Self { cx, value }
    }
}

// `EnumAccess` is provided to the `Visitor` to give it the ability to determine
// which variant of the enum is supposed to be deserialized.
impl<'de, 'a, 'b> EnumAccess<'de> for JsEnumDeserializer<'a, 'b> {
    type Error = JsDeserializationError;
    type Variant = Self;

    fn variant_seed<T>(self, seed: T) -> Result<(T::Value, Self::Variant), Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        let variant = seed.deserialize(JsValueDeserializer::new(self.cx, self.value))?;
        Ok((variant, self))
    }
}

impl<'de, 'a, 'b> VariantAccess<'de> for JsEnumDeserializer<'a, 'b> {
    type Error = JsDeserializationError;

    // If the `Visitor` expected this variant to be a unit variant, the input
    // should have been the plain string case handled in `deserialize_enum`.
    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }

    // Newtype variants are represented in JSON as `{ NAME: VALUE }` so
    // deserialize the value here.
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(JsValueDeserializer::new(self.cx, self.value))
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }` so
    // deserialize the sequence of data here.
    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Deserializer::deserialize_seq(JsValueDeserializer::new(self.cx, self.value), visitor)
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }` so
    // deserialize the inner map here.
    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        Deserializer::deserialize_map(JsValueDeserializer::new(self.cx, self.value), visitor)
    }
}
