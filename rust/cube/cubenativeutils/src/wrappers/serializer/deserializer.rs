use super::error::NativeObjSerializerError;
use crate::wrappers::{
    inner_types::InnerTypes,
    object::{
        NativeArray, NativeBoolean, NativeNumber, NativeString, NativeStruct, NativeTypedObject,
    },
    object_handle::NativeObjectHandle,
};
use serde::{
    self,
    de::{DeserializeOwned, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    forward_to_deserialize_any, Deserializer,
};

pub struct NativeSerdeDeserializer<IT: InnerTypes> {
    input: NativeObjectHandle<IT>,
}

impl<IT: InnerTypes> NativeSerdeDeserializer<IT> {
    pub fn new(input: NativeObjectHandle<IT>) -> Self {
        Self { input }
    }

    pub fn deserialize<T>(self) -> Result<T, NativeObjSerializerError>
    where
        T: DeserializeOwned,
    {
        T::deserialize(self)
    }
}

impl<'de, IT: InnerTypes> Deserializer<'de> for NativeSerdeDeserializer<IT> {
    type Error = NativeObjSerializerError;
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.input.into_typed()? {
            NativeTypedObject::Null | NativeTypedObject::Undefined => visitor.visit_unit(),
            NativeTypedObject::Boolean(val) => visitor.visit_bool(val.value()?),
            NativeTypedObject::String(val) => visitor.visit_string(val.into_value()?),
            NativeTypedObject::Number(val) => {
                let num = val.value()?;
                // Self-describing consumers like FilterValue expect whole numbers as ints.
                if num.fract() == 0.0 && num.is_finite() {
                    visitor.visit_i64(num as i64)
                } else {
                    visitor.visit_f64(num)
                }
            }
            NativeTypedObject::Array(val) => {
                visitor.visit_seq(NativeSeqDeserializer::<IT>::new(val)?)
            }
            NativeTypedObject::Struct(val) => {
                visitor.visit_map(NativeMapDeserializer::<IT>::new(val)?)
            }
            NativeTypedObject::Function(_) | NativeTypedObject::RustBox(_) => Err(
                NativeObjSerializerError::Message("deserializer is not implemented".to_string()),
            ),
        }
    }
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if self.input.is_null()? || self.input.is_undefined()? {
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // The native deserializer has no notion of serde's tagged enum
        // representation, so a derived `Deserialize` on an enum can't work here.
        // Implement `Deserialize` manually via `deserialize_any` instead (see
        // `FilterValue` in cubesqlplanner for an example).
        Err(NativeObjSerializerError::Message(format!(
            "Deserializing enum `{name}` is not supported by the native deserializer; \
             implement Deserialize manually via deserialize_any"
        )))
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
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 char str string
        unit unit_struct seq tuple tuple_struct map struct identifier
        newtype_struct
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Ok(val) = self.input.into_number() {
            visitor.visit_f32(val.value()? as f32)
        } else {
            Err(NativeObjSerializerError::Message(
                "JS Number expected for f32 field".to_string(),
            ))
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        if let Ok(val) = self.input.into_number() {
            visitor.visit_f64(val.value()?)
        } else {
            Err(NativeObjSerializerError::Message(
                "JS Number expected for f64 field".to_string(),
            ))
        }
    }
}

pub struct NativeSeqDeserializer<IT: InnerTypes> {
    input: IT::Array,
    idx: u32,
    len: u32,
}

impl<IT: InnerTypes> NativeSeqDeserializer<IT> {
    pub fn new(input: IT::Array) -> Result<Self, NativeObjSerializerError> {
        let len = input.len()?;
        Ok(Self { input, idx: 0, len })
    }
}

impl<'de, IT: InnerTypes> SeqAccess<'de> for NativeSeqDeserializer<IT> {
    type Error = NativeObjSerializerError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        if self.idx >= self.len {
            return Ok(None);
        }
        let idx = self.idx;
        let v = self.input.get(idx).map_err(|err| {
            NativeObjSerializerError::from(err)
                .context(format!("element {idx}: failed to read value"))
        })?;

        self.idx += 1;

        let de = NativeSerdeDeserializer::new(v);
        seed.deserialize(de)
            .map(Some)
            .map_err(|err| err.context(format!("element {idx}")))
    }
}

struct NativeMapDeserializer<IT: InnerTypes> {
    input: IT::Struct,
    prop_names: Vec<NativeObjectHandle<IT>>,
    key_idx: u32,
    value_idx: u32,
    len: u32,
}

impl<IT: InnerTypes> NativeMapDeserializer<IT> {
    pub fn new(input: IT::Struct) -> Result<Self, NativeObjSerializerError> {
        let prop_names = input.get_own_property_names().map_err(|err| {
            NativeObjSerializerError::from(err).context("failed to get property names")
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

impl<'de, IT: InnerTypes> MapAccess<'de> for NativeMapDeserializer<IT> {
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
            return Err(NativeObjSerializerError::Message(
                "Array index out of bounds".to_string(),
            ));
        }
        let prop_name = self
            .prop_names
            .get(self.value_idx as usize)
            .ok_or_else(|| {
                NativeObjSerializerError::Message("Array index out of bounds".to_string())
            })?;
        let field = || {
            let key = prop_name
                .to_string()
                .and_then(|s| s.into_value())
                .unwrap_or_default();
            format!("field `{key}`")
        };
        let value = self.input.get_field_by_key(prop_name).map_err(|err| {
            NativeObjSerializerError::from(err)
                .context(format!("{}: failed to read value", field()))
        })?;

        self.value_idx += 1;
        let de = NativeSerdeDeserializer::new(value);
        seed.deserialize(de).map_err(|err| err.context(field()))
    }
}
