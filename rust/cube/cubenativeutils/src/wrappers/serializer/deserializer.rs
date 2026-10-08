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
    de::{DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor},
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
        visit_typed(self.input.into_typed()?, visitor)
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

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // `fields` lists every name the struct accepts, so the rest would be read only to be ignored.
        match self.input.into_typed()? {
            NativeTypedObject::Struct(val) => visitor.visit_map(
                NativeMapDeserializer::<IT, _>::new(val.entries_for_fields(fields)?),
            ),
            typed => visit_typed(typed, visitor),
        }
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
        unit unit_struct seq tuple tuple_struct map identifier
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

fn visit_typed<'de, IT: InnerTypes, V: Visitor<'de>>(
    typed: NativeTypedObject<IT>,
    visitor: V,
) -> Result<V::Value, NativeObjSerializerError> {
    match typed {
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
        NativeTypedObject::Array(val) => visitor.visit_seq(NativeSeqDeserializer::<IT>::new(val)?),
        NativeTypedObject::Struct(val) => {
            visitor.visit_map(NativeMapDeserializer::<IT, _>::new(val.entries()?))
        }
        NativeTypedObject::Function(_) | NativeTypedObject::RustBox(_) => Err(
            NativeObjSerializerError::Message("deserializer is not implemented".to_string()),
        ),
    }
}

pub struct NativeSeqDeserializer<IT: InnerTypes> {
    items: std::iter::Enumerate<std::vec::IntoIter<NativeObjectHandle<IT>>>,
}

impl<IT: InnerTypes> NativeSeqDeserializer<IT> {
    pub fn new(input: IT::Array) -> Result<Self, NativeObjSerializerError> {
        let items = input.to_vec()?;
        Ok(Self {
            items: items.into_iter().enumerate(),
        })
    }
}

impl<'de, IT: InnerTypes> SeqAccess<'de> for NativeSeqDeserializer<IT> {
    type Error = NativeObjSerializerError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        let Some((idx, item)) = self.items.next() else {
            return Ok(None);
        };
        seed.deserialize(NativeSerdeDeserializer::new(item))
            .map(Some)
            .map_err(|err| err.context(format!("element {idx}")))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len())
    }
}

struct NativeMapDeserializer<IT: InnerTypes, K: AsRef<str>> {
    entries: std::vec::IntoIter<(K, NativeObjectHandle<IT>)>,
    // Set in `next_key_seed`; the key stays here for the error context of its value.
    current: Option<(K, NativeObjectHandle<IT>)>,
}

impl<IT: InnerTypes, K: AsRef<str>> NativeMapDeserializer<IT, K> {
    fn new(entries: Vec<(K, NativeObjectHandle<IT>)>) -> Self {
        Self {
            entries: entries.into_iter(),
            current: None,
        }
    }
}

impl<'de, IT: InnerTypes, K: AsRef<str>> MapAccess<'de> for NativeMapDeserializer<IT, K> {
    type Error = NativeObjSerializerError;
    fn next_key_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: DeserializeSeed<'de>,
    {
        let Some(entry) = self.entries.next() else {
            return Ok(None);
        };
        let (key, _) = self.current.insert(entry);
        // By `&str`: struct field identifiers only need to compare it.
        seed.deserialize(key.as_ref().into_deserializer()).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let (key, value) = self.current.take().ok_or_else(|| {
            NativeObjSerializerError::Message(
                "next_value_seed called before next_key_seed".to_string(),
            )
        })?;
        seed.deserialize(NativeSerdeDeserializer::new(value))
            .map_err(|err| err.context(format!("field `{}`", key.as_ref())))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}
