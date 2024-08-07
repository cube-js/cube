use super::error::NativeObjSerializerError;
use crate::wrappers::{NativeArray, NativeContextHolder, NativeObjectHandler, NativeStruct};
use serde::{ser, Serialize};

pub struct NativeSerdeSerializer {
    context: NativeContextHolder,
}

pub struct NativeSeqSerializer {
    context: NativeContextHolder,
    last_id: u32,
    seq: Box<dyn NativeArray>,
}

pub struct NativeMapSerializer {
    context: NativeContextHolder,
    obj: Box<dyn NativeStruct>,
}

pub struct NativeTupleSerializer {
    _context: NativeContextHolder,
    tuple: Box<dyn NativeArray>,
}

impl NativeSerdeSerializer {
    pub fn new(context: NativeContextHolder) -> Self {
        Self { context }
    }

    pub fn serialize<T: ?Sized>(
        value: &T,
        context: NativeContextHolder,
    ) -> Result<NativeObjectHandler, NativeObjSerializerError>
    where
        T: Serialize,
    {
        let serializer = NativeSerdeSerializer::new(context);
        value.serialize(serializer)
    }
}

impl ser::Serializer for NativeSerdeSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;
    type SerializeSeq = NativeSeqSerializer;
    type SerializeTuple = NativeTupleSerializer;
    type SerializeTupleStruct = NativeTupleSerializer;
    type SerializeTupleVariant = NativeTupleSerializer;
    type SerializeMap = NativeMapSerializer;
    type SerializeStruct = NativeMapSerializer;
    type SerializeStructVariant = NativeMapSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.boolean(v).into_object(),
        ))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.number(v as f64).into_object(),
        ))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.string(String::from(v)).into_object(),
        ))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(
            self.context.string(String::from(v)).into_object(),
        ))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        Err(NativeObjSerializerError::Message(
            "serialize_bytes is not implemented".to_string(),
        ))
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.context.undefined()))
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        NativeSerdeSerializer::serialize(value, self.context.clone())
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.context.undefined()))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.context.undefined()))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Err(NativeObjSerializerError::Message(
            "serialize_unit_variant is not implemented".to_string(),
        ))
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "serialize_newtype_struct is not implemented".to_string(),
        ))
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "serialize_newtype_variant is not implemented".to_string(),
        ))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let seq = self.context.empty_array();

        Ok(NativeSeqSerializer {
            context: self.context.clone(),
            last_id: 0,
            seq,
        })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Err(NativeObjSerializerError::Message(
            "serialize_tuple is not implemented".to_string(),
        ))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Err(NativeObjSerializerError::Message(
            "serialize_tuple_stuct is not implemented".to_string(),
        ))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Err(NativeObjSerializerError::Message(
            "serialize_tuple_variant is not implemented".to_string(),
        ))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        let obj = self.context.empty_struct();
        Ok(NativeMapSerializer {
            context: self.context.clone(),
            obj,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        let obj = self.context.empty_struct();
        Ok(NativeMapSerializer {
            context: self.context.clone(),
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
        Err(NativeObjSerializerError::Message(
            "serialize_struct_variant is not implemented".to_string(),
        ))
    }
}

impl ser::SerializeSeq for NativeSeqSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value_value = NativeSerdeSerializer::serialize(value, self.context.clone())?;
        self.seq.set(self.last_id, value_value).map_err(|e| {
            NativeObjSerializerError::Message(format!("Can't set value to array: {}", e))
        })?;

        self.last_id += 1;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.seq.into_object()))
    }
}

impl ser::SerializeMap for NativeMapSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "SerializeMap.serialize_key is not implemented".to_string(),
        ))
    }

    fn serialize_value<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NativeObjSerializerError::Message(
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
        let key_value = NativeSerdeSerializer::serialize(key, self.context.clone())?;
        let value_value = NativeSerdeSerializer::serialize(value, self.context.clone())?;
        let string_down_cast = key_value.into_string().map_err(|e| {
            NativeObjSerializerError::Message(format!("Can't downcast key to native string: {}", e))
        })?;
        let key_value = string_down_cast.value().map_err(|e| {
            NativeObjSerializerError::Message(format!("Can't downcast key to string: {}", e))
        })?;
        self.obj
            .set_field(key_value.as_str(), value_value)
            .map_err(|e| {
                NativeObjSerializerError::Message(format!("Can't set value to obj: {}", e))
            })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.obj.into_object()))
    }
}

impl ser::SerializeStruct for NativeMapSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value_value = NativeSerdeSerializer::serialize(value, self.context.clone())?;
        self.obj.set_field(key, value_value).map_err(|e| {
            NativeObjSerializerError::Message(format!("Can't set value to obj: {}", e))
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.obj.into_object()))
    }
}

impl ser::SerializeStructVariant for NativeMapSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_field<T: ?Sized>(
        &mut self,
        _key: &'static str,
        _value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "SerializeStructVariant is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.obj.into_object()))
    }
}

impl ser::SerializeTuple for NativeTupleSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "NativeTupleSerializer is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.tuple.into_object()))
    }
}

impl ser::SerializeTupleStruct for NativeTupleSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "SerializeTupleStruct is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.tuple.into_object()))
    }
}

impl ser::SerializeTupleVariant for NativeTupleSerializer {
    type Ok = NativeObjectHandler;
    type Error = NativeObjSerializerError;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        Err(NativeObjSerializerError::Message(
            "SerializeTupleVariant is not implemented".to_string(),
        ))
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(NativeObjectHandler::new(self.tuple.into_object()))
    }
}
