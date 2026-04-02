use byteorder::{BigEndian, WriteBytesExt};

/// Trait for types that can be serialized as part of a RocksDB secondary index key.
///
/// Implementations define how each type is written into a byte buffer.
/// Used by the `#[derive(SecondaryIndexKey)]` macro to generate `to_bytes()` on index key enums.
pub trait IndexKeyToBytes {
    fn write_index_key_bytes(&self, buf: &mut Vec<u8>);
}

impl IndexKeyToBytes for String {
    fn write_index_key_bytes(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_bytes());
    }
}

impl IndexKeyToBytes for u64 {
    fn write_index_key_bytes(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(*self).unwrap();
    }
}

impl IndexKeyToBytes for bool {
    fn write_index_key_bytes(&self, buf: &mut Vec<u8>) {
        buf.push(if *self { 1 } else { 0 });
    }
}

impl IndexKeyToBytes for Option<String> {
    fn write_index_key_bytes(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self.as_deref().unwrap_or("__null__").as_bytes());
    }
}

impl IndexKeyToBytes for Option<u64> {
    fn write_index_key_bytes(&self, buf: &mut Vec<u8>) {
        match self {
            None => buf.push(0),
            Some(id) => {
                buf.push(1);
                buf.write_u64::<BigEndian>(*id).unwrap();
            }
        }
    }
}
