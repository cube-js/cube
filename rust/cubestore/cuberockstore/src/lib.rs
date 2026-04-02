// re-export
pub use rocksdb;

mod index_key;
pub use index_key::IndexKeyToBytes;

// re-export derive macro
pub use cuberockstore_derive::SecondaryIndexKey;

#[cfg(test)]
mod tests {
    use super::*;
    use crate as cuberockstore;

    #[derive(Hash, Clone, Debug, SecondaryIndexKey)]
    enum SimpleKey {
        ByName(String),
        ById(u64),
    }

    #[test]
    fn test_to_bytes_string() {
        let key = SimpleKey::ByName("hello".to_string());
        assert_eq!(key.to_bytes(), b"hello");
    }

    #[test]
    fn test_to_bytes_u64() {
        let key = SimpleKey::ById(42);
        let bytes = key.to_bytes();
        assert_eq!(bytes.len(), 8);
        // Big-endian encoding of 42
        assert_eq!(bytes, 42u64.to_be_bytes());
    }

    #[derive(Hash, Clone, Debug, SecondaryIndexKey)]
    enum MultiFieldKey {
        ByIdAndName(u64, String),
    }

    #[test]
    fn test_to_bytes_multi_field() {
        let key = MultiFieldKey::ByIdAndName(1, "abc".to_string());
        let mut expected = Vec::new();
        expected.extend_from_slice(&1u64.to_be_bytes());
        expected.extend_from_slice(b"abc");
        assert_eq!(key.to_bytes(), expected);
    }

    #[derive(Hash, Clone, Debug, SecondaryIndexKey)]
    #[allow(dead_code)]
    enum UnitKey {
        All,
        ByName(String),
    }

    #[test]
    fn test_to_bytes_unit_variant() {
        let key = UnitKey::All;
        assert_eq!(key.to_bytes(), Vec::<u8>::new());
    }

    #[derive(Hash, Clone, Debug, SecondaryIndexKey)]
    enum NullableKey {
        ByPath(String),
        #[nullable]
        ByExternalId(Option<String>),
        #[nullable]
        ByOptionalNum(Option<u64>),
    }

    #[test]
    fn test_is_nullable() {
        assert!(!NullableKey::ByPath("x".to_string()).is_nullable());
        assert!(NullableKey::ByExternalId(Some("x".to_string())).is_nullable());
        assert!(NullableKey::ByExternalId(None).is_nullable());
        assert!(NullableKey::ByOptionalNum(Some(1)).is_nullable());
        assert!(NullableKey::ByOptionalNum(None).is_nullable());
    }

    #[test]
    fn test_to_bytes_nullable_some() {
        let key = NullableKey::ByExternalId(Some("test".to_string()));
        assert_eq!(key.to_bytes(), b"test");
    }

    #[test]
    fn test_to_bytes_nullable_none_string() {
        let key = NullableKey::ByExternalId(None);
        assert_eq!(key.to_bytes(), b"__null__");
    }

    #[test]
    fn test_to_bytes_nullable_none_u64() {
        let key = NullableKey::ByOptionalNum(None);
        assert_eq!(key.to_bytes(), vec![0]);
    }

    #[test]
    fn test_to_bytes_nullable_some_u64() {
        let key = NullableKey::ByOptionalNum(Some(99));
        let mut expected = vec![1u8];
        expected.extend_from_slice(&99u64.to_be_bytes());
        assert_eq!(key.to_bytes(), expected);
    }

    #[test]
    fn test_is_nullable_returns_false_without_attribute() {
        assert!(!SimpleKey::ByName("x".to_string()).is_nullable());
        assert!(!SimpleKey::ById(1).is_nullable());
        assert!(!UnitKey::All.is_nullable());
    }
}
