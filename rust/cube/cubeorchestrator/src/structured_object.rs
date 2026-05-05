//! IndexMap-shaped object with key names held once via `Arc<StructuredObjectShape>`
//! and per-instance values stored in a position-aligned `Vec`.
//!
//! Built so result-set rows can share a single key list instead of cloning the
//! same column names per row.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};

/// Shared, refcounted handle to a `StructuredObjectShape`. Every
/// `StructuredObject` row in a result set holds one of these instead of cloning
/// the key list.
pub type StructuredObjectShapeRef = Arc<StructuredObjectShape>;

#[derive(Debug, Clone)]
pub struct StructuredObjectShape {
    keys: Vec<String>,
    index: HashMap<String, usize>,
}

impl StructuredObjectShape {
    pub fn builder() -> StructuredObjectShapeBuilder {
        StructuredObjectShapeBuilder::default()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    pub fn position(&self, key: &str) -> Option<usize> {
        self.index.get(key).copied()
    }
}

#[derive(Debug, Default)]
pub struct StructuredObjectShapeBuilder {
    keys: Vec<String>,
    index: HashMap<String, usize>,
}

impl StructuredObjectShapeBuilder {
    /// Append a key. If the key already exists, its existing position is returned
    /// — the shape stays a unique, ordered list.
    pub fn insert(&mut self, key: impl Into<String>) -> usize {
        let key = key.into();

        if let Some(&i) = self.index.get(&key) {
            return i;
        }

        let i = self.keys.len();
        self.index.insert(key.clone(), i);
        self.keys.push(key);
        i
    }

    pub fn position(&self, key: &str) -> Option<usize> {
        self.index.get(key).copied()
    }

    pub fn build(self) -> StructuredObjectShape {
        StructuredObjectShape {
            keys: self.keys,
            index: self.index,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StructuredObject<V: Debug + Clone = crate::query_result_transform::DBResponsePrimitive> {
    shape: StructuredObjectShapeRef,
    values: Vec<V>,
}

impl<V: Debug + Clone> StructuredObject<V> {
    pub fn shape(&self) -> &StructuredObjectShapeRef {
        &self.shape
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.shape.position(key).map(|i| &self.values[i])
    }

    /// Overwrite the slot for `key`. Returns the previous value if `key` is in the
    /// shape, or `None` (and leaves `self` unchanged) if it isn't.
    pub fn insert(&mut self, key: &str, value: V) -> Option<V> {
        let idx = self.shape.position(key)?;
        Some(std::mem::replace(&mut self.values[idx], value))
    }

    /// Fast-path setter for callers that already know the position (e.g. via a plan).
    /// Panics if `idx >= len()`.
    pub fn set_by_position(&mut self, idx: usize, value: V) {
        self.values[idx] = value;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &V)> {
        self.shape
            .keys
            .iter()
            .map(|k| k.as_str())
            .zip(self.values.iter())
    }

    pub fn values(&self) -> &[V] {
        &self.values
    }

    pub fn with_shape_filled(shape: StructuredObjectShapeRef, fill: V) -> Self {
        let len = shape.len();
        Self {
            shape,
            values: vec![fill; len],
        }
    }
}

impl<V: Debug + Clone + Default> StructuredObject<V> {
    pub fn with_shape_default(shape: StructuredObjectShapeRef) -> Self {
        let len = shape.len();
        Self {
            shape,
            values: vec![V::default(); len],
        }
    }
}

impl<V: Debug + Clone + Serialize> Serialize for StructuredObject<V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.values.len()))?;
        for (k, v) in self.iter() {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

impl<'de, V> Deserialize<'de> for StructuredObject<V>
where
    V: Debug + Clone + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor<V>(std::marker::PhantomData<V>);

        impl<'de, V> Visitor<'de> for ObjectVisitor<V>
        where
            V: Debug + Clone + Deserialize<'de>,
        {
            type Value = StructuredObject<V>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object representing a StructuredObject")
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut shape = StructuredObjectShape::builder();
                let mut values: Vec<V> = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((k, v)) = map.next_entry::<String, V>()? {
                    let idx = shape.insert(k);
                    if idx == values.len() {
                        values.push(v);
                    } else {
                        // Duplicate key: overwrite the existing slot to mirror IndexMap.
                        values[idx] = v;
                    }
                }
                Ok(StructuredObject {
                    shape: Arc::new(shape.build()),
                    values,
                })
            }
        }

        deserializer.deserialize_map(ObjectVisitor::<V>(std::marker::PhantomData))
    }
}

impl<V: Debug + Clone + PartialEq> PartialEq for StructuredObject<V> {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }
        // Compare by (key, value) pairs in shape order. This avoids requiring shape
        // pointer identity while still respecting key order — matches IndexMap semantics.
        self.iter().eq(other.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape_of(keys: &[&str]) -> StructuredObjectShapeRef {
        let mut b = StructuredObjectShape::builder();
        for k in keys {
            b.insert(*k);
        }
        Arc::new(b.build())
    }

    #[test]
    fn shape_dedups_and_preserves_order() {
        let mut b = StructuredObjectShape::builder();
        assert_eq!(b.insert("a"), 0);
        assert_eq!(b.insert("b"), 1);
        assert_eq!(b.insert("a"), 0);
        let s = b.build();
        assert_eq!(s.keys(), &["a".to_string(), "b".to_string()]);
        assert_eq!(s.position("a"), Some(0));
        assert_eq!(s.position("b"), Some(1));
        assert_eq!(s.position("c"), None);
    }

    #[test]
    fn insert_and_get() {
        let shape = shape_of(&["x", "y"]);
        let mut obj: StructuredObject<i64> = StructuredObject::with_shape_default(shape);
        assert_eq!(obj.get("x"), Some(&0));
        let prev = obj.insert("x", 42);
        assert_eq!(prev, Some(0));
        assert_eq!(obj.get("x"), Some(&42));
        assert_eq!(obj.insert("missing", 1), None);
    }

    #[test]
    fn iter_in_shape_order() {
        let shape = shape_of(&["a", "b", "c"]);
        let mut obj: StructuredObject<i64> = StructuredObject::with_shape_default(shape);
        obj.set_by_position(0, 1);
        obj.set_by_position(1, 2);
        obj.set_by_position(2, 3);
        let collected: Vec<_> = obj.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        assert_eq!(
            collected,
            vec![
                ("a".to_string(), 1),
                ("b".to_string(), 2),
                ("c".to_string(), 3),
            ]
        );
    }

    #[test]
    fn serializes_as_object_in_order() {
        let shape = shape_of(&["beta", "alpha"]);
        let mut obj: StructuredObject<i64> = StructuredObject::with_shape_default(shape);
        obj.set_by_position(0, 1);
        obj.set_by_position(1, 2);
        let json = serde_json::to_string(&obj).unwrap();
        // Order follows the shape, not lexicographic.
        assert_eq!(json, r#"{"beta":1,"alpha":2}"#);
    }

    #[test]
    fn equality_compares_pairs_in_order() {
        let s1 = shape_of(&["a", "b"]);
        let s2 = shape_of(&["a", "b"]);
        let mut o1: StructuredObject<i64> = StructuredObject::with_shape_default(s1);
        let mut o2: StructuredObject<i64> = StructuredObject::with_shape_default(s2);
        o1.set_by_position(0, 1);
        o1.set_by_position(1, 2);
        o2.set_by_position(0, 1);
        o2.set_by_position(1, 2);
        assert_eq!(o1, o2);
        o2.set_by_position(1, 3);
        assert_ne!(o1, o2);
    }
}
