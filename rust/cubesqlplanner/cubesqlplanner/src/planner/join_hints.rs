use crate::cube_bridge::join_hints::JoinHintItem;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct JoinHints {
    items: Vec<JoinHintItem>,
}

impl JoinHints {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn from_items(mut items: Vec<JoinHintItem>) -> Self {
        items.sort();
        items.dedup();
        Self { items }
    }

    pub fn insert(&mut self, item: JoinHintItem) {
        match self.items.binary_search(&item) {
            Ok(_) => {}
            Err(pos) => self.items.insert(pos, item),
        }
    }

    pub fn extend(&mut self, other: &JoinHints) {
        for item in other.items.iter() {
            self.insert(item.clone());
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn items(&self) -> &[JoinHintItem] {
        &self.items
    }

    pub fn iter(&self) -> std::slice::Iter<'_, JoinHintItem> {
        self.items.iter()
    }

    pub fn into_items(self) -> Vec<JoinHintItem> {
        self.items
    }
}

impl IntoIterator for JoinHints {
    type Item = JoinHintItem;
    type IntoIter = std::vec::IntoIter<JoinHintItem>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<'a> IntoIterator for &'a JoinHints {
    type Item = &'a JoinHintItem;
    type IntoIter = std::slice::Iter<'a, JoinHintItem>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn s(name: &str) -> JoinHintItem {
        JoinHintItem::Single(name.to_string())
    }

    fn v(names: &[&str]) -> JoinHintItem {
        JoinHintItem::Vector(names.iter().map(|n| n.to_string()).collect())
    }

    fn hash_of(h: &JoinHints) -> u64 {
        let mut hasher = DefaultHasher::new();
        h.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_from_items_normalizes_and_deduplicates() {
        let hints = JoinHints::from_items(vec![
            s("orders"),
            v(&["users", "orders"]),
            s("orders"),
            s("abc"),
        ]);

        assert_eq!(hints.len(), 3);
        // sorted: Single comes before Vector for same content, and Singles are alphabetical
        assert_eq!(hints.items()[0], s("abc"));
        assert_eq!(hints.items()[1], s("orders"));
        assert_eq!(hints.items()[2], v(&["users", "orders"]));

        // Different insertion order → same result
        let hints2 = JoinHints::from_items(vec![s("abc"), v(&["users", "orders"]), s("orders")]);
        assert_eq!(hints, hints2);
        assert_eq!(hash_of(&hints), hash_of(&hints2));
    }

    #[test]
    fn test_insert_and_extend_preserve_invariant() {
        let mut a = JoinHints::new();
        assert!(a.is_empty());

        a.insert(s("orders"));
        a.insert(s("abc"));
        a.insert(s("orders")); // duplicate
        assert_eq!(a.len(), 2);
        assert_eq!(a.items()[0], s("abc"));
        assert_eq!(a.items()[1], s("orders"));

        let b = JoinHints::from_items(vec![s("orders"), v(&["a", "b"]), s("zzz")]);
        a.extend(&b);
        assert_eq!(a.len(), 4);
        // abc, orders, zzz (Singles sorted), then Vector
        assert_eq!(a.items()[0], s("abc"));
        assert_eq!(a.items()[1], s("orders"));
        assert_eq!(a.items()[2], s("zzz"));
        assert_eq!(a.items()[3], v(&["a", "b"]));
    }

    #[test]
    fn test_into_items_and_into_iter() {
        let hints = JoinHints::from_items(vec![s("b"), s("a"), v(&["x", "y"])]);
        let cloned = hints.clone();

        // into_iter
        let collected: Vec<_> = cloned.into_iter().collect();
        assert_eq!(collected.len(), 3);

        // into_items
        let items = hints.into_items();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], s("a"));
        assert_eq!(items[1], s("b"));
    }
}
