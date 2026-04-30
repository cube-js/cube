use crate::cube_bridge::join_hints::JoinHintItem;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct JoinHints {
    items: Vec<JoinHintItem>,
}

impl JoinHints {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn from_items(items: Vec<JoinHintItem>) -> Self {
        Self { items }
    }

    pub fn push(&mut self, item: JoinHintItem) {
        if let JoinHintItem::Single(ref name) = item {
            if let Some(last) = self.items.last() {
                let redundant = match last {
                    JoinHintItem::Single(s) => s == name,
                    JoinHintItem::Vector(v) => v.last() == Some(name),
                };
                if redundant {
                    return;
                }
            }
        }
        self.items.push(item);
    }

    pub fn extend(&mut self, other: &JoinHints) {
        for item in &other.items {
            self.push(item.clone());
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

    fn s(name: &str) -> JoinHintItem {
        JoinHintItem::Single(name.to_string())
    }

    fn v(names: &[&str]) -> JoinHintItem {
        JoinHintItem::Vector(names.iter().map(|n| n.to_string()).collect())
    }

    #[test]
    fn test_from_items_preserves_order() {
        let hints = JoinHints::from_items(vec![s("orders"), v(&["users", "orders"]), s("abc")]);

        assert_eq!(hints.len(), 3);
        assert_eq!(hints.items()[0], s("orders"));
        assert_eq!(hints.items()[1], v(&["users", "orders"]));
        assert_eq!(hints.items()[2], s("abc"));
    }

    #[test]
    fn test_push_and_extend() {
        let mut a = JoinHints::new();
        assert!(a.is_empty());

        a.push(s("orders"));
        a.push(s("abc"));
        assert_eq!(a.len(), 2);

        let b = JoinHints::from_items(vec![s("zzz"), v(&["a", "b"])]);
        a.extend(&b);
        assert_eq!(a.len(), 4);
        assert_eq!(a.items()[0], s("orders"));
        assert_eq!(a.items()[1], s("abc"));
        assert_eq!(a.items()[2], s("zzz"));
        assert_eq!(a.items()[3], v(&["a", "b"]));
    }

    #[test]
    fn test_extend_skips_redundant_at_boundary() {
        let mut a = JoinHints::new();
        a.push(s("orders"));

        let b = JoinHints::from_items(vec![s("orders"), s("abc")]);
        a.extend(&b);
        assert_eq!(a.len(), 2);
        assert_eq!(a.items()[0], s("orders"));
        assert_eq!(a.items()[1], s("abc"));

        let mut c = JoinHints::new();
        c.push(v(&["x", "abc"]));
        let d = JoinHints::from_items(vec![s("abc"), s("zzz")]);
        c.extend(&d);
        assert_eq!(
            c.len(),
            2,
            "Single after Vector ending with same name is skipped on extend"
        );
        assert_eq!(c.items()[0], v(&["x", "abc"]));
        assert_eq!(c.items()[1], s("zzz"));
    }

    #[test]
    fn test_push_skips_redundant_single() {
        let mut hints = JoinHints::new();
        hints.push(s("orders"));
        hints.push(s("orders"));
        assert_eq!(hints.len(), 1);

        hints.push(v(&["users", "orders"]));
        hints.push(s("orders"));
        assert_eq!(
            hints.len(),
            2,
            "Single after Vector ending with same name is skipped"
        );

        hints.push(s("abc"));
        assert_eq!(hints.len(), 3, "Different Single is added");
    }

    #[test]
    fn test_into_items_and_into_iter() {
        let hints = JoinHints::from_items(vec![s("b"), s("a"), v(&["x", "y"])]);
        let cloned = hints.clone();

        let collected: Vec<_> = cloned.into_iter().collect();
        assert_eq!(collected.len(), 3);

        let items = hints.into_items();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], s("b"));
        assert_eq!(items[1], s("a"));
    }
}
