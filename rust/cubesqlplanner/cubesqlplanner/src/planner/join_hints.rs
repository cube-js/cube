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
