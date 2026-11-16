use std::{
    fmt,
    ops::Deref,
    sync::{Arc, OnceLock},
};

/// An immutable list that shares its sub-lists instead of copying them: copying a cons list's
/// tail into every cell makes analysis data quadratic. Elements are flattened lazily on read.
pub struct SharedList<T>(Arc<SharedListInner<T>>);

struct SharedListInner<T> {
    parts: Vec<Part<T>>,
    flat: OnceLock<Vec<T>>,
}

enum Part<T> {
    Item(T),
    List(SharedList<T>),
}

impl<T> Clone for SharedList<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Clone> SharedList<T> {
    pub fn builder() -> SharedListBuilder<T> {
        SharedListBuilder { parts: Vec::new() }
    }

    pub fn single(item: T) -> Self {
        let mut builder = Self::builder();
        builder.push(item);
        builder.build()
    }

    fn flatten(&self) -> Vec<T> {
        let mut result = Vec::new();
        // Explicit stack: a cons list nests as deep as it is long
        let mut stack = vec![self.0.parts.iter()];
        while let Some(parts) = stack.last_mut() {
            match parts.next() {
                None => {
                    stack.pop();
                }
                Some(Part::Item(item)) => result.push(item.clone()),
                Some(Part::List(list)) => match list.0.flat.get() {
                    Some(flat) => result.extend_from_slice(flat),
                    None => stack.push(list.0.parts.iter()),
                },
            }
        }
        result.shrink_to_fit();
        result
    }
}

impl<T: Clone> Deref for SharedList<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.0.flat.get_or_init(|| self.flatten())
    }
}

impl<'a, T: Clone> IntoIterator for &'a SharedList<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T: Clone + fmt::Debug> fmt::Debug for SharedList<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl<T> Drop for SharedListInner<T> {
    fn drop(&mut self) {
        // Unlinks uniquely owned nested lists iteratively, so dropping a long cons list does not
        // recurse once per cell
        let mut pending = std::mem::take(&mut self.parts);
        while let Some(part) = pending.pop() {
            if let Part::List(list) = part {
                // Unlike try_unwrap, into_inner never leaves the last reference to another thread
                if let Some(mut inner) = Arc::into_inner(list.0) {
                    pending.append(&mut inner.parts);
                }
            }
        }
    }
}

pub struct SharedListBuilder<T> {
    parts: Vec<Part<T>>,
}

impl<T: Clone> SharedListBuilder<T> {
    pub fn push(&mut self, item: T) {
        self.parts.push(Part::Item(item));
    }

    pub fn extend(&mut self, list: SharedList<T>) {
        if !list.0.parts.is_empty() {
            self.parts.push(Part::List(list));
        }
    }

    pub fn build(mut self) -> SharedList<T> {
        if let [Part::List(_)] = self.parts.as_slice() {
            let Some(Part::List(list)) = self.parts.pop() else {
                unreachable!()
            };
            return list;
        }
        self.parts.shrink_to_fit();
        SharedList(Arc::new(SharedListInner {
            parts: self.parts,
            flat: OnceLock::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_list_order() {
        let mut tail = SharedList::builder();
        tail.push(3);
        tail.push(4);
        let tail = tail.build();
        let mut list = SharedList::builder();
        list.push(1);
        list.extend(SharedList::builder().build());
        list.extend(tail.clone());
        list.push(5);
        list.extend(tail);
        assert_eq!(&*list.build(), &[1, 3, 4, 5, 3, 4]);
    }

    #[test]
    fn test_shared_list_deep() {
        let mut list = SharedList::builder().build();
        for i in 0..1_000_000 {
            let mut cell = SharedList::builder();
            cell.push(i);
            cell.extend(list);
            list = cell.build();
        }
        assert_eq!(list.len(), 1_000_000);
        assert_eq!(list[0], 999_999);
    }
}
