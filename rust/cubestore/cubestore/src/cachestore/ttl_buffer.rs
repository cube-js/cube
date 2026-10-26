use crate::cachestore::cache_eviction_manager::CachePolicyData;
use crate::metastore::RocksSecondaryIndexValueVersionEncoder;
use chrono::Utc;
use deepsize::DeepSizeOf;
use std::hash::BuildHasher;

/// `Option<T>` packed into `T` itself: the last value, `T::max_value()`, stands for `None`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LastValueOption<T>(T);

impl<T: num::Bounded + Copy + PartialEq + std::fmt::Debug> LastValueOption<T> {
    fn none() -> Self {
        Self(T::max_value())
    }

    fn some(value: T) -> Self {
        debug_assert_ne!(value, T::max_value(), "the last value is reserved for None");
        Self(value)
    }

    fn get(self) -> Option<T> {
        if self.0 == T::max_value() {
            None
        } else {
            Some(self.0)
        }
    }
}

#[derive(Debug)]
struct Node {
    row_id: u64,
    data: CachePolicyData,
    /// Previous node in the queue; unused while the node is free
    prev: LastValueOption<u32>,
    /// Next node in the queue, or the next free node while the node is free
    next: LastValueOption<u32>,
}

/// Pending LRU/LFU updates, flushed in the order keys became dirty (updates keep a key's position).
/// Hash-bucket order would starve some keys forever, leaving a stale `lru` that eviction treats as coldest.
#[derive(Debug)]
pub struct TTLBuffer {
    index: hashbrown::HashTable<u32>,
    hasher: hashbrown::DefaultHashBuilder,
    nodes: Vec<Node>,
    free: LastValueOption<u32>,
    head: LastValueOption<u32>,
    tail: LastValueOption<u32>,
}

impl TTLBuffer {
    pub fn new() -> Self {
        Self {
            index: hashbrown::HashTable::new(),
            hasher: hashbrown::DefaultHashBuilder::default(),
            nodes: Vec::new(),
            free: LastValueOption::none(),
            head: LastValueOption::none(),
            tail: LastValueOption::none(),
        }
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    fn find(&self, row_id: u64) -> Option<u32> {
        let nodes = &self.nodes;
        self.index
            .find(self.hasher.hash_one(row_id), |&i| {
                nodes[i as usize].row_id == row_id
            })
            .copied()
    }

    #[cfg(test)]
    pub fn get(&self, row_id: &u64) -> Option<&CachePolicyData> {
        self.find(*row_id).map(|i| &self.nodes[i as usize].data)
    }

    #[cfg(test)]
    pub fn get_mut(&mut self, row_id: &u64) -> Option<&mut CachePolicyData> {
        self.find(*row_id).map(|i| &mut self.nodes[i as usize].data)
    }

    pub fn on_lookup(&mut self, max_size: usize, row_id: u64, key_hash: [u8; 8], raw_size: u32) {
        let now = Utc::now().encode_value_as_u32().unwrap();

        if let Some(i) = self.find(row_id) {
            let data = &mut self.nodes[i as usize].data;
            data.hits = data.hits.saturating_add(1);
            data.last_access = now;

            return;
        }

        // Drop new keys instead of evicting queued ones, so keys already waiting keep their turn.
        // u32::MAX is reserved for `LastValueOption::none()`, so indexes stay below it
        if self.len() >= max_size.min(u32::MAX as usize) {
            return;
        }

        let node = Node {
            row_id,
            data: CachePolicyData {
                key_hash,
                raw_size,
                hits: 1,
                first_access: now,
                last_access: now,
            },
            prev: self.tail,
            next: LastValueOption::none(),
        };

        let i = match self.free.get() {
            Some(i) => {
                self.free = self.nodes[i as usize].next;
                self.nodes[i as usize] = node;
                i
            }
            None => {
                self.nodes.push(node);
                (self.nodes.len() - 1) as u32
            }
        };

        match self.tail.get() {
            Some(tail) => self.nodes[tail as usize].next = LastValueOption::some(i),
            None => self.head = LastValueOption::some(i),
        }
        self.tail = LastValueOption::some(i);

        let (nodes, hasher) = (&self.nodes, &self.hasher);
        self.index.insert_unique(hasher.hash_one(row_id), i, |&j| {
            hasher.hash_one(nodes[j as usize].row_id)
        });
    }

    pub fn remove(&mut self, row_id: &u64) -> Option<CachePolicyData> {
        let i = self.remove_from_index(*row_id)?;
        self.unlink(i);

        Some(self.nodes[i as usize].data.clone())
    }

    pub fn take_batch(&mut self, batch_size: usize) -> Vec<(u64, CachePolicyData)> {
        let mut batch = Vec::with_capacity(batch_size.min(self.len()));
        while batch.len() < batch_size {
            let Some(i) = self.head.get() else {
                break;
            };

            let node = &self.nodes[i as usize];
            let row_id = node.row_id;
            batch.push((row_id, node.data.clone()));

            self.remove_from_index(row_id);
            self.unlink(i);
        }

        batch
    }

    fn remove_from_index(&mut self, row_id: u64) -> Option<u32> {
        let nodes = &self.nodes;
        let entry = self
            .index
            .find_entry(self.hasher.hash_one(row_id), |&i| {
                nodes[i as usize].row_id == row_id
            })
            .ok()?;

        Some(entry.remove().0)
    }

    /// Detaches the node from the queue and moves it to the free list
    fn unlink(&mut self, i: u32) {
        let Node { prev, next, .. } = self.nodes[i as usize];

        match prev.get() {
            Some(prev) => self.nodes[prev as usize].next = next,
            None => self.head = next,
        }

        match next.get() {
            Some(next) => self.nodes[next as usize].prev = prev,
            None => self.tail = prev,
        }

        self.nodes[i as usize].next = self.free;
        self.free = LastValueOption::some(i);
    }
}

impl Default for TTLBuffer {
    fn default() -> Self {
        Self::new()
    }
}

impl DeepSizeOf for TTLBuffer {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.index.allocation_size() + self.nodes.capacity() * std::mem::size_of::<Node>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metastore::{PackedDateTime, RocksSecondaryIndexValueVersionDecoder};

    fn lookup(buf: &mut TTLBuffer, max_size: usize, row_id: u64) {
        buf.on_lookup(max_size, row_id, row_id.to_be_bytes(), 1);
    }

    fn ids(batch: &[(u64, CachePolicyData)]) -> Vec<u64> {
        batch.iter().map(|(id, _)| *id).collect()
    }

    #[test]
    fn test_last_value_option() {
        assert_eq!(LastValueOption::<u32>::none().get(), None);
        assert_eq!(LastValueOption::some(0u32).get(), Some(0));
        assert_eq!(
            LastValueOption::some(u32::MAX - 1).get(),
            Some(u32::MAX - 1)
        );

        assert_eq!(LastValueOption::<u64>::none().get(), None);
        assert_eq!(LastValueOption::some(42u64).get(), Some(42));
        assert_eq!(std::mem::size_of::<LastValueOption<u64>>(), 8);
    }

    #[test]
    fn test_ttl_buffer_take_batch_fifo() {
        let mut buf = TTLBuffer::new();
        for id in 1..=300 {
            lookup(&mut buf, 1_000, id);
        }

        assert_eq!(ids(&buf.take_batch(150)), (1..=150).collect::<Vec<_>>());
        assert_eq!(ids(&buf.take_batch(150)), (151..=300).collect::<Vec<_>>());
        assert!(buf.take_batch(150).is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_ttl_buffer_update_keeps_position() {
        let mut buf = TTLBuffer::new();
        for id in 1..=3 {
            lookup(&mut buf, 1_000, id);
        }

        buf.get_mut(&1).unwrap().last_access = PackedDateTime::default();
        lookup(&mut buf, 1_000, 1);
        assert_eq!(buf.get(&1).unwrap().hits, 2);
        assert!(buf
            .get(&1)
            .unwrap()
            .last_access
            .decode_value_as_opt_datetime()
            .unwrap()
            .is_some());

        assert_eq!(ids(&buf.take_batch(10)), vec![1, 2, 3]);
    }

    #[test]
    fn test_ttl_buffer_redirtied_key_goes_to_tail() {
        let mut buf = TTLBuffer::new();
        for id in 1..=4 {
            lookup(&mut buf, 1_000, id);
        }

        assert_eq!(ids(&buf.take_batch(2)), vec![1, 2]);
        lookup(&mut buf, 1_000, 1);
        assert_eq!(ids(&buf.take_batch(10)), vec![3, 4, 1]);
    }

    #[test]
    fn test_ttl_buffer_delete_and_max_size() {
        let mut buf = TTLBuffer::new();
        for id in 1..=3 {
            lookup(&mut buf, 3, id);
        }

        // Full: new keys are dropped, queued keys are still updated
        lookup(&mut buf, 3, 4);
        lookup(&mut buf, 3, 2);
        assert_eq!(buf.len(), 3);
        assert!(buf.get(&4).is_none());
        assert_eq!(buf.get(&2).unwrap().hits, 2);

        assert_eq!(buf.remove(&2).unwrap().hits, 2);
        assert!(buf.remove(&2).is_none());
        lookup(&mut buf, 3, 5);
        assert_eq!(ids(&buf.take_batch(10)), vec![1, 3, 5]);
    }

    #[test]
    fn test_ttl_buffer_remove_head_and_tail() {
        let mut buf = TTLBuffer::new();
        for id in 1..=4 {
            lookup(&mut buf, 1_000, id);
        }

        buf.remove(&1);
        buf.remove(&4);
        lookup(&mut buf, 1_000, 5);
        assert_eq!(ids(&buf.take_batch(10)), vec![2, 3, 5]);

        buf.remove(&5);
        lookup(&mut buf, 1_000, 6);
        assert_eq!(ids(&buf.take_batch(10)), vec![6]);
    }

    #[test]
    fn test_ttl_buffer_reuses_freed_nodes() {
        let mut buf = TTLBuffer::new();
        for id in 1..=100 {
            lookup(&mut buf, 1_000, id);
        }

        let nodes_capacity = buf.nodes.capacity();
        assert_eq!(buf.take_batch(100).len(), 100);
        for id in 101..=200 {
            lookup(&mut buf, 1_000, id);
        }

        assert_eq!(buf.nodes.len(), 100);
        assert_eq!(buf.nodes.capacity(), nodes_capacity);
        assert_eq!(ids(&buf.take_batch(100)), (101..=200).collect::<Vec<_>>());
    }

    #[test]
    fn test_ttl_buffer_deep_size_of() {
        let mut buf = TTLBuffer::new();
        assert_eq!(buf.deep_size_of(), std::mem::size_of::<TTLBuffer>());

        for id in 1..=16_384 {
            lookup(&mut buf, 16_384, id);
        }

        // Each index bucket is a u32 slot plus a control byte, plus a trailing control group and
        // alignment
        let nodes = buf.nodes.capacity() * std::mem::size_of::<Node>();
        let table = buf.index.num_buckets() * (std::mem::size_of::<u32>() + 1);
        let size = buf.deep_size_of() - std::mem::size_of::<TTLBuffer>();
        assert!(
            size >= nodes + table && size <= nodes + table + 64,
            "{} vs {}",
            size,
            nodes + table
        );
    }
}
