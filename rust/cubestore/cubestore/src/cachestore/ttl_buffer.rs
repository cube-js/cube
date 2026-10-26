use crate::cachestore::cache_eviction_manager::CachePolicyData;
use crate::metastore::RocksSecondaryIndexValueVersionEncoder;
use chrono::Utc;
use deepsize::DeepSizeOf;
use std::hash::BuildHasher;

const NIL: u32 = u32::MAX;

#[derive(Debug)]
struct Node {
    row_id: u64,
    data: CachePolicyData,
    /// Previous node in the queue; unused while the node is free
    prev: u32,
    /// Next node in the queue, or the next free node while the node is free
    next: u32,
}

/// Pending LRU/LFU updates, flushed in the order keys became dirty, so every key waits behind the
/// keys queued before it instead of being picked in hash-bucket order, which could starve some keys
/// forever. Updates of a queued key keep its position.
///
/// Nodes live in one `Vec` linked by `u32` indexes and freed slots are reused, so there is no
/// allocation per key and the memory is bounded by the peak length. The hash index stores only node
/// indexes and compares `row_id` through the node, so a slot is 4 bytes instead of 16 for
/// `HashMap<u64, u32>`.
#[derive(Debug)]
pub struct TTLBuffer {
    index: hashbrown::HashTable<u32>,
    hasher: hashbrown::DefaultHashBuilder,
    nodes: Vec<Node>,
    free: u32,
    head: u32,
    tail: u32,
}

impl TTLBuffer {
    pub fn new() -> Self {
        Self {
            index: hashbrown::HashTable::new(),
            hasher: hashbrown::DefaultHashBuilder::default(),
            nodes: Vec::new(),
            free: NIL,
            head: NIL,
            tail: NIL,
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
        // NIL is reserved, so indexes stay below it
        if self.len() >= max_size.min(NIL as usize) {
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
            next: NIL,
        };

        let i = if self.free != NIL {
            let i = self.free;
            self.free = self.nodes[i as usize].next;
            self.nodes[i as usize] = node;
            i
        } else {
            self.nodes.push(node);
            (self.nodes.len() - 1) as u32
        };

        if self.tail == NIL {
            self.head = i;
        } else {
            self.nodes[self.tail as usize].next = i;
        }
        self.tail = i;

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
        while batch.len() < batch_size && self.head != NIL {
            let i = self.head;
            let node = &self.nodes[i as usize];
            batch.push((node.row_id, node.data.clone()));

            let row_id = node.row_id;
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
        let (prev, next) = {
            let node = &self.nodes[i as usize];
            (node.prev, node.next)
        };

        if prev == NIL {
            self.head = next;
        } else {
            self.nodes[prev as usize].next = next;
        }

        if next == NIL {
            self.tail = prev;
        } else {
            self.nodes[next as usize].prev = prev;
        }

        self.nodes[i as usize].next = self.free;
        self.free = i;
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

        // 16384 nodes of 40 bytes + 32768 index buckets of a u32 and a control byte, plus the
        // trailing control group and alignment
        let size = buf.deep_size_of() - std::mem::size_of::<TTLBuffer>();
        let expected = 16_384 * 40 + 32_768 * (4 + 1);
        assert!(
            size >= expected && size <= expected + 64,
            "{} vs {}",
            size,
            expected
        );
    }
}
