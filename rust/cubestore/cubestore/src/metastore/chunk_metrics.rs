use super::{Chunk, IdRow};
use crate::app_metrics;
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkCount {
    pub chunks: u64,
    pub rows: u64,
}

/// Chunk-table summary gathered while another pass scans every chunk row.
#[derive(Default, Debug)]
pub struct ChunkStats {
    /// Indexed by `[in_memory][active]`.
    pub counts: [[ChunkCount; 2]; 2],
    pub oldest_active_in_memory_created_at: Option<DateTime<Utc>>,
    /// Indexed by `in_memory`.
    pub oldest_deactivated_at: [Option<DateTime<Utc>>; 2],
    pub active_in_memory_by_partition: HashMap<u64, u64>,
}

impl ChunkStats {
    pub fn observe(&mut self, chunk: &IdRow<Chunk>) {
        let c = chunk.get_row();
        let in_memory = c.in_memory() as usize;
        let count = &mut self.counts[in_memory][c.active() as usize];
        count.chunks += 1;
        count.rows += c.get_row_count();

        if c.active() {
            if c.in_memory() {
                *self
                    .active_in_memory_by_partition
                    .entry(c.get_partition_id())
                    .or_default() += 1;
                min_time(
                    &mut self.oldest_active_in_memory_created_at,
                    *c.created_at(),
                );
            }
        } else {
            // A chunk deactivated by an older binary has no deactivation time.
            let deactivated_at = c.deactivated_at().or(*c.created_at());
            min_time(&mut self.oldest_deactivated_at[in_memory], deactivated_at);
        }
    }

    /// `node_by_partition` maps the partitions of `active_in_memory_by_partition` to their owner.
    pub fn report(&self, node_by_partition: &HashMap<u64, String>) {
        let now = Utc::now();
        let mut chunks = Vec::with_capacity(4);
        let mut rows = Vec::with_capacity(4);
        for in_memory in [false, true] {
            for active in [false, true] {
                let tags = vec![
                    format!("in_memory:{}", in_memory),
                    format!("active:{}", active),
                ];
                let count = self.counts[in_memory as usize][active as usize];
                chunks.push((tags.clone(), count.chunks as i64));
                rows.push((tags, count.rows as i64));
            }
        }
        app_metrics::METASTORE_CHUNKS_FAMILY.report(chunks);
        app_metrics::METASTORE_CHUNKS_ROWS_FAMILY.report(rows);

        let mut by_node: HashMap<&str, i64> = HashMap::new();
        for (partition_id, count) in &self.active_in_memory_by_partition {
            if let Some(node) = node_by_partition.get(partition_id) {
                *by_node.entry(node.as_str()).or_default() += *count as i64;
            }
        }
        app_metrics::METASTORE_IN_MEMORY_CHUNKS_BY_NODE_FAMILY.report(
            by_node
                .into_iter()
                .map(|(node, count)| (vec![format!("node:{}", node)], count)),
        );

        app_metrics::METASTORE_OLDEST_ACTIVE_IN_MEMORY_CHUNK_AGE_SECS
            .report(age_secs(now, self.oldest_active_in_memory_created_at));
        app_metrics::METASTORE_OLDEST_INACTIVE_CHUNK_AGE_SECS_FAMILY.report(
            [false, true].into_iter().map(|in_memory| {
                (
                    vec![format!("in_memory:{}", in_memory)],
                    age_secs(now, self.oldest_deactivated_at[in_memory as usize]),
                )
            }),
        );
    }
}

fn min_time(current: &mut Option<DateTime<Utc>>, candidate: Option<DateTime<Utc>>) {
    if let Some(candidate) = candidate {
        if current.map_or(true, |c| candidate < c) {
            *current = Some(candidate);
        }
    }
}

fn age_secs(now: DateTime<Utc>, since: Option<DateTime<Utc>>) -> i64 {
    since.map_or(0, |t| now.signed_duration_since(t).num_seconds().max(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn chunk(
        id: u64,
        partition_id: u64,
        rows: usize,
        in_memory: bool,
        active: bool,
    ) -> IdRow<Chunk> {
        let mut c = Chunk::new(partition_id, rows, None, None, in_memory);
        if active {
            c = c.set_uploaded(true);
        } else {
            c = c.set_uploaded(true).deactivate();
        }
        IdRow::new(id, c)
    }

    #[test]
    fn chunk_stats_count_by_state() {
        let mut stats = ChunkStats::default();
        let rows = [
            chunk(1, 10, 5, true, true),
            chunk(2, 10, 7, true, true),
            chunk(3, 11, 1, true, true),
            chunk(4, 10, 3, true, false),
            chunk(5, 12, 100, false, true),
            chunk(6, 12, 50, false, false),
        ];
        for c in &rows {
            stats.observe(c);
        }

        assert_eq!(
            stats.counts[1][1],
            ChunkCount {
                chunks: 3,
                rows: 13
            }
        );
        assert_eq!(stats.counts[1][0], ChunkCount { chunks: 1, rows: 3 });
        assert_eq!(
            stats.counts[0][1],
            ChunkCount {
                chunks: 1,
                rows: 100
            }
        );
        assert_eq!(
            stats.counts[0][0],
            ChunkCount {
                chunks: 1,
                rows: 50
            }
        );
        assert_eq!(
            stats.active_in_memory_by_partition,
            HashMap::from([(10, 2), (11, 1)])
        );
        assert!(stats.oldest_active_in_memory_created_at.is_some());
        assert!(stats.oldest_deactivated_at[0].is_some());
        assert!(stats.oldest_deactivated_at[1].is_some());
    }

    #[test]
    fn chunk_stats_track_oldest() {
        let now = Utc::now();
        let mut stats = ChunkStats::default();
        min_time(&mut stats.oldest_deactivated_at[1], Some(now));
        min_time(
            &mut stats.oldest_deactivated_at[1],
            Some(now - Duration::seconds(90)),
        );
        min_time(
            &mut stats.oldest_deactivated_at[1],
            Some(now - Duration::seconds(30)),
        );
        assert_eq!(age_secs(now, stats.oldest_deactivated_at[1]), 90);
        assert_eq!(age_secs(now, None), 0);
    }
}
