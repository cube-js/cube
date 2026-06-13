// Reproduces the main/router-node memory spike: the disk-space limit check
// (`get_used_disk_space_out_of_queue`) materializes ALL partitions + ALL chunks
// of the tenant into RAM, runs out-of-queue (concurrent, ungated), and its cache
// has no single-flight. So N concurrent callers (a burst of chunk writes during a
// CSV pre-aggregation import, all missing a cold cache) each hold a full copy at
// once => peak memory grows ~linearly with concurrency = the 20-24GB spikes.
//
// Run on Linux (Docker) for a faithful RSS / OOM-kill picture; glibc malloc + the
// `malloc_trim` loop are what make the spike retract on the production graphs.
//
//   PARTITIONS=2000 CHUNKS=300000 CACHE_SECS=0 WITH_MINMAX=0 \
//     cargo bench -p cubestore --bench disk_space_stampede
//
// Knobs (env): PARTITIONS, CHUNKS, CACHE_SECS, WITH_MINMAX, CONCURRENCY (csv).

use cubestore::config::Config;
use cubestore::metastore::{
    BaseRocksStoreFs, Chunk, Column, ColumnType, MetaStore, Partition, RocksMetaStore,
};
use cubestore::remotefs::LocalDirRemoteFs;
use cubestore::table::{Row, TableValue};
use cubestore::CubeError;
use std::env;
use std::fs;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

mod tracking_allocator;
use tracking_allocator::TrackingAllocator;

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / 1024.0 / 1024.0
}

fn build_metastore(name: &str, cache_secs: u64) -> Arc<RocksMetaStore> {
    let mut obj = Config::test_config_obj(name);
    obj.disk_space_cache_duration_secs = cache_secs;
    let config = Config::make_test_config(obj);

    let base = env::current_dir().unwrap().join("target").join("bench");
    let store_path = base.join(format!("stampede-local-{}", name));
    let remote_store_path = base.join(format!("stampede-remote-{}", name));
    let _ = fs::remove_dir_all(&store_path);
    let _ = fs::remove_dir_all(&remote_store_path);

    let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path), store_path.clone());
    RocksMetaStore::new(
        store_path.join("metastore").as_path(),
        BaseRocksStoreFs::new_for_metastore(remote_fs, config.config_obj()),
        config.config_obj(),
    )
    .unwrap()
}

async fn populate(
    ms: &Arc<RocksMetaStore>,
    partitions: usize,
    chunks_total: usize,
    with_minmax: bool,
) -> Result<(), CubeError> {
    ms.create_schema("s".to_string(), false).await?;
    let cols = vec![
        Column::new("name".to_string(), ColumnType::String, 0),
        Column::new("ts".to_string(), ColumnType::Timestamp, 1),
    ];
    ms.create_table(
        "s".to_string(),
        "t".to_string(),
        cols,
        None,
        None,
        vec![],
        true,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        None,
    )
    .await?;

    let p1 = ms.get_partition(1).await?;
    let index_id = p1.get_row().get_index_id();
    let mut part_ids = vec![p1.get_id()];
    for _ in 1..partitions {
        let p = ms
            .create_partition(Partition::new(index_id, None, None, None))
            .await?;
        part_ids.push(p.get_id());
    }

    let mk_row = |i: usize| {
        Some(Row::new(vec![
            TableValue::String(format!("key-{:024}", i)),
            TableValue::Int(i as i64),
        ]))
    };

    let batch = 10_000usize;
    let mut made = 0usize;
    while made < chunks_total {
        let n = batch.min(chunks_total - made);
        let chunks: Vec<Chunk> = (0..n)
            .map(|i| {
                let pid = part_ids[(made + i) % part_ids.len()];
                let (min, max) = if with_minmax {
                    (mk_row(made + i), mk_row(made + i + 1))
                } else {
                    (None, None)
                };
                Chunk::new(pid, 1000, min, max, false)
            })
            .collect();
        ms.insert_chunks(chunks).await?;
        made += n;
    }
    println!(
        "populated: {} partitions, {} chunks (with_minmax={})",
        partitions, chunks_total, with_minmax
    );
    Ok(())
}

async fn run_concurrent(ms: &Arc<RocksMetaStore>, k: usize) {
    let handles: Vec<_> = (0..k)
        .map(|_| {
            let m = ms.clone();
            tokio::spawn(async move { m.get_used_disk_space_out_of_queue(None).await })
        })
        .collect();
    for h in handles {
        h.await.unwrap().unwrap();
    }
}

async fn run_sequential(ms: &Arc<RocksMetaStore>, k: usize) {
    for _ in 0..k {
        ms.get_used_disk_space_out_of_queue(None).await.unwrap();
    }
}

fn parse_concurrency() -> Vec<usize> {
    env::var("CONCURRENCY")
        .ok()
        .map(|v| {
            v.split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| vec![1, 2, 4, 8, 16])
}

fn main() {
    let partitions = env_usize("PARTITIONS", 2000);
    let chunks = env_usize("CHUNKS", 300_000);
    let cache_secs = env_usize("CACHE_SECS", 0) as u64;
    let with_minmax = env_usize("WITH_MINMAX", 0) != 0;
    let concurrency = parse_concurrency();

    let runtime: Runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    let ms = runtime.block_on(async {
        let ms = build_metastore("disk_space_stampede", cache_secs);
        populate(&ms, partitions, chunks, with_minmax)
            .await
            .unwrap();
        ms
    });

    // Sequential baseline: with serialized calls only one scan is ever live, so
    // peak ~= a single scan regardless of how many calls (the cost we should pay).
    ALLOCATOR.reset_stats();
    runtime.block_on(run_sequential(&ms, *concurrency.iter().max().unwrap()));
    let seq_peak = ALLOCATOR.peak_allocated();

    println!(
        "\n=== disk-space-check stampede (cache_secs={}) ===",
        cache_secs
    );
    println!(
        "{:>5}  {:>12}  {:>16}",
        "K", "peak (MB)", "total alloc (MB)"
    );
    for &k in concurrency.iter() {
        ALLOCATOR.reset_stats();
        runtime.block_on(run_concurrent(&ms, k));
        let peak = mb(ALLOCATOR.peak_allocated());
        let total = mb(ALLOCATOR.total_allocated());
        println!("{:>5}  {:>12.1}  {:>16.1}", k, peak, total);
    }
    println!(
        "\nsequential K={} peak: {:.1} MB",
        concurrency.iter().max().unwrap(),
        mb(seq_peak)
    );
    println!(
        "peak (MB) = max live memory: flat in K => no memory stampede (streaming scan).\n\
         total alloc (MB) = work done: flat in K with cache_secs>0 => single-flight\n\
         dedup (one scan shared by all waiters); ~linear in K would mean every caller scanned."
    );
}
