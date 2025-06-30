use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use cubestore::cachestore::{CacheStore, QueueAddPayload, QueueItemStatus, RocksCacheStore};
use cubestore::config::{Config, CubeServices};
use cubestore::CubeError;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

fn prepare_benchmark(name: &str) -> Result<Arc<RocksCacheStore>, CubeError> {
    let config = Config::test(&name).update_config(|mut config| {
        // disable periodic eviction
        config.cachestore_cache_eviction_loop_interval = 100000;

        config
    });

    let (_, cachestore) = RocksCacheStore::prepare_bench_cachestore(&name, config);

    let cachestore_to_move = cachestore.clone();

    tokio::task::spawn(async move {
        let loops = cachestore_to_move.spawn_processing_loops();
        CubeServices::wait_loops(loops).await
    });

    Ok(cachestore)
}

async fn do_insert(
    cachestore: &Arc<RocksCacheStore>,
    total: usize,
    size_kb: usize,
    queue_path: &str,
) {
    for i in 0..total {
        let fut = cachestore.queue_add(QueueAddPayload {
            path: format!("{}:{}", queue_path, i),
            value: "a".repeat(size_kb * 1024), // size in bytes
            priority: 0,
            orphaned: None,
        });

        let res = fut.await;
        assert!(res.is_ok());
    }
}

fn do_insert_bench(c: &mut Criterion, runtime: &Runtime, total: usize, size_kb: usize) {
    let cachestore = runtime.block_on(async {
        prepare_benchmark(&format!("cachestore_queue_insert_{}", size_kb)).unwrap()
    });

    c.bench_with_input(
        BenchmarkId::new(
            format!("insert queues:1, total:{}, size:{} kb", total, size_kb),
            total,
        ),
        &(total, size_kb),
        |b, (total, size_kb)| {
            b.to_async(runtime)
                .iter(|| do_insert(&cachestore, *total, *size_kb, "queue:1"));
        },
    );
}

async fn do_list(cachestore: &Arc<RocksCacheStore>, total: usize) {
    for _ in 0..total {
        let fut = cachestore.queue_list(
            "queue:1".to_string(),
            Some(QueueItemStatus::Pending),
            true,
            false,
        );

        let res = fut.await;
        assert!(res.is_ok());
    }
}

fn do_list_bench(
    c: &mut Criterion,
    runtime: &Runtime,
    in_queue: usize,
    size_kb: usize,
    total: usize,
) {
    let cachestore = runtime.block_on(async {
        let cachestore = prepare_benchmark(&format!("cachestore_queue_list_{}", size_kb)).unwrap();

        do_insert(&cachestore, in_queue, size_kb, "queue:1").await;
        do_insert(&cachestore, in_queue, size_kb, "queue:2").await;
        do_insert(&cachestore, in_queue, size_kb, "queue:3").await;
        do_insert(&cachestore, in_queue, size_kb, "queue:4").await;
        do_insert(&cachestore, in_queue, size_kb, "queue:5").await;

        cachestore
    });

    c.bench_with_input(
        BenchmarkId::new(
            format!("list queues:1, total:{}, size:{} kb", total, size_kb),
            total,
        ),
        &total,
        |b, total| {
            b.to_async(runtime).iter(|| do_list(&cachestore, *total));
        },
    );
}

fn do_benches(c: &mut Criterion) {
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    do_insert_bench(c, &runtime, 1_000, 64);
    do_insert_bench(c, &runtime, 1_000, 256);
    do_insert_bench(c, &runtime, 1_000, 512);

    do_list_bench(c, &runtime, 10_000, 128, 128);
}

criterion_group!(benches, do_benches);
criterion_main!(benches);
