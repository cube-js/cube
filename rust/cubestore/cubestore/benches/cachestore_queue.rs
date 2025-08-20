use criterion::{criterion_group, BenchmarkId, Criterion};
use cubestore::cachestore::{
    CacheStore, QueueAddPayload, QueueItemStatus, QueueKey, RocksCacheStore,
};
use cubestore::config::{Config, CubeServices};
use cubestore::CubeError;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};

mod tracking_allocator;

use tracking_allocator::TrackingAllocator;

#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator::new();

fn prepare_cachestore(name: &str) -> Result<Arc<RocksCacheStore>, CubeError> {
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

fn generate_queue_path(queue_path: &str, queue_id: usize) -> String {
    format!(
        "{}:{}",
        queue_path,
        format!("{:x}", md5::compute(queue_id.to_be_bytes()))
    )
}

async fn do_insert(
    cachestore_name: &str,
    cachestore: &Arc<RocksCacheStore>,
    total: usize,
    size_kb: usize,
    queue_path: &str,
    insert_id_padding: usize,
) {
    println!(
        "[Preparing] {}: Inserting {} items into queue: {}",
        cachestore_name, total, queue_path
    );

    for i in 0..total {
        let fut = cachestore.queue_add(QueueAddPayload {
            path: generate_queue_path(queue_path, i + insert_id_padding),
            value: "a".repeat(size_kb * 1024), // size in bytes
            priority: 0,
            orphaned: None,
        });

        let res = fut.await;
        assert!(res.is_ok());
    }

    println!("[Preparing] {}: Done", cachestore_name);
}

fn do_insert_bench(c: &mut Criterion, runtime: &Runtime, total: usize, size_kb: usize) {
    let cachestore_name = format!("cachestore_queue_add_{}", size_kb);
    let cachestore = runtime.block_on(async { prepare_cachestore(&cachestore_name).unwrap() });

    c.bench_with_input(
        BenchmarkId::new(format!("queue_add queues:1, size:{} kb", size_kb), total),
        &(total, size_kb),
        |b, (total, size_kb)| {
            let mut insert_id_padding = 0;

            b.to_async(runtime).iter(|| {
                let prev_value = insert_id_padding.clone();
                insert_id_padding += total;

                do_insert(
                    &cachestore_name,
                    &cachestore,
                    *total,
                    *size_kb,
                    &"STANDALONE#queue",
                    prev_value,
                )
            });
        },
    );
}

async fn do_list(
    cachestore: &Arc<RocksCacheStore>,
    status_filter: Option<QueueItemStatus>,
    total: usize,
) {
    for _ in 0..total {
        let fut = cachestore.queue_list(
            "STANDALONE#queue:1".to_string(),
            status_filter.clone(),
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
    status_filter: Option<QueueItemStatus>,
    per_queue: usize,
    size_kb: usize,
    total: usize,
) {
    let cachestore = runtime.block_on(async {
        let cachestore_name = format!(
            "cachestore_queue_list_{}_{}",
            format!("{:?}", status_filter).to_ascii_lowercase(),
            size_kb
        );
        let cachestore = prepare_cachestore(&cachestore_name).unwrap();

        for idx in 0..5 {
            do_insert(
                &cachestore_name,
                &cachestore,
                per_queue,
                size_kb,
                &format!("STANDALONE#queue{}", idx + 1),
                0,
            )
            .await;
        }

        cachestore
    });

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "queue_list status_filter: {:?} queues:5, size:{} kb, per_queue:{}",
                status_filter, size_kb, per_queue
            ),
            total,
        ),
        &(status_filter, total),
        |b, (status_filter, total)| {
            b.to_async(runtime)
                .iter(|| do_list(&cachestore, status_filter.clone(), *total));
        },
    );
}

async fn do_get(cachestore: &Arc<RocksCacheStore>, total: usize, total_queues: usize) {
    for i in 0..total {
        let fut = cachestore.queue_get(QueueKey::ByPath(generate_queue_path(
            &format!("STANDALONE#queue{}", (i % total_queues) + 1),
            i,
        )));

        let res = fut.await;
        assert!(res.is_ok());
    }
}

fn do_get_bench(
    c: &mut Criterion,
    runtime: &Runtime,
    per_queue: usize,
    size_kb: usize,
    total: usize,
    queues: usize,
) {
    let cachestore = runtime.block_on(async {
        let cachestore_name = format!("cachestore_queue_get_{}", size_kb);
        let cachestore = prepare_cachestore(&cachestore_name).unwrap();

        for idx in 0..queues {
            do_insert(
                &cachestore_name,
                &cachestore,
                per_queue,
                size_kb,
                &format!("STANDALONE#queue{}", idx + 1),
                0,
            )
            .await;
        }

        cachestore
    });

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "queue_get queues:5, size:{} kb, per_queue:{}",
                size_kb, per_queue
            ),
            total,
        ),
        &total,
        |b, total| {
            b.to_async(runtime).iter(|| do_get(&cachestore, *total, 3));
        },
    );
}

fn do_benches(c: &mut Criterion) {
    ALLOCATOR.reset_stats();
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    do_insert_bench(c, &runtime, 512, 64);
    do_insert_bench(c, &runtime, 512, 256);
    do_insert_bench(c, &runtime, 512, 512);

    do_list_bench(c, &runtime, Some(QueueItemStatus::Pending), 1_000, 128, 128);
    do_list_bench(c, &runtime, Some(QueueItemStatus::Active), 1_000, 128, 128);

    do_get_bench(c, &runtime, 2_500, 128, 128, 4);
}

criterion_group!(benches, do_benches);

fn main() {
    benches();
    ALLOCATOR.print_stats();
}
