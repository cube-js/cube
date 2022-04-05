use std::cell::{Cell, RefCell};
use criterion::{criterion_group, criterion_main, Criterion};
use cubestore::config::{env_parse, Config};
use cubestore_sql_tests::cubestore_benches;
use rocksdb::{Options, DB};
use std::fs;
use std::sync::Arc;
use tokio::runtime::Builder;

fn inline_bench(criterion: &mut Criterion) {
    let benches = cubestore_benches();
    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    for bench in benches.iter() {
        let config = Config::test(bench.name()).update_config(|mut c| {
            c.partition_split_threshold = 10_000_000;
            c.max_partition_split_threshold = 10_000_000;
            c.max_cached_queries = 0;
            c.max_cached_metadata = env_parse("CUBESTORE_MAX_CACHED_METADATA", 0);
            c
        });

        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = fs::remove_dir_all(config.local_dir().clone());

        {
            let (services, state) = runtime.block_on(async {
                let services = config.configure().await;
                services.start_processing_loops().await.unwrap();
                let state = bench.setup(&services).await.unwrap();
                (services, state)
            });

            // rocksdb keeps its LOCK file alive too long (past process death!)
            // according to docs, it's supposed to close cleanly on moving out of scope
            // make sure criterion doesn't inadvertently fail to release the bench closure,
            // thus inadvertently keeping a ref to the rocksdb instance.
            let services_cell = Arc::new(Cell::new(Some(services.clone())));

            criterion.bench_function(bench.name(), |b| {
                b.to_async(&runtime).iter(|| async {
                    let bench = bench.clone();
                    let services_cell = services_cell.clone();
                    let state = state.clone();
                    async move {
                        let services = services_cell.replace(None).unwrap();
                        bench.bench(&services, state).await.unwrap();
                        services_cell.replace(Some(services));
                    }.await;
                });
            });

            // release the ref used by the criterion closure.
            services_cell.replace(None).unwrap();

            runtime.block_on(async {
                services.stop_processing_loops().await.unwrap();
            });

            // let db = services.rocks_meta_store.unwrap().db.clone();
            // core::ptr::drop_in_place(*db);
            // println!("QQQ {} {}", Arc::<DB>::weak_count(&db), Arc::<DB>::strong_count(&db));
            // DB::destroy(db);
            // std::mem::drop(db.as_ref());
        }

        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = fs::remove_dir_all(config.local_dir().clone());
    }
}

criterion_group!(benches, inline_bench);
criterion_main!(benches);
