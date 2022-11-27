use criterion::{criterion_group, criterion_main, Criterion};
use cubestore_sql_tests::cubestore_benches;
use rocksdb::{Options, DB};
use std::fs;
use tokio::runtime::Builder;

fn in_process_bench(criterion: &mut Criterion) {
    let benches = cubestore_benches();
    for bench in benches.iter() {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

        let (name, config) = bench.config("in_process");

        let _ = fs::remove_dir_all(config.local_dir().clone());

        {
            let (services, state) = runtime.block_on(async {
                let services = config.configure().await;
                services.start_processing_loops().await.unwrap();
                let state = bench.setup(&services).await.unwrap();
                (services, state)
            });

            criterion.bench_function(name.as_str(), |b| {
                b.to_async(&runtime).iter(|| async {
                    let bench = bench.clone();
                    let services = services.clone();
                    let state = state.clone();
                    async move {
                        bench.bench(&services, state).await.unwrap();
                    }
                    .await;
                });
            });

            runtime.block_on(async {
                services.stop_processing_loops().await.unwrap();
            });
        }

        let _ = DB::destroy(&Options::default(), config.meta_store_path());
        let _ = DB::destroy(&Options::default(), config.cache_store_path());
        let _ = fs::remove_dir_all(config.local_dir().clone());
    }
}

criterion_group!(benches, in_process_bench);
criterion_main!(benches);
