//! Measures what the planning throttle does to a burst of concurrent queries.
//! Runs against a real metastore directory pointed to by CUBESTORE_METASTORE_DUMP_PATH:
//!
//!   CUBESTORE_METASTORE_DUMP_PATH=/path/to/metastore SHAPE=union UNION_FAMILY=<table name part> \
//!     cargo test --release -p cubestore throttle_sweep -- --ignored --nocapture

use crate::cachestore::RocksCacheStore;
use crate::config::Config;
use crate::metastore::table::TablePath;
use crate::metastore::{BaseRocksStoreFs, MetaStore, RocksMetaStore};
use crate::queryplanner::metadata_cache::BasicMetadataCacheFactory;
use crate::queryplanner::{QueryPlanner, QueryPlannerImpl};
use crate::remotefs::LocalDirRemoteFs;
use crate::sql::cache::SqlResultCache;
use crate::CubeError;
use datafusion::sql::parser::DFParser;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn copy_dir(from: &Path, to: &Path) -> Result<(), CubeError> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

struct Dump {
    meta_store: Arc<RocksMetaStore>,
    cache_store: Arc<RocksCacheStore>,
    config: Config,
    store_path: PathBuf,
    remote_store_path: PathBuf,
}

impl Dump {
    /// Opens a copy of the metastore: opening may run migrations and write to the store.
    fn open(test_name: &str) -> Result<Dump, CubeError> {
        let source = env::var("CUBESTORE_METASTORE_DUMP_PATH").expect(
            "set CUBESTORE_METASTORE_DUMP_PATH to the metastore directory you want to inspect",
        );

        let config = Config::test(test_name);
        let store_path = env::current_dir()?.join(format!("{}-local", test_name));
        let remote_store_path = env::current_dir()?.join(format!("{}-remote", test_name));
        let _ = fs::remove_dir_all(&store_path);
        let _ = fs::remove_dir_all(&remote_store_path);

        let db_path = store_path.join("metastore");
        copy_dir(Path::new(&source), &db_path)?;

        let remote_fs = LocalDirRemoteFs::new(Some(remote_store_path.clone()), store_path.clone());
        let meta_store = RocksMetaStore::new(
            db_path.as_path(),
            BaseRocksStoreFs::new_for_metastore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )?;
        let cache_store = RocksCacheStore::new(
            store_path.join("cachestore").as_path(),
            BaseRocksStoreFs::new_for_cachestore(remote_fs.clone(), config.config_obj()),
            config.config_obj(),
        )?;

        Ok(Dump {
            meta_store,
            cache_store,
            config,
            store_path,
            remote_store_path,
        })
    }

    fn planner(&self, max_concurrent: usize, max_queued: usize) -> Arc<QueryPlannerImpl> {
        let config = self.config.update_config(move |mut c| {
            c.max_concurrent_query_plans = max_concurrent;
            c.max_queued_query_plans = max_queued;
            c
        });
        QueryPlannerImpl::new(
            self.meta_store.clone(),
            self.cache_store.clone(),
            config.config_obj(),
            Arc::new(SqlResultCache::new(1 << 30, Some(120), 10000, None)),
            Arc::new(BasicMetadataCacheFactory::new()),
        )
    }
}

impl Drop for Dump {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.store_path);
        let _ = fs::remove_dir_all(&self.remote_store_path);
    }
}

/// Resident set size of this process, in MB.
fn rss_mb() -> f64 {
    let out = std::process::Command::new("ps")
        .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps");
    String::from_utf8_lossy(&out.stdout)
        .trim()
        .parse::<f64>()
        .unwrap_or(0.0)
        / 1024.0
}

async fn plan_one(planner: Arc<QueryPlannerImpl>, sql: String) -> Result<(), CubeError> {
    let statement = DFParser::parse_sql(&sql)
        .map_err(|e| CubeError::internal(format!("parse {}: {}", sql, e)))?
        .pop_front()
        .unwrap();
    planner
        .logical_plan(statement, &Vec::new(), None)
        .await
        .map(|_| ())
}

fn simple_queries(paths: &[TablePath]) -> Vec<String> {
    paths
        .iter()
        .filter(|t| t.table.get_row().is_ready())
        .map(|t| {
            format!(
                "SELECT * FROM {}.{} LIMIT 1",
                t.schema.get_row().get_name(),
                t.table.get_row().get_table_name()
            )
        })
        .collect()
}

fn union_query(paths: &[TablePath], family: &str) -> (String, usize) {
    let mut tables: Vec<&TablePath> = paths
        .iter()
        .filter(|t| {
            t.table.get_row().is_ready() && t.table.get_row().get_table_name().contains(family)
        })
        .collect();
    tables.sort_by_key(|t| t.table.get_row().get_table_name().clone());
    let parts: Vec<String> = tables
        .iter()
        .map(|t| {
            format!(
                "SELECT * FROM {}.{}",
                t.schema.get_row().get_name(),
                t.table.get_row().get_table_name()
            )
        })
        .collect();
    (
        format!("SELECT count(*) FROM ({}) AS u", parts.join(" UNION ALL ")),
        tables.len(),
    )
}

struct RssSampler {
    sampling: Arc<AtomicBool>,
    peak: Arc<Mutex<f64>>,
    handle: std::thread::JoinHandle<()>,
}

impl RssSampler {
    /// The peak must be sampled while the queries are in flight: after they finish it is gone.
    fn start(from: f64) -> RssSampler {
        let sampling = Arc::new(AtomicBool::new(true));
        let peak = Arc::new(Mutex::new(from));
        let handle = {
            let (sampling, peak) = (sampling.clone(), peak.clone());
            std::thread::spawn(move || {
                while sampling.load(Ordering::Relaxed) {
                    let rss = rss_mb();
                    let mut p = peak.lock().unwrap();
                    if rss > *p {
                        *p = rss;
                    }
                    drop(p);
                    std::thread::sleep(Duration::from_millis(2));
                }
            })
        };
        RssSampler {
            sampling,
            peak,
            handle,
        }
    }

    fn stop(self) -> f64 {
        self.sampling.store(false, Ordering::Relaxed);
        self.handle.join().unwrap();
        let peak = *self.peak.lock().unwrap();
        peak
    }
}

/// Sweeps CUBESTORE_MAX_CONCURRENT_QUERY_PLANS at a fixed arrival burst.
/// SHAPE=simple plans one query per table, SHAPE=union plans the wide UNION ALL shape
/// Cube generates over per-date pre-aggregation partitions.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn throttle_sweep() -> Result<(), CubeError> {
    let dump = Dump::open("throttle_sweep")?;
    let paths = dump.meta_store.get_tables_with_path(false).await?;

    let shape = env::var("SHAPE").unwrap_or_else(|_| "simple".to_string());
    let queries: Vec<String> = if shape == "union" {
        let family = env::var("UNION_FAMILY")
            .expect("set UNION_FAMILY to the common part of the table names to union");
        let (sql, tables) = union_query(&paths, &family);
        println!("shape: union of {} tables", tables);
        assert!(tables > 0, "no ready tables matching {}", family);
        vec![sql]
    } else {
        let queries = simple_queries(&paths);
        println!("shape: simple, {} plannable tables", queries.len());
        assert!(!queries.is_empty(), "no ready tables in the dump");
        queries
    };

    let concurrency: usize = env::var("PLANNING_CONCURRENCY")
        .unwrap_or_else(|_| "2048".to_string())
        .parse()
        .unwrap();
    let limits: Vec<usize> = env::var("THROTTLE_LIMITS")
        .unwrap_or_else(|_| "0,2,4,8,16,32".to_string())
        .split(',')
        .map(|s| s.trim().parse().unwrap())
        .collect();

    // Warm up CachedTables and the RocksDB block cache so the sweep measures steady state.
    plan_one(dump.planner(0, 0), queries[0].clone()).await?;

    println!(
        "\nburst of {} queries\n{:>6} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
        concurrency,
        "limit",
        "wall_ms",
        "per_q_ms",
        "p50_ms",
        "p95_ms",
        "max_ms",
        "peak_mb",
        "d_rss_mb"
    );
    for &limit in limits.iter() {
        let planner = dump.planner(limit, 0);
        let rss_before = rss_mb();
        let sampler = RssSampler::start(rss_before);
        let started = Instant::now();

        let mut handles = Vec::with_capacity(concurrency);
        for i in 0..concurrency {
            let planner = planner.clone();
            let sql = queries[i % queries.len()].clone();
            handles.push(tokio::spawn(async move {
                let at = Instant::now();
                plan_one(planner, sql).await.map(|_| at.elapsed())
            }));
        }
        let mut latencies = Vec::with_capacity(concurrency);
        for handle in handles {
            latencies.push(handle.await.unwrap()?);
        }
        let wall = started.elapsed();
        let peak_mb = sampler.stop();
        latencies.sort();

        println!(
            "{:>6} {:>10.1} {:>12.2} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>10.1}",
            limit,
            wall.as_secs_f64() * 1000.0,
            wall.as_secs_f64() * 1000.0 / concurrency as f64,
            latencies[latencies.len() / 2].as_secs_f64() * 1000.0,
            latencies[latencies.len() * 95 / 100].as_secs_f64() * 1000.0,
            latencies.last().unwrap().as_secs_f64() * 1000.0,
            peak_mb,
            peak_mb - rss_before
        );
    }

    Ok(())
}
