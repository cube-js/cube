use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

struct CubeStoreInstance {
    mysql_port: u16,
    _child: Child,
}

static CLEANUP_PID: OnceLock<u32> = OnceLock::new();
static CLEANUP_DATA_DIR: OnceLock<PathBuf> = OnceLock::new();

extern "C" fn cleanup_cubestored() {
    if let Some(pid) = CLEANUP_PID.get() {
        let _ = Command::new("kill").arg(pid.to_string()).output();
    }
    if let Some(dir) = CLEANUP_DATA_DIR.get() {
        let _ = std::fs::remove_dir_all(dir);
    }
}

fn register_atexit_cleanup(pid: u32, data_dir: PathBuf) {
    CLEANUP_PID.set(pid).ok();
    CLEANUP_DATA_DIR.set(data_dir).ok();
    extern "C" {
        fn atexit(cb: extern "C" fn()) -> std::os::raw::c_int;
    }
    unsafe {
        atexit(cleanup_cubestored);
    }
}

static CS_INSTANCE: OnceCell<CubeStoreInstance> = OnceCell::const_new();
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

fn cubestored_bin() -> PathBuf {
    if let Ok(path) = std::env::var("CUBESTORED_BIN_PATH") {
        let path = PathBuf::from(path);
        if path.exists() {
            return path;
        }
        panic!("CUBESTORED_BIN_PATH points to a missing file: {:?}", path);
    }

    let cubestore_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../cubestore");
    for candidate in [
        cubestore_root.join("target/release/cubestored"),
        cubestore_root.join("target/debug/cubestored"),
        cubestore_root.join("downloaded/latest/bin/cubestored"),
    ] {
        if candidate.exists() {
            return candidate;
        }
    }

    panic!(
        "cubestored binary not found. Build it with \
         `cargo build -p cubestore --bin cubestored` in rust/cubestore \
         or set CUBESTORED_BIN_PATH"
    );
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind to a free port")
        .local_addr()
        .expect("Failed to get local addr")
        .port()
}

async fn init_cubestore() -> CubeStoreInstance {
    let bin = cubestored_bin();
    let mysql_port = free_port();
    let http_port = free_port();
    let status_port = free_port();

    let data_dir = std::env::temp_dir().join(format!(
        "cubestored-test-{}-{}",
        std::process::id(),
        mysql_port
    ));

    let child = Command::new(&bin)
        .env("CUBESTORE_PORT", mysql_port.to_string())
        .env("CUBESTORE_HTTP_PORT", http_port.to_string())
        .env("CUBESTORE_STATUS_PORT", status_port.to_string())
        .env("CUBESTORE_BIND_ADDR", format!("127.0.0.1:{}", mysql_port))
        .env(
            "CUBESTORE_HTTP_BIND_ADDR",
            format!("127.0.0.1:{}", http_port),
        )
        .env(
            "CUBESTORE_STATUS_BIND_ADDR",
            format!("127.0.0.1:{}", status_port),
        )
        .env("CUBESTORE_DATA_DIR", &data_dir)
        .env("CUBESTORE_SELECT_WORKERS", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to spawn cubestored from {:?}: {}", bin, e));

    register_atexit_cleanup(child.id(), data_dir.clone());

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if std::net::TcpStream::connect(("127.0.0.1", mysql_port)).is_ok() {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "cubestored did not open MySQL port {} within 60s (binary: {:?})",
                mysql_port, bin
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    CubeStoreInstance {
        mysql_port,
        _child: child,
    }
}

/// Connects to the shared cubestored instance and creates a fresh
/// per-test schema; returns the connection and the schema name.
pub async fn connect_with_schema() -> (mysql_async::Conn, String) {
    let instance = CS_INSTANCE.get_or_init(init_cubestore).await;

    let url = format!("mysql://root:@127.0.0.1:{}/", instance.mysql_port);
    let opts = mysql_async::Opts::from_url(&url).expect("Invalid cubestore connection URL");
    let mut conn = mysql_async::Conn::new(opts)
        .await
        .expect("Failed to connect to cubestored");

    let schema = format!("test_{}", SCHEMA_COUNTER.fetch_add(1, Ordering::Relaxed));
    use mysql_async::prelude::Queryable;
    conn.query_drop(format!("CREATE SCHEMA {}", schema))
        .await
        .unwrap_or_else(|e| panic!("Failed to create schema {}: {}", schema, e));

    (conn, schema)
}
