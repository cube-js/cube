use std::collections::HashMap;
use testcontainers::runners::AsyncRunner;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::{Mutex, OnceCell};
use tokio_postgres::{Client, NoTls};

struct PgInstance {
    _container: ContainerAsync<Postgres>,
    host: String,
    port: u16,
    seeded: Mutex<HashMap<String, ()>>,
}

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

async fn init_pg() -> PgInstance {
    let container = Postgres::default()
        .start()
        .await
        .expect("Failed to start Postgres container");

    let host = container
        .get_host()
        .await
        .expect("Failed to get container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get container port");

    PgInstance {
        _container: container,
        host,
        port,
        seeded: Mutex::new(HashMap::new()),
    }
}

pub async fn connect() -> Client {
    let pg = PG_INSTANCE.get_or_init(|| init_pg()).await;
    let conn_str = format!(
        "host={} port={} user=postgres password=postgres dbname=postgres",
        pg.host, pg.port
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("Failed to connect to Postgres");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {}", e);
        }
    });

    client
}

pub async fn run_seed(client: &Client, seed_file: &str) {
    let pg = PG_INSTANCE.get().expect("PG not initialized");
    let mut seeded = pg.seeded.lock().await;
    if seeded.contains_key(seed_file) {
        return;
    }

    let seed_path = format!(
        "{}/src/test_fixtures/schemas/yaml_files/seeds/{}",
        env!("CARGO_MANIFEST_DIR"),
        seed_file
    );
    let sql = std::fs::read_to_string(&seed_path)
        .unwrap_or_else(|e| panic!("Failed to read seed file {}: {}", seed_path, e));
    client
        .batch_execute(&sql)
        .await
        .unwrap_or_else(|e| panic!("Failed to execute seed SQL from {}: {}", seed_file, e));

    seeded.insert(seed_file.to_string(), ());
}
