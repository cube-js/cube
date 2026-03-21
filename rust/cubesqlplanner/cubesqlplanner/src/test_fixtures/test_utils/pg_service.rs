use std::sync::atomic::{AtomicU64, Ordering};
use testcontainers::core::{CmdWaitFor, ExecCommand};
use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;
use tokio_postgres::{Client, NoTls};

type PgContainer = testcontainers::ContainerAsync<Postgres>;

struct PgInstance {
    _container: PgContainer,
    host: String,
    port: u16,
}

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();
static DB_COUNTER: AtomicU64 = AtomicU64::new(0);

async fn init_pg() -> PgInstance {
    let container = Postgres::default()
        .with_tag("16-bookworm")
        .start()
        .await
        .expect("Failed to start Postgres container");

    // Install HLL extension for countDistinctApprox support
    container
        .exec(
            ExecCommand::new(vec![
                "sh",
                "-c",
                "apt-get update -qq && apt-get install -y -qq postgresql-16-hll > /dev/null 2>&1",
            ])
            .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("Failed to install postgresql-16-hll");

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
    }
}

async fn connect_to(db_name: &str) -> Client {
    let pg = PG_INSTANCE.get_or_init(|| init_pg()).await;
    let conn_str = format!(
        "host={} port={} user=postgres password=postgres dbname={}",
        pg.host, pg.port, db_name
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to {}: {}", db_name, e));

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {}", e);
        }
    });

    client
}

pub async fn connect_and_seed(seed_file: &str) -> Client {
    let id = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let db_name = format!("test_{}", id);

    let admin = connect_to("postgres").await;
    admin
        .execute(&format!("CREATE DATABASE \"{db_name}\""), &[])
        .await
        .unwrap_or_else(|e| panic!("Failed to create database {}: {}", db_name, e));
    drop(admin);

    let client = connect_to(&db_name).await;
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

    client
}
