use crate::{
    compile::{test::TestContext, DatabaseProtocol},
    config::processing_loop::{ProcessingLoop, ShutdownMode},
    sql::postgres::PostgresServer,
};
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

async fn connect(port: u16) -> Client {
    for _ in 0..100 {
        if let Ok((client, connection)) = tokio_postgres::connect(
            &format!("host=127.0.0.1 port={} user=test password=test", port),
            NoTls,
        )
        .await
        {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server did not start listening on port {}", port);
}

/// Each reply is several small writes; with Nagle on, every statement after the first write
/// waits for the client's delayed ACK (40 ms minimum on Linux).
#[tokio::test]
async fn test_simple_queries_are_not_delayed_by_nagle() {
    let context = TestContext::new(DatabaseProtocol::PostgreSQL).await;
    let port = portpicker::pick_unused_port().expect("must pick a port");
    let server = PostgresServer::new(
        format!("127.0.0.1:{}", port),
        context.session.session_manager.clone(),
    );
    let loop_server = server.clone();
    tokio::spawn(async move { loop_server.processing_loop().await });

    let client = connect(port).await;
    client.simple_query("SELECT 1").await.unwrap();

    let queries = 20;
    let start = Instant::now();
    for _ in 0..queries {
        client.simple_query("SELECT 1").await.unwrap();
    }
    let elapsed = start.elapsed();

    server.stop_processing(ShutdownMode::Fast).await.unwrap();
    assert!(
        elapsed < Duration::from_millis(20 * queries),
        "{} queries took {:?}",
        queries,
        elapsed
    );
}
