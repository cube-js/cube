use cubestore::mysql::MySqlServer;
use futures::future::{join3};
use cubestore::config::Config;
use simple_logger::SimpleLogger;
use log::Level;
use std::env;

#[tokio::main]
async fn main() {
    let log_level = match env::var("CUBESTORE_LOG_LEVEL").unwrap_or("info".to_string()).to_lowercase().as_str() {
        "error" => Level::Error,
        "warn" => Level::Warn,
        "info" => Level::Info,
        "debug" => Level::Debug,
        "trace" => Level::Trace,
        x => panic!("Unrecognized log level: {}", x)
    };

    SimpleLogger::new()
        .with_level(Level::Error.to_level_filter())
        .with_module_level("cubestore", log_level.to_level_filter())
        .init().unwrap();

    let services = Config::default().configure().await;

    services.start_processing_loops().await.unwrap();

    let (r1, r2, r3) = join3(
        MySqlServer::listen("127.0.0.1:3306".to_string(), services.sql_service.clone()),
        services.scheduler.write().await.run_scheduler(),
        services.listener.run_listener()
    ).await;
    r1.unwrap();
    r2.unwrap();
    r3.unwrap();
}
