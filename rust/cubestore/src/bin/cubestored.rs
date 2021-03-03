use cubestore::config::{Config, CubeServices};
use cubestore::telemetry::{track_event, ReportingLogger};
use log::debug;
use log::Level;
use simple_logger::SimpleLogger;
use std::collections::HashMap;
use std::env;
use tokio::runtime::Builder;

fn main() {
    let log_level = match env::var("CUBESTORE_LOG_LEVEL")
        .unwrap_or("info".to_string())
        .to_lowercase()
        .as_str()
    {
        "error" => Level::Error,
        "warn" => Level::Warn,
        "info" => Level::Info,
        "debug" => Level::Debug,
        "trace" => Level::Trace,
        x => panic!("Unrecognized log level: {}", x),
    };

    let logger = SimpleLogger::new()
        .with_level(Level::Error.to_level_filter())
        .with_module_level("cubestore", log_level.to_level_filter());
    ReportingLogger::init(Box::new(logger), log_level.to_level_filter()).unwrap();

    let config = Config::default();

    config.configure_worker();

    debug!("New process started");

    #[cfg(not(target_os = "windows"))]
    procspawn::init();

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    runtime.block_on(async move {
        let services = config.configure().await;

        track_event("Cube Store Start".to_string(), HashMap::new()).await;

        stop_on_ctrl_c(&services).await;
        services.wait_processing_loops().await.unwrap();
    });
}

async fn stop_on_ctrl_c(s: &CubeServices) {
    let s = s.clone();
    tokio::spawn(async move {
        let mut counter = 0;
        loop {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Failed to listen for Ctrl+C: {}", e);
                break;
            }
            counter += 1;
            if counter == 1 {
                log::info!("Received Ctrl+C, shutting down.");
                s.stop_processing_loops().await.ok();
            } else if counter == 3 {
                log::info!("Received Ctrl+C 3 times, exiting immediately.");
                std::process::exit(130); // 130 is the default exit code when killed by a signal.
            }
        }
    });
}
