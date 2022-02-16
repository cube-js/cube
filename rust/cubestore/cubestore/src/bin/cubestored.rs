use cubestore::app_metrics;
use cubestore::config::{validate_config, Config, CubeServices};
use cubestore::http::status::serve_status_probes;
use cubestore::telemetry::{init_agent_sender, track_event};
use cubestore::util::logger::init_cube_logger;
use cubestore::util::metrics::init_metrics;
use cubestore::util::{metrics, spawn_malloc_trim_loop};
use datafusion::cube_ext;
use log::debug;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::runtime::Builder;

const PACKAGE_JSON: &'static str = std::include_str!("../../../package.json");

fn main() {
    let package_json: Value = serde_json::from_str(PACKAGE_JSON).unwrap();
    let version = package_json
        .get("version")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    let metrics_mode = match std::env::var("CUBESTORE_METRICS") {
        Ok(s) if s == "statsd" => metrics::Compatibility::StatsD,
        Ok(s) if s == "dogstatsd" => metrics::Compatibility::DogStatsD,
        Ok(s) => panic!(
            "CUBESTORE_METRICS must be 'statsd' or 'dogstatsd', got '{}'",
            s
        ),
        Err(_) => metrics::Compatibility::StatsD,
    };
    init_metrics("127.0.0.1:0", "127.0.0.1:8125", metrics_mode);
    init_cube_logger(true);

    log::info!("Cube Store version {}", version);

    let config = Config::default();
    Config::configure_worker_services();

    let trim_every = config.config_obj().malloc_trim_every_secs();
    if trim_every != 0 {
        spawn_malloc_trim_loop(Duration::from_secs(trim_every));
    }

    debug!("New process started");
    app_metrics::STARTUPS.increment();

    #[cfg(not(target_os = "windows"))]
    cubestore::util::respawn::init();

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    runtime.block_on(async move {
        init_agent_sender().await;

        validate_config(config.config_obj().as_ref()).report_and_abort_on_errors();

        config.configure_injector().await;

        serve_status_probes(&config);

        let services = config.cube_services().await;

        track_event("Cube Store Start".to_string(), HashMap::new()).await;

        stop_on_ctrl_c(&services).await;
        services.wait_processing_loops().await.unwrap();
    });
}

async fn stop_on_ctrl_c(s: &CubeServices) {
    let s = s.clone();
    cube_ext::spawn(async move {
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
