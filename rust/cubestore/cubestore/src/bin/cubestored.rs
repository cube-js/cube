use cubestore::config::{validate_config, Config, CubeServices};
use cubestore::http::status::serve_status_probes;
use cubestore::telemetry::{init_agent_sender, track_event};
use cubestore::util::logger::init_cube_logger;
use cubestore::util::metrics::init_metrics;
use cubestore::util::{metrics, spawn_malloc_trim_loop};
use cubestore::{app_metrics, CubeError};
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
    let metrics_format = match std::env::var("CUBESTORE_METRICS_FORMAT") {
        Ok(s) if s == "statsd" => metrics::Compatibility::StatsD,
        Ok(s) if s == "dogstatsd" => metrics::Compatibility::DogStatsD,
        Ok(s) => panic!(
            "CUBESTORE_METRICS_FORMAT must be 'statsd' or 'dogstatsd', got '{}'",
            s
        ),
        Err(_) => metrics::Compatibility::StatsD,
    };
    let metrics_addr =
        std::env::var("CUBESTORE_METRICS_ADDRESS").unwrap_or("127.0.0.1".to_string());
    let metrics_port = std::env::var("CUBESTORE_METRICS_PORT").unwrap_or("8125".to_string());
    let metrics_server_address = format!("{}:{}", metrics_addr, metrics_port);

    init_metrics(
        "127.0.0.1:0",
        metrics_server_address,
        metrics_format,
        vec![],
    );
    let telemetry_env = std::env::var("CUBESTORE_TELEMETRY")
        .or(std::env::var("CUBEJS_TELEMETRY"))
        .unwrap_or("true".to_string());
    let enable_telemetry = telemetry_env
        .parse::<bool>()
        .map_err(|e| {
            CubeError::user(format!(
                "Can't parse telemetry env variable '{}': {}",
                telemetry_env, e
            ))
        })
        .unwrap();
    init_cube_logger(enable_telemetry);

    log::info!("Cube Store version {}", version);

    let config = Config::default();

    let trim_every = config.config_obj().malloc_trim_every_secs();
    if trim_every != 0 {
        spawn_malloc_trim_loop(Duration::from_secs(trim_every));
    }

    debug!("New process started");
    app_metrics::STARTUPS.increment();

    #[cfg(not(target_os = "windows"))]
    cubestore::util::respawn::init();

    let mut tokio_builder = Builder::new_multi_thread();
    tokio_builder.enable_all();
    tokio_builder.thread_name("cubestore-main");
    if let Ok(var) = std::env::var("CUBESTORE_EVENT_LOOP_WORKER_THREADS") {
        tokio_builder.worker_threads(var.parse().unwrap());
    }
    let runtime = tokio_builder.build().unwrap();
    runtime.block_on(async move {
        init_agent_sender().await;

        validate_config(config.config_obj().as_ref()).report_and_abort_on_errors();

        config.configure_injector().await;

        serve_status_probes(&config);

        let services = config.cube_services().await;

        if enable_telemetry {
            track_event("Cube Store Start".to_string(), HashMap::new()).await;
        }

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
