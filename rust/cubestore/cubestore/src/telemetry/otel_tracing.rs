use log::{Log, Metadata, Record};
use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

const OTEL_SERVICE_NAME: &str = "cubestore";

pub fn init_tracing_telemetry(version: String) {
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_http_client(reqwest::Client::new());
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .with_trace_config(opentelemetry_sdk::trace::Config::default().with_resource(
            Resource::new(vec![
                KeyValue::new("service.name", OTEL_SERVICE_NAME),
                KeyValue::new("service.version", version),
            ]),
        ))
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("Should be able to initialise the tracer_provider");

    let tracer = tracer_provider.tracer_builder(OTEL_SERVICE_NAME).build();

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = Registry::default().with(telemetry);

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default tracing subscriber failed");
}

pub struct OpenTelemetryLogger {
    inner_logger: Box<dyn Log>,
    otel_logger: Box<dyn Log>,
}

impl OpenTelemetryLogger {
    pub fn new(logger: Box<dyn Log>) -> Self {
        Self {
            inner_logger: logger,
            otel_logger: Box::new(LogTracer::new()),
        }
    }
}

impl Log for OpenTelemetryLogger {
    fn enabled<'a>(&self, metadata: &Metadata<'a>) -> bool {
        self.inner_logger.enabled(metadata)
    }

    fn log<'a>(&self, record: &Record<'a>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        self.inner_logger.log(&record);
        self.otel_logger.log(&record);
    }

    fn flush(&self) {
        self.inner_logger.flush();
        self.otel_logger.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{Level, Metadata, Record};
    use std::sync::{Arc, Mutex};

    struct MockLogger {
        logs: Arc<Mutex<Vec<String>>>,
        enabled: bool,
    }

    impl MockLogger {
        fn new(enabled: bool) -> Self {
            MockLogger {
                logs: Arc::new(Mutex::new(Vec::new())),
                enabled,
            }
        }
    }

    impl Log for MockLogger {
        fn enabled<'a>(&self, _metadata: &Metadata<'a>) -> bool {
            self.enabled
        }

        fn log<'a>(&self, record: &Record<'a>) {
            let message = format!("{} - {}", record.level(), record.args());
            self.logs.lock().unwrap().push(message);
        }

        fn flush(&self) {}
    }

    #[test]
    fn test_log_forwarding_enabled() {
        let mock_inner_logger = Box::new(MockLogger::new(true));
        let mock_otel_logger = Box::new(MockLogger::new(true));

        let inner_logs = Arc::clone(&mock_inner_logger.logs);
        let otel_logs = Arc::clone(&mock_otel_logger.logs);

        let logger = OpenTelemetryLogger {
            inner_logger: mock_inner_logger,
            otel_logger: mock_otel_logger,
        };

        let record = Record::builder()
            .level(Level::Info)
            .args(format_args!("Test log message"))
            .build();

        logger.log(&record);

        let inner_log_messages = inner_logs.lock().unwrap();
        let otel_log_messages = otel_logs.lock().unwrap();

        assert_eq!(
            inner_log_messages.get(0).unwrap(),
            "INFO - Test log message"
        );
        assert_eq!(otel_log_messages.get(0).unwrap(), "INFO - Test log message");
    }

    #[test]
    fn test_log_forwarding_disabled() {
        let mock_inner_logger = Box::new(MockLogger::new(false));
        let mock_otel_logger = Box::new(MockLogger::new(false));

        let inner_logs = Arc::clone(&mock_inner_logger.logs);
        let otel_logs = Arc::clone(&mock_otel_logger.logs);

        let logger = OpenTelemetryLogger {
            inner_logger: mock_inner_logger,
            otel_logger: mock_otel_logger,
        };

        let record = Record::builder()
            .level(Level::Info)
            .args(format_args!("Test log message"))
            .build();

        logger.log(&record);

        let inner_log_messages = inner_logs.lock().unwrap();
        let otel_log_messages = otel_logs.lock().unwrap();

        assert_eq!(inner_log_messages.len(), 0);
        assert_eq!(otel_log_messages.len(), 0);
    }
}
