use log::{Log, Metadata, Record};
use opentelemetry::trace::TracerProvider;
use tracing_log::LogTracer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

const OTEL_SERVICE_NAME: &str = "cubestore";

pub fn init_tracing_telemetry() {
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_http_client(reqwest::Client::new());
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
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
