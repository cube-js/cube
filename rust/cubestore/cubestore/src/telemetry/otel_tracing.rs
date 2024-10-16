use lazy_static::lazy_static;
use log::{Level, Log, Metadata, Record};
use opentelemetry::global::ObjectSafeSpan;
use opentelemetry::logs::AnyValue::String;
use opentelemetry::logs::{LogRecord, Logger, LoggerProvider};
use opentelemetry::trace::{SpanKind, Tracer, TracerProvider};
use opentelemetry::{KeyValue, StringValue};
use opentelemetry_sdk::logs::{LogRecord as LogRecordSDK, Logger as LoggerSDK};
use opentelemetry_sdk::trace::Tracer as TracerSDK;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

const OTEL_SERVICE_NAME: &str = "cubestore";

lazy_static! {
    pub static ref OT_TRACER: TracerSDK = init_tracing().unwrap();
    pub static ref OT_LOGGER: LoggerSDK = init_logging().unwrap();
}

pub fn init_tracing_telemetry() {
    let telemetry = tracing_opentelemetry::layer().with_tracer(OT_TRACER.clone());
    let subscriber = Registry::default().with(telemetry);

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default tracing subscriber failed");
}

pub fn init_tracing() -> Result<TracerSDK, Box<dyn std::error::Error>> {
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_http_client(reqwest::Client::new());
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(otlp_exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let tracer = tracer_provider.tracer_builder(OTEL_SERVICE_NAME).build();
    Ok(tracer)
}

pub fn init_logging() -> Result<LoggerSDK, Box<dyn std::error::Error>> {
    let otlp_exporter = opentelemetry_otlp::new_exporter()
        .http()
        .with_http_client(reqwest::Client::new());
    let logger_provider = opentelemetry_otlp::new_pipeline()
        .logging()
        .with_exporter(otlp_exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let logger = logger_provider.logger_builder(OTEL_SERVICE_NAME).build();
    Ok(logger)
}

pub struct OpenTelemetryLogger {
    logger: Box<dyn Log>,
}

impl OpenTelemetryLogger {
    pub fn new(logger: Box<dyn Log>) -> Self {
        Self { logger }
    }
}

impl Log for OpenTelemetryLogger {
    fn enabled<'a>(&self, metadata: &Metadata<'a>) -> bool {
        self.logger.enabled(metadata)
    }

    fn log<'a>(&self, record: &Record<'a>) {
        if !self.enabled(record.metadata()) {
            return;
        }
        self.logger.log(&record);

        match record.metadata().level() {
            Level::Error => {
                // Log error messages using OpenTelemetry logger
                let logger = &*OT_LOGGER;
                emit_log(record, logger);
            }
            Level::Warn => {
                // Log warning messages using OpenTelemetry logger
                let logger = &*OT_LOGGER;
                emit_log(record, logger);
            }
            Level::Info => {}
            Level::Debug => {
                // Create an OpenTelemetry trace for Debug level
                let tracer = &*OT_TRACER;
                create_log_trace(record, tracer);
            }
            Level::Trace => {
                // Create an OpenTelemetry trace for Trace level
                let tracer = &*OT_TRACER;
                create_log_trace(record, tracer);
            }
        }
    }

    fn flush(&self) {
        self.logger.flush()
    }
}

fn emit_log(record: &Record, logger: &LoggerSDK) {
    let mut rec = LogRecordSDK::default();

    rec.set_target(record.target().to_string());
    rec.set_severity_text(record.level().as_str());
    rec.set_body(String(StringValue::from(record.args().to_string())));

    logger.emit(rec);
}

fn create_log_trace(record: &Record, tracer: &TracerSDK) {
    let mut span = tracer
        .span_builder(format!(
            "{} ({})",
            record.module_path().unwrap_or(record.target()),
            record.file().unwrap_or("-")
        ))
        .with_kind(SpanKind::Server)
        .with_attributes([
            KeyValue::new("level", record.level().as_str()),
            KeyValue::new("target", record.target().to_string()),
            KeyValue::new("message", record.args().to_string()),
        ])
        .start(tracer);
    span.end();
}
