use crate::telemetry::ReportingLogger;
use log::{Level, Log, Metadata, Record};
use simple_logger::SimpleLogger;
use std::env;

/// Logger will add 'CUBESTORE_LOG_CONTEXT' to all messages.
/// Set it during `procspawn` to help distinguish processes in the logs.
pub fn init_cube_logger(enable_telemetry: bool) {
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

    let mut ctx = format!("pid:{}", std::process::id());
    if let Ok(extra) = env::var("CUBESTORE_LOG_CONTEXT") {
        ctx += " ";
        ctx += &extra;
    }
    let mut logger: Box<dyn Log> = Box::new(ContextLogger::new(ctx, logger));
    if enable_telemetry {
        logger = Box::new(ReportingLogger::new(logger))
    }

    log::set_boxed_logger(logger).expect("Failed to initialize logger");
    log::set_max_level(log_level.to_level_filter());
}

/// Adds the same 'context' string to all log messages.
pub struct ContextLogger<Logger> {
    context: String,
    inner: Logger,
}

impl<Logger: Log> ContextLogger<Logger> {
    pub fn new(context: String, inner: Logger) -> Self {
        Self { context, inner }
    }
}

impl<Logger: Log> Log for ContextLogger<Logger> {
    fn enabled(&self, metadata: &Metadata<'a>) -> bool {
        self.inner.enabled(metadata)
    }

    fn log(&self, record: &Record<'a>) {
        if !self.enabled(record.metadata()) {
            // Assume inner logger is not interested.
            return;
        }
        self.inner.log(
            &record
                .to_builder()
                .args(format_args!("<{}> {}", self.context, record.args()))
                .build(),
        )
    }

    fn flush(&self) {
        self.inner.flush()
    }
}
