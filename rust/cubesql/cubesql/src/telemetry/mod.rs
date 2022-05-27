use crate::{sql::SessionState, CubeError};
use log::{Level, LevelFilter, Log, Metadata, Record};
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, RwLock},
};

lazy_static! {
    static ref REPORTER: RwLock<Box<dyn LogReporter>> = RwLock::new(Box::new(LocalReporter::new()));
}

pub trait LogReporter: Send + Sync + Debug {
    fn log(&self, event: String, properties: HashMap<String, String>, level: Level);

    fn is_active(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct LocalReporter {}

impl LocalReporter {
    pub fn new() -> LocalReporter {
        Self {}
    }
}

impl LogReporter for LocalReporter {
    fn log(&self, _event: String, _properties: HashMap<String, String>, _level: Level) {}

    fn is_active(&self) -> bool {
        false
    }
}
pub struct ReportingLogger {
    logger: Box<dyn Log>,
    need_to_report: bool,
}

impl ReportingLogger {
    pub fn init(
        logger: Box<dyn Log>,
        reporter: Box<dyn LogReporter>,
        max_level: LevelFilter,
    ) -> Result<(), CubeError> {
        let reporting_logger = Self {
            logger,
            need_to_report: reporter.is_active(),
        };

        let mut guard = REPORTER
            .write()
            .expect("failed to unlock REPORTER for writing");
        *guard = reporter;

        log::set_boxed_logger(Box::new(reporting_logger))?;
        log::set_max_level(max_level);

        Ok(())
    }
}

impl Log for ReportingLogger {
    fn enabled<'a>(&self, metadata: &Metadata<'a>) -> bool {
        self.logger.enabled(metadata)
    }

    fn log<'a>(&self, record: &Record<'a>) {
        match (record.metadata().level(), self.need_to_report) {
            // reporting errors only
            (Level::Error, true) => {
                report(
                    "Cube SQL Error".to_string(),
                    HashMap::from([("error".to_string(), format!("{}", record.args()))]),
                    Level::Error,
                );
            }
            _ => self.logger.log(record),
        }
    }

    fn flush(&self) {
        self.logger.flush()
    }
}

pub trait ContextLogger: Send + Sync + Debug {
    fn error(&self, message: &str);
}

#[derive(Debug)]
pub struct SessionLogger {
    session_state: Arc<SessionState>,
}

impl SessionLogger {
    pub fn new(session_state: Arc<SessionState>) -> SessionLogger {
        Self { session_state }
    }

    fn log(&self, target: &str, props: HashMap<String, String>, level: Level) {
        // TODO: MySQL app_name
        let mut meta_fields = props;
        if let Some(name) = self.session_state.all_variables().get("application_name") {
            meta_fields.insert("appName".to_string(), name.value.to_string());
        }
        let protocol = self.session_state.protocol.to_string();
        meta_fields.insert("protocol".to_string(), protocol);
        meta_fields.insert("apiType".to_string(), "sql".to_string());

        if report(target.to_string(), meta_fields.clone(), level) == false {
            log::log!(target: target, level, "{:?}", meta_fields);
        }
    }
}

impl ContextLogger for SessionLogger {
    fn error(&self, message: &str) {
        self.log(
            "Cube SQL Error",
            HashMap::from([("error".to_string(), message.to_string())]),
            Level::Error,
        );
    }
}

fn report(event: String, properties: HashMap<String, String>, level: Level) -> bool {
    let guard = REPORTER
        .read()
        .expect("failed to unlock REPORTER for reading");
    if !guard.is_active() {
        return false;
    }

    let mut properties = properties;
    properties.insert("apiType".to_string(), "sql".to_string());

    guard.log(event, properties, level);

    true
}
